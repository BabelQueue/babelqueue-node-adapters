import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  AsbConsumer,
  AsbPublisher,
  toServiceBusMessage,
  type AsbMessage,
  type AsbReceivedMessage,
  type AsbReceiver,
  type AsbSender,
  type BabelHandlers,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(): Envelope {
  return EnvelopeCodec.make(URN, { order_id: 1042 }, { queue: "orders" });
}

class FakeSender implements AsbSender {
  entityPath = "orders";
  sent: AsbMessage[] = [];
  scheduled: { message: AsbMessage; at: Date }[] = [];

  async sendMessages(message: AsbMessage): Promise<void> {
    this.sent.push(message);
  }

  async scheduleMessages(message: AsbMessage, scheduledEnqueueTimeUtc: Date): Promise<bigint[]> {
    this.scheduled.push({ message, at: scheduledEnqueueTimeUtc });
    return [1n];
  }
}

class FakeReceiver implements AsbReceiver {
  completed: AsbReceivedMessage[] = [];
  abandoned: AsbReceivedMessage[] = [];
  deadLettered: { message: AsbReceivedMessage; reason?: string; description?: string }[] = [];

  constructor(private readonly messages: AsbReceivedMessage[] = []) {}

  async receiveMessages(maxMessageCount: number): Promise<AsbReceivedMessage[]> {
    return this.messages.splice(0, maxMessageCount);
  }

  async completeMessage(message: AsbReceivedMessage): Promise<void> {
    this.completed.push(message);
  }

  async abandonMessage(message: AsbReceivedMessage): Promise<void> {
    this.abandoned.push(message);
  }

  async deadLetterMessage(
    message: AsbReceivedMessage,
    options?: { deadLetterReason?: string; deadLetterErrorDescription?: string },
  ): Promise<void> {
    this.deadLettered.push({
      message,
      reason: options?.deadLetterReason,
      description: options?.deadLetterErrorDescription,
    });
  }
}

function received(deliveryCount: number, body: string): AsbReceivedMessage {
  return { body, deliveryCount };
}

test("projection maps native fields and application properties", () => {
  const env = envelope();
  const msg = toServiceBusMessage(env);
  assert.equal(msg.subject, URN);
  assert.equal(msg.correlationId, env.trace_id);
  assert.equal(msg.messageId, env.meta.id);
  assert.equal(msg.contentType, "application/json");
  assert.equal(msg.applicationProperties?.["bq-schema-version"], env.meta.schema_version);
  assert.equal(msg.applicationProperties?.["bq-source-lang"], env.meta.lang);
  assert.equal(msg.applicationProperties?.["bq-created-at"], env.meta.created_at);
  assert.equal(EnvelopeCodec.urn(EnvelopeCodec.decode(String(msg.body))), URN);
});

test("publish projects subject and returns message id", async () => {
  const sender = new FakeSender();
  const id = await new AsbPublisher(sender).publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  assert.equal(sender.sent.length, 1);
  assert.equal(sender.sent[0]!.subject, URN);
  assert.equal(sender.sent[0]!.correlationId, "trace-1");
  assert.equal(sender.sent[0]!.messageId, id);
});

test("publish with delay schedules instead of sending", async () => {
  const sender = new FakeSender();
  await new AsbPublisher(sender).publish(URN, {}, { delayMs: 30000 });
  assert.equal(sender.scheduled.length, 1);
  assert.equal(sender.sent.length, 0);
  assert.equal(sender.scheduled[0]!.message.applicationProperties?.["bq-delay"], 30000);
});

test("consume: attempts is deliveryCount - 1 and completes", async () => {
  const receiver = new FakeReceiver([received(3, EnvelopeCodec.encode(envelope()))]);
  let seen: number | undefined;
  const handlers: BabelHandlers = { [URN]: (env) => { seen = env.attempts; } };
  const count = await new AsbConsumer(receiver, handlers).poll();
  assert.equal(count, 1);
  assert.equal(seen, 2);
  assert.equal(receiver.completed.length, 1);
});

test("consume: first delivery is zero attempts", async () => {
  const receiver = new FakeReceiver([received(1, EnvelopeCodec.encode(envelope()))]);
  let seen: number | undefined;
  await new AsbConsumer(receiver, { [URN]: (env) => { seen = env.attempts; } }).poll();
  assert.equal(seen, 0);
});

test("consume: throwing handler abandons and reports onError", async () => {
  const receiver = new FakeReceiver([received(1, EnvelopeCodec.encode(envelope()))]);
  let reported: unknown;
  await new AsbConsumer(receiver, { [URN]: () => { throw new Error("boom"); } }, {
    onError: (error) => { reported = error; },
  }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(receiver.abandoned.length, 1);
  assert.equal(receiver.completed.length, 0);
});

test("consume: unknown urn with hook completes", async () => {
  const receiver = new FakeReceiver([received(1, EnvelopeCodec.encode(envelope()))]);
  let called = false;
  await new AsbConsumer(receiver, {}, { onUnknownUrn: () => { called = true; } }).poll();
  assert.ok(called);
  assert.equal(receiver.completed.length, 1);
});

test("consume: unknown urn without hook abandons and reports onError", async () => {
  const receiver = new FakeReceiver([received(1, EnvelopeCodec.encode(envelope()))]);
  let reported: unknown;
  await new AsbConsumer(receiver, {}, { onError: (error) => { reported = error; } }).poll();
  assert.ok(reported);
  assert.equal(receiver.abandoned.length, 1);
});

test("consume: non-conformant envelope abandons and reports onError", async () => {
  const bad =
    '{"trace_id":"t","data":{"x":1},"meta":{"id":"m","queue":"q","lang":"node","schema_version":1,"created_at":1},"attempts":0}';
  const receiver = new FakeReceiver([received(1, bad)]);
  let reported: unknown;
  await new AsbConsumer(receiver, {}, { onError: (error) => { reported = error; } }).poll();
  assert.ok(reported);
  assert.equal(receiver.abandoned.length, 1);
});

// --- §4.7 schema-version gate (decided before the body is decoded) -------------------------------

function gated(value?: unknown): AsbReceivedMessage {
  const body = EnvelopeCodec.encode(envelope());
  return value === undefined
    ? { body, deliveryCount: 1 }
    : { body, deliveryCount: 1, applicationProperties: { "bq-schema-version": value } };
}

test("gate: a missing or blank bq-schema-version decodes and runs the handler", async () => {
  for (const value of [undefined, "", " \t", "\n\u000b\f\r"]) {
    const receiver = new FakeReceiver([gated(value)]);
    let calls = 0;
    await new AsbConsumer(receiver, { [URN]: () => { calls += 1; } }).poll();
    assert.equal(calls, 1, JSON.stringify(value));
    assert.equal(receiver.completed.length, 1, JSON.stringify(value));
    assert.equal(receiver.deadLettered.length, 0, JSON.stringify(value));
  }
});

test("gate: bq-schema-version 1 (number) and \"1\" (string) run the handler", async () => {
  for (const value of [1, 1n, "1"]) {
    const receiver = new FakeReceiver([gated(value)]);
    let calls = 0;
    await new AsbConsumer(receiver, { [URN]: () => { calls += 1; } }).poll();
    assert.equal(calls, 1, String(value));
    assert.equal(receiver.completed.length, 1, String(value));
  }
});

test("gate: an unsupported bq-schema-version is dead-lettered explicitly, undecoded, never abandoned", async () => {
  for (const value of ["2", "x", 2, 2n, 0, 1.5, true]) {
    const receiver = new FakeReceiver([gated(value)]);
    let calls = 0;
    let captured: unknown = null;
    let seenEnvelope: unknown = null;
    await new AsbConsumer(
      receiver,
      { [URN]: () => { calls += 1; } },
      { onError: (e, env) => { captured = e; seenEnvelope = env; } },
    ).poll();
    const label = String(value);
    assert.equal(calls, 0, label);
    assert.ok(captured instanceof Error, label);
    assert.match((captured as Error).message, /unsupported bq-schema-version/, label);
    assert.deepEqual(seenEnvelope, {}, label);
    assert.equal(receiver.deadLettered.length, 1, label);
    assert.equal(receiver.deadLettered[0]?.reason, "unsupported schema version", label);
    assert.equal(receiver.abandoned.length, 0, label);
    assert.equal(receiver.completed.length, 0, label);
  }
});

test("gate: an oversized bq-schema-version is truncated in the dead-letter description", async () => {
  const receiver = new FakeReceiver([gated("9".repeat(5000))]);
  let captured: unknown = null;
  await new AsbConsumer(receiver, {}, { onError: (e) => { captured = e; } }).poll();
  assert.equal(receiver.deadLettered.length, 1);
  const description = receiver.deadLettered[0]?.description ?? "";
  assert.ok(description.includes(`"${"9".repeat(64)}..."`));
  assert.ok(!description.includes("9".repeat(65)));
  assert.ok(description.length < 4096);
  assert.equal((captured as Error).message, description);
});

test("gate: the version is decided before decode - an undecodable body with version 2 is dead-lettered by the gate", async () => {
  const receiver = new FakeReceiver([{ body: "{not json", deliveryCount: 1, applicationProperties: { "bq-schema-version": "2" } }]);
  await new AsbConsumer(receiver, {}).poll();
  assert.equal(receiver.deadLettered.length, 1);
  assert.equal(receiver.abandoned.length, 0);
});

test("gate: a receiver without deadLetterMessage degrades to abandon (still undecoded)", async () => {
  const abandoned: AsbReceivedMessage[] = [];
  const receiver: AsbReceiver = {
    async receiveMessages() { return [gated("2")]; },
    async completeMessage() {},
    async abandonMessage(m) { abandoned.push(m); },
  };
  let calls = 0;
  const errors: unknown[] = [];
  await new AsbConsumer(receiver, { [URN]: () => { calls += 1; } }, { onError: (e) => { errors.push(e); } }).poll();
  assert.equal(calls, 0);
  assert.equal(abandoned.length, 1);
  assert.equal(errors.length, 1);
  assert.match((errors[0] as Error).message, /degraded to abandon/);
});
