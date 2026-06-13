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
