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

// --- ADR-0037: version gate and poison with a <queue>.dlq sender ---------------------------------

const GATE_BODY = '{\n  "job": "urn:babelqueue:order.created",\n  "data": { "big": 12345678901234567890, "ratio": 1.50 },\n  "meta": {"schema_version": 2}\n}\n';

/** A `<queue>.dlq` sender that records into a shared event log, or rejects the write. */
class DlqSender implements AsbSender {
  entityPath = "orders.dlq";
  sent: AsbMessage[] = [];
  constructor(private readonly log: string[], private readonly fails = false) {}
  async sendMessages(message: AsbMessage): Promise<void> {
    if (this.fails) throw new Error("dlq down");
    this.log.push("send");
    this.sent.push(message);
  }
  async scheduleMessages(): Promise<bigint[]> {
    return [];
  }
}

/**
 * What `@azure/service-bus` puts on the wire for a sent body (its `dataTransformer.encode`): a Buffer / Uint8Array is
 * the data section as is; every other body, a string included, is `JSON.stringify`ed (a string becomes a quoted literal).
 */
function wireText(body: unknown): string {
  return body instanceof Uint8Array ? Buffer.from(body).toString("utf8") : JSON.stringify(body);
}

/** A receiver that logs how each message is settled, in order. */
function loggingReceiver(message: AsbReceivedMessage, log: string[]): AsbReceiver {
  return {
    async receiveMessages() { return [message]; },
    async completeMessage() { log.push("complete"); },
    async abandonMessage() { log.push("abandon"); },
    async deadLetterMessage() { log.push("deadletter"); },
  };
}

test("gate: with a DLQ sender the raw body goes to <queue>.dlq byte for byte and is completed only after the send", async () => {
  const log: string[] = [];
  const dlq = new DlqSender(log);
  let calls = 0;
  await new AsbConsumer(
    loggingReceiver({ body: GATE_BODY, deliveryCount: 9, applicationProperties: { "bq-schema-version": "2" } }, log),
    { [URN]: () => { calls += 1; } },
    { deadLetterSender: dlq, onError: () => undefined },
  ).poll();
  assert.equal(calls, 0);
  assert.deepEqual(log, ["send", "complete"]);
  assert.ok(dlq.sent[0]!.body instanceof Uint8Array, "bytes, so the SDK does not JSON-encode them into a quoted literal");
  assert.equal(wireText(dlq.sent[0]!.body), GATE_BODY, "byte for byte on the wire");
  assert.ok(!wireText(dlq.sent[0]!.body).includes("dead_letter"));
});

test("gate: a failed DLQ write abandons the message (never completes or natively dead-letters it) and reports it", async () => {
  const log: string[] = [];
  const errors: unknown[] = [];
  await new AsbConsumer(
    loggingReceiver(gated("2"), log),
    {},
    { deadLetterSender: new DlqSender(log, true), onError: (e) => void errors.push(e) },
  ).poll();
  assert.deepEqual(log, ["abandon"]);
  assert.equal(errors.length, 2); // the gate error, then the failed write
  assert.match((errors[1] as Error).message, /dlq down/);
});

test("gate: a hook-only consumer (onUnknownUrn, no DLQ) hands the message to the native mechanism, never completes it", async () => {
  // A hook completes an unknown-URN message; the version gate must not read that as a drop.
  const log: string[] = [];
  let hook = 0;
  await new AsbConsumer(loggingReceiver(gated("2"), log), {}, { onUnknownUrn: () => { hook += 1; }, onError: () => undefined }).poll();
  assert.deepEqual(log, ["deadletter"]);
  assert.equal(hook, 0, "the hook is for an unknown URN only");

  const withDlq: string[] = [];
  const dlq = new DlqSender(withDlq);
  await new AsbConsumer(loggingReceiver(gated("2"), withDlq), {}, { onUnknownUrn: () => { hook += 1; }, deadLetterSender: dlq, onError: () => undefined }).poll();
  assert.deepEqual(withDlq, ["send", "complete"], "DLQ on: dlq_raw");
  assert.equal(hook, 0);
});

test("poison: with a DLQ sender the ADR-0035 record is sent and the message completed only after the send", async () => {
  const original = '{"job": 5, "big": 12345678901234567890}';
  const log: string[] = [];
  const dlq = new DlqSender(log);
  await new AsbConsumer(loggingReceiver({ body: original, deliveryCount: 3 }, log), {}, { deadLetterSender: dlq, onError: () => undefined }).poll();
  assert.deepEqual(log, ["send", "complete"]);
  assert.ok(dlq.sent[0]!.body instanceof Uint8Array);
  const record = wireText(dlq.sent[0]!.body);
  assert.ok(record.startsWith("{"), "an ADR-0035 record on the wire, not a quoted JSON string literal");
  assert.ok(record.startsWith(original.slice(0, -1) + ',"dead_letter":'), record);
  const deadLetter = (JSON.parse(record) as { dead_letter: Record<string, unknown> }).dead_letter;
  assert.equal(deadLetter["reason"], "poison");
  assert.equal(deadLetter["original_queue"], "orders", "the DLQ entity name without its suffix");
  assert.equal(deadLetter["attempts"], 2);

  const bytes = Buffer.from([0xff, 0xfe, 0x00, 0x41]);
  const binLog: string[] = [];
  const binDlq = new DlqSender(binLog);
  await new AsbConsumer(loggingReceiver({ body: bytes, deliveryCount: 1 }, binLog), {}, { deadLetterSender: binDlq, onError: () => undefined }).poll();
  const wrapper = JSON.parse(wireText(binDlq.sent[0]!.body)) as { raw: string; raw_encoding: string };
  assert.equal(wrapper.raw_encoding, "base64");
  assert.deepEqual(Buffer.from(wrapper.raw, "base64"), bytes);
});

test("poison: a failed DLQ write abandons it; without a DLQ sender it is abandoned for the native MaxDeliveryCount", async () => {
  const failed: string[] = [];
  await new AsbConsumer(loggingReceiver({ body: "not-json", deliveryCount: 1 }, failed), {}, { deadLetterSender: new DlqSender(failed, true), onError: () => undefined }).poll();
  assert.deepEqual(failed, ["abandon"]);

  const native: string[] = [];
  await new AsbConsumer(loggingReceiver({ body: "not-json", deliveryCount: 1 }, native), {}, { onUnknownUrn: () => undefined, onError: () => undefined }).poll();
  assert.deepEqual(native, ["abandon"]);
});

test("poison: an onUnknownUrn hook does not apply to a poison body (record written / abandoned, hook not called)", async () => {
  let hook = 0;
  const log: string[] = [];
  const dlq = new DlqSender(log);
  await new AsbConsumer(loggingReceiver({ body: "not-json", deliveryCount: 1 }, log), {}, { onUnknownUrn: () => { hook += 1; }, deadLetterSender: dlq, onError: () => undefined }).poll();
  assert.deepEqual(log, ["send", "complete"]);
  assert.equal(hook, 0);
});

// --- bodies as the real @azure/service-bus receiver delivers them ------------------------------

test("a data-section JSON body that the SDK parsed into an object still routes to its handler", async () => {
  // Default receiver (skipParsingBodyAsJson false): a valid-JSON data section arrives as a parsed object.
  const log: string[] = [];
  let seen: Envelope | null = null;
  await new AsbConsumer(
    loggingReceiver({ body: JSON.parse(EnvelopeCodec.encode(envelope())) as unknown, deliveryCount: 1 }, log),
    { [URN]: (e) => { seen = e; } },
  ).poll();
  assert.equal(seen!.job, URN);
  assert.deepEqual(log, ["complete"]);
});

test("a parsed (object) body that is not an envelope is never completed into a DLQ record: abandoned and reported", async () => {
  const log: string[] = [];
  const dlq = new DlqSender(log);
  const errors: unknown[] = [];
  await new AsbConsumer(
    loggingReceiver({ body: { foo: 1 }, deliveryCount: 1 }, log),
    {},
    { deadLetterSender: dlq, onError: (e) => void errors.push(e) },
  ).poll();
  assert.deepEqual(log, ["abandon"], "no record written, nothing completed");
  assert.equal(dlq.sent.length, 0);
  assert.match((errors[errors.length - 1] as Error).message, /skipParsingBodyAsJson/);
});

test("a version-gated delivery whose body the SDK already parsed is abandoned, not written as a re-encoded record", async () => {
  const log: string[] = [];
  const dlq = new DlqSender(log);
  await new AsbConsumer(
    loggingReceiver({ body: { job: URN }, deliveryCount: 1, applicationProperties: { "bq-schema-version": "2" } }, log),
    {},
    { deadLetterSender: dlq, onError: () => undefined },
  ).poll();
  assert.deepEqual(log, ["abandon"]);
  assert.equal(dlq.sent.length, 0);
});

test("with exact bytes (skipParsingBodyAsJson), a JSON object body that is not an envelope is spliced into the poison record", async () => {
  const original = '{"foo": 1, "big": 12345678901234567890}';
  const log: string[] = [];
  const dlq = new DlqSender(log);
  await new AsbConsumer(loggingReceiver({ body: Buffer.from(original), deliveryCount: 1 }, log), {}, { deadLetterSender: dlq, onError: () => undefined }).poll();
  assert.deepEqual(log, ["send", "complete"]);
  assert.ok(wireText(dlq.sent[0]!.body).startsWith(original.slice(0, -1) + ',"dead_letter":'));
});

// --- publisher: the envelope reaches the wire as the envelope bytes (GR-5) ----------------------

test("publish sends the envelope as bytes: the wire carries the JSON object, not a quoted JSON string", async () => {
  const sender = new FakeSender();
  await new AsbPublisher(sender).publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  const body = sender.sent[0]!.body;
  assert.ok(body instanceof Uint8Array, "a string would be JSON-encoded by the SDK");
  const wire = wireText(body);
  assert.equal(wire.charAt(0), "{");
  assert.equal(EnvelopeCodec.urn(EnvelopeCodec.decode(wire)), URN);
  assert.equal(wireText(toServiceBusMessage(envelope()).body).charAt(0), "{");
});

// --- migration: the double-encoded body of releases before the publisher fix -----------------------

/** The data section the old publisher produced: the envelope string, JSON-encoded again (a quoted literal). */
const legacyWire = (): string => JSON.stringify(EnvelopeCodec.encode(envelope()));

test("legacy double-encoded body, exact bytes (skipParsingBodyAsJson: true): opened once and routed to its handler", async () => {
  const log: string[] = [];
  let seen: Envelope | null = null;
  await new AsbConsumer(
    loggingReceiver({ body: Buffer.from(legacyWire(), "utf8"), deliveryCount: 1 }, log),
    { [URN]: (e) => { seen = e; } },
  ).poll();
  assert.equal(seen!.job, URN);
  assert.deepEqual(log, ["complete"]);
});

test("legacy double-encoded body, default receiver (the SDK parsed the literal into its inner string): routed to its handler", async () => {
  const log: string[] = [];
  let seen: Envelope | null = null;
  // JSON.parse of the quoted literal, as the default receiver does for a data section.
  const parsed = JSON.parse(legacyWire()) as string;
  await new AsbConsumer(loggingReceiver({ body: parsed, deliveryCount: 1 }, log), { [URN]: (e) => { seen = e; } }).poll();
  assert.equal(seen!.job, URN);
  // and a literal that reaches the consumer still quoted (string path) is opened as well
  await new AsbConsumer(loggingReceiver({ body: legacyWire(), deliveryCount: 1 }, log), { [URN]: (e) => { seen = e; } }).poll();
  assert.deepEqual(log, ["complete", "complete"]);
});

test("a quoted JSON string that is not an envelope is not opened: it stays poison with its original bytes in the record", async () => {
  for (const quoted of ['"hello"', JSON.stringify('{"foo":1}'), JSON.stringify(JSON.stringify({ job: 5 })), '"unterminated']) {
    const log: string[] = [];
    const dlq = new DlqSender(log);
    let handled = 0;
    await new AsbConsumer(
      loggingReceiver({ body: Buffer.from(quoted, "utf8"), deliveryCount: 1 }, log),
      { [URN]: () => { handled += 1; } },
      { deadLetterSender: dlq, onError: () => undefined },
    ).poll();
    assert.equal(handled, 0, quoted);
    assert.deepEqual(log, ["send", "complete"], quoted);
    assert.equal((JSON.parse(wireText(dlq.sent[0]!.body)) as { raw: string }).raw, quoted, "the record keeps the original (quoted) bytes");
  }
});

test("a legacy double-encoded body whose content is a version-gated delivery is still settled by the gate, bytes untouched", async () => {
  const log: string[] = [];
  const dlq = new DlqSender(log);
  const bytes = Buffer.from(legacyWire(), "utf8");
  await new AsbConsumer(
    loggingReceiver({ body: bytes, deliveryCount: 1, applicationProperties: { "bq-schema-version": "2" } }, log),
    {},
    { deadLetterSender: dlq, onError: () => undefined },
  ).poll();
  assert.deepEqual(log, ["send", "complete"]);
  assert.equal(wireText(dlq.sent[0]!.body), bytes.toString("utf8"));
});

// --- default receiver (JSON-parsed body) together with a deadLetterSender (ADR-0037 decision 2) -----------------

/**
 * rhea's decoded section (`rhea/lib/message.js`: `new Section(typecode, content)`), which `AmqpAnnotatedMessage.fromRheaMessage`
 * passes through as `_rawAmqpMessage.body`. A data section is typecode 0x75 with the received Buffer as `content`
 * (an array of Buffers when there are several); an amqp-sequence is 0x76. An amqp-value is NOT wrapped by rhea: it is
 * the plain value (a string or a Buffer). `Section(0x77, …)` below is a defensive shape only, not one rhea produces.
 */
class Section {
  constructor(readonly typecode: number, readonly content: unknown) {}
}

/**
 * A delivery as the default `@azure/service-bus` receiver builds it: `body` is the JSON-parsed view of the data section,
 * and `_rawAmqpMessage.body` is rhea's `Section { typecode: 0x75, content: Buffer }` holding the exact bytes received.
 */
function parsedDelivery(raw: string, extra: Partial<AsbReceivedMessage> = {}): AsbReceivedMessage {
  return {
    body: JSON.parse(raw) as unknown,
    _rawAmqpMessage: { body: new Section(0x75, Buffer.from(raw, "utf8")), bodyType: "data" },
    deliveryCount: 1,
    ...extra,
  };
}

test("default receiver + deadLetterSender: a poison body that the SDK parsed is written to <queue>.dlq from the raw bytes and completed", async () => {
  const original = '{ "foo": 1,\n  "big": 12345678901234567890, "ratio": 1.50 }';
  const log: string[] = [];
  const dlq = new DlqSender(log);
  await new AsbConsumer(loggingReceiver(parsedDelivery(original), log), {}, { deadLetterSender: dlq, onError: () => undefined }).poll();

  assert.deepEqual(log, ["send", "complete"], "the BabelQueue DLQ, never the native sub-queue (no abandon / dead-letter)");
  const record = wireText(dlq.sent[0]!.body);
  assert.ok(record.startsWith(original.slice(0, -1) + ',"dead_letter":'), record);
  assert.ok(record.includes("12345678901234567890") && record.includes("1.50"), "no re-encoding: number forms and the big integer survive");
  assert.equal((JSON.parse(record) as { dead_letter: { reason: string } }).dead_letter.reason, "poison");
});

test("default receiver + deadLetterSender: a version-gated delivery goes to <queue>.dlq as the raw bytes and is completed", async () => {
  const original = '{\n  "job": "urn:babelqueue:order.created",\n  "data": { "big": 12345678901234567890 },\n  "meta": {"schema_version": 2}\n}\n';
  const log: string[] = [];
  const dlq = new DlqSender(log);
  await new AsbConsumer(
    loggingReceiver(parsedDelivery(original, { applicationProperties: { "bq-schema-version": "2" } }), log),
    {},
    { deadLetterSender: dlq, onError: () => undefined },
  ).poll();
  assert.deepEqual(log, ["send", "complete"]);
  assert.equal(wireText(dlq.sent[0]!.body), original, "byte for byte");
});

test("default receiver: an envelope is decoded from the raw section bytes and routed to its handler", async () => {
  const raw = EnvelopeCodec.encode(envelope());
  const log: string[] = [];
  let seen: Envelope | null = null;
  // the parsed view is deliberately unusable: only the raw Section carries the envelope text
  await new AsbConsumer(
    loggingReceiver({ body: undefined, _rawAmqpMessage: { body: new Section(0x75, Buffer.from(raw, "utf8")), bodyType: "data" }, deliveryCount: 1 }, log),
    { [URN]: (e) => { seen = e; } },
  ).poll();
  assert.equal(seen!.job, URN);
  assert.deepEqual(log, ["complete"]);
});

test("rhea section shapes: several data sections are concatenated; a plain amqp-value string / Buffer is read as is (defensive Section(0x77) too)", async () => {
  const original = '{ "foo": 1, "big": 12345678901234567890 }';
  const cases: Array<[string, unknown]> = [
    ["data sections (array of Buffers)", new Section(0x75, [Buffer.from(original.slice(0, 10)), Buffer.from(original.slice(10))])],
    ["amqp-value string (plain value, as rhea delivers it)", original],
    ["amqp-value binary (plain Buffer, as rhea delivers it)", Buffer.from(original)],
    ["defensive: Section(0x77) with a string content", new Section(0x77, original)],
    ["defensive: Section(0x77) with a binary content", new Section(0x77, Buffer.from(original))],
  ];
  for (const [name, body] of cases) {
    const log: string[] = [];
    const dlq = new DlqSender(log);
    await new AsbConsumer(
      loggingReceiver({ body: { foo: 1 }, _rawAmqpMessage: { body }, deliveryCount: 1 }, log),
      {},
      { deadLetterSender: dlq, onError: () => undefined },
    ).poll();
    assert.deepEqual(log, ["send", "complete"], name);
    assert.ok(wireText(dlq.sent[0]!.body).startsWith(original.slice(0, -1) + ',"dead_letter":'), name);
  }
});

test("unrecognised raw shapes keep the safe path: reported and abandoned, never completed", async () => {
  const unknown: Array<[string, unknown]> = [
    ["amqp-sequence section", new Section(0x76, [[1, 2]])],
    ["amqp-value of a non-text value (defensive Section(0x77))", new Section(0x77, { map: true })],
    ["amqp-value of a non-text plain value (a map)", { map: true }],
    ["a data section with a non-binary content", new Section(0x75, "text")],
    ["a data-section array with a non-binary part", new Section(0x75, [Buffer.from("{}"), 5])],
    ["an empty data-section array", new Section(0x75, [])],
    ["an object without a typecode", { content: Buffer.from("{}") }],
  ];
  for (const [name, body] of unknown) {
    const log: string[] = [];
    const dlq = new DlqSender(log);
    const errors: unknown[] = [];
    await new AsbConsumer(
      loggingReceiver({ body: { foo: 1 }, _rawAmqpMessage: { body }, deliveryCount: 1 }, log),
      {},
      { deadLetterSender: dlq, onError: (e) => void errors.push(e) },
    ).poll();
    assert.deepEqual(log, ["abandon"], name);
    assert.equal(dlq.sent.length, 0, name);
    assert.match((errors[errors.length - 1] as Error).message, /no raw AMQP body/, name);
  }
});

test("default receiver + deadLetterSender without any raw view: still abandoned and reported, never completed (no byte-faithful record possible)", async () => {
  const log: string[] = [];
  const dlq = new DlqSender(log);
  const errors: unknown[] = [];
  await new AsbConsumer(loggingReceiver({ body: { foo: 1 }, deliveryCount: 1 }, log), {}, { deadLetterSender: dlq, onError: (e) => void errors.push(e) }).poll();
  assert.deepEqual(log, ["abandon"]);
  assert.match((errors[errors.length - 1] as Error).message, /no raw AMQP body/);
});
