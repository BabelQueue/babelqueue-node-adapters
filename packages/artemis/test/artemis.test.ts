import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  ArtemisConsumer,
  ArtemisPublisher,
  artemisMessage,
  messageBody,
  JMS_TYPE_KEY,
  SCHEDULED_DELIVERY_KEY,
  type AmqpDelivery,
  type AmqpEventContext,
  type AmqpMessage,
  type AmqpReceiver,
  type AmqpSender,
  type BabelHandlers,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

function incoming(env: Envelope, opts: { jmsType?: string | null; deliveryCount?: number } = {}): AmqpMessage {
  const message: AmqpMessage = { body: EnvelopeCodec.encode(env) };
  if (opts.deliveryCount) message.delivery_count = opts.deliveryCount;
  const jt = opts.jmsType === undefined ? env.job : opts.jmsType;
  if (jt != null) message.message_annotations = { [JMS_TYPE_KEY]: jt };
  return message;
}

class FakeSender implements AmqpSender {
  sent: AmqpMessage[] = [];
  send(message: AmqpMessage): unknown {
    this.sent.push(message);
    return {};
  }
}

class FakeDelivery implements AmqpDelivery {
  accepted = 0;
  released: unknown[] = [];
  accept(): void {
    this.accepted += 1;
  }
  release(params?: { delivery_failed?: boolean }): void {
    this.released.push(params ?? {});
  }
}

class FakeReceiver implements AmqpReceiver {
  handler?: (context: AmqpEventContext) => unknown;
  on(_event: "message", handler: (context: AmqpEventContext) => void): unknown {
    this.handler = handler;
    return this;
  }
}

function context(message: AmqpMessage, delivery = new FakeDelivery()): { context: AmqpEventContext; delivery: FakeDelivery } {
  return { context: { message, delivery }, delivery };
}

// --- projection ---------------------------------------------------------------

test("artemisMessage projects body, correlation, jms-type annotation and bq_ properties", () => {
  const env = envelope(2);
  const message = artemisMessage(env);

  assert.equal(EnvelopeCodec.decode(messageBody(message)).job, URN);
  assert.equal(message.correlation_id, "trace-1");
  assert.equal(message.creation_time, env.meta.created_at);
  assert.equal(message.message_annotations?.[JMS_TYPE_KEY], URN);
  assert.equal(message.application_properties?.["bq_schema_version"], "1");
  assert.equal(message.application_properties?.["bq_source_lang"], env.meta.lang);
  assert.equal(message.application_properties?.["bq_attempts"], "2");
  assert.equal(message.application_properties?.["bq_app_id"], "babelqueue");
});

test("artemisMessage with a delay sets bq_delay and the scheduled-delivery annotation", () => {
  const message = artemisMessage(envelope(), 30000);
  assert.equal(message.application_properties?.["bq_delay"], "30000");
  assert.ok(SCHEDULED_DELIVERY_KEY in (message.message_annotations ?? {}));
});

test("messageBody decodes string, Buffer, rhea Data section and null", () => {
  assert.equal(messageBody({ body: "hello" }), "hello");
  assert.equal(messageBody({ body: Buffer.from("hi", "utf8") }), "hi");
  assert.equal(messageBody({ body: { content: Buffer.from("data", "utf8") } }), "data");
  assert.equal(messageBody({ body: undefined }), "");
});

// --- publisher ----------------------------------------------------------------

test("publish sends the projected message and returns the message id", async () => {
  const sender = new FakeSender();
  const id = await ArtemisPublisher.create(sender, "orders").publish(URN, { order_id: 7 }, { traceId: "trace-1" });

  assert.equal(sender.sent.length, 1);
  const decoded = EnvelopeCodec.decode(messageBody(sender.sent[0]!)) as Envelope;
  assert.equal(decoded.job, URN);
  assert.equal(decoded.meta.queue, "orders");
  assert.equal(decoded.meta.id, id);
  assert.equal(sender.sent[0]!.message_annotations?.[JMS_TYPE_KEY], URN);
});

test("publish with a delay carries bq_delay", async () => {
  const sender = new FakeSender();
  await ArtemisPublisher.create(sender, "orders").publish(URN, {}, { delayMs: 15000 });
  assert.equal(sender.sent[0]!.application_properties?.["bq_delay"], "15000");
});

// --- consumer: success + routing + reconcile ----------------------------------

test("success accepts and exposes attempts from the delivery-count", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(0), { deliveryCount: 3 }));
  let seen = -1;
  const handlers: BabelHandlers = { [URN]: (env) => { seen = env.attempts ?? -1; } };

  await new ArtemisConsumer(handlers, { maxTries: 99 }).handle(ctx);

  assert.equal(seen, 3); // max(body 0, delivery-count 3) — no −1
  assert.equal(delivery.accepted, 1);
});

test("body attempt count is never lowered by a smaller delivery-count", async () => {
  const { context: ctx } = context(incoming(envelope(5), { deliveryCount: 2 }));
  let seen = -1;
  await new ArtemisConsumer({ [URN]: (env) => { seen = env.attempts ?? -1; } }, { maxTries: 99 }).handle(ctx);
  assert.equal(seen, 5);
});

test("routes by the body URN when the annotation is absent", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(0), { jmsType: null, deliveryCount: 0 }));
  let handled = false;
  await new ArtemisConsumer({ [URN]: () => { handled = true; } }).handle(ctx);
  assert.ok(handled);
  assert.equal(delivery.accepted, 1);
});

// --- consumer: retry / DLQ ----------------------------------------------------

test("a throwing handler releases for redelivery (not accepted)", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(0), { deliveryCount: 0 }));
  let reported: unknown;
  const options = { maxTries: 3, onError: (e: unknown) => { reported = e; } };

  await new ArtemisConsumer({ [URN]: () => { throw new Error("boom"); } }, options).handle(ctx);

  assert.ok(reported instanceof Error);
  assert.equal(delivery.released.length, 1);
  assert.deepEqual(delivery.released[0], { delivery_failed: true });
  assert.equal(delivery.accepted, 0);
});

test("a terminal failure dead-letters with a dead_letter block and accepts", async () => {
  const dlq = new FakeSender();
  const { context: ctx, delivery } = context(incoming(envelope(2), { deliveryCount: 0 })); // attempts 2, next 3 == maxTries
  const options = { maxTries: 3, deadLetterSender: dlq };

  await new ArtemisConsumer({ [URN]: () => { throw new Error("boom"); } }, options).handle(ctx);

  assert.equal(dlq.sent.length, 1);
  assert.equal(delivery.accepted, 1);
  assert.equal((EnvelopeCodec.decode(messageBody(dlq.sent[0]!)) as Envelope).dead_letter!.reason, "failed");
});

test("a terminal failure with no DLQ drops and accepts", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(2), { deliveryCount: 0 }));
  await new ArtemisConsumer({ [URN]: () => { throw new Error("boom"); } }, { maxTries: 3 }).handle(ctx);
  assert.equal(delivery.accepted, 1);
  assert.equal(delivery.released.length, 0);
});

test("a non-conformant message is forwarded raw to the DLQ and accepted", async () => {
  const dlq = new FakeSender();
  const { context: ctx, delivery } = context({ body: "not-json" });
  let reported: unknown;
  const options = { deadLetterSender: dlq, onError: (e: unknown) => { reported = e; } };

  await new ArtemisConsumer({}, options).handle(ctx);

  assert.equal(dlq.sent.length, 1);
  assert.equal(messageBody(dlq.sent[0]!), "not-json");
  assert.equal(delivery.accepted, 1);
  assert.ok(reported);
});

// --- consumer: unknown URN ----------------------------------------------------

test("unknown URN with fail throws and does not settle", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(0)));
  await assert.rejects(new ArtemisConsumer({}).handle(ctx), UnknownUrnError);
  assert.equal(delivery.accepted, 0);
  assert.equal(delivery.released.length, 0);
});

test("unknown URN with delete accepts", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(0)));
  await new ArtemisConsumer({}, { unknownUrn: UnknownUrnStrategy.DELETE }).handle(ctx);
  assert.equal(delivery.accepted, 1);
});

test("unknown URN with release releases", async () => {
  const { context: ctx, delivery } = context(incoming(envelope(0)));
  await new ArtemisConsumer({}, { unknownUrn: UnknownUrnStrategy.RELEASE }).handle(ctx);
  assert.equal(delivery.released.length, 1);
});

test("unknown URN with dead_letter dead-letters and accepts", async () => {
  const dlq = new FakeSender();
  const { context: ctx, delivery } = context(incoming(envelope(0)));
  const options = { unknownUrn: UnknownUrnStrategy.DEAD_LETTER, deadLetterSender: dlq };

  await new ArtemisConsumer({}, options).handle(ctx);

  assert.equal(dlq.sent.length, 1);
  assert.equal(delivery.accepted, 1);
  assert.equal((EnvelopeCodec.decode(messageBody(dlq.sent[0]!)) as Envelope).dead_letter!.reason, "unknown_urn");
});

// --- listen wiring ------------------------------------------------------------

test("listen wires the receiver's message event to handle", async () => {
  const receiver = new FakeReceiver();
  const { context: ctx, delivery } = context(incoming(envelope(0)));
  new ArtemisConsumer({ [URN]: () => {} }).listen(receiver);

  assert.ok(receiver.handler);
  await receiver.handler!(ctx);
  assert.equal(delivery.accepted, 1);
});

test("handle tolerates a context with no delivery", async () => {
  await new ArtemisConsumer({ [URN]: () => {} }).handle({ message: incoming(envelope(0)) }); // no throw
});
