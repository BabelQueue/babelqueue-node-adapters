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

// --- consumer: schema-version gate (contract §7.7) ---------------------------

function gated(version: unknown, opts: { absent?: boolean } = {}): AmqpMessage {
  const message = incoming(envelope(0));
  if (!opts.absent) message.application_properties = { bq_schema_version: version };
  return message;
}

test("gate: an absent or blank bq_schema_version follows the normal decode path", async () => {
  const cases: AmqpMessage[] = [
    gated(undefined, { absent: true }),
    gated(""),
    gated(" \t"),
    gated("\n\u000b\f\r"),
    gated(null),
  ];
  for (const message of cases) {
    const { context: ctx, delivery } = context(message);
    let calls = 0;
    await new ArtemisConsumer({ [URN]: () => void (calls += 1) }).handle(ctx);
    assert.equal(calls, 1);
    assert.equal(delivery.accepted, 1);
  }
});

test("gate: \"1\" is decoded and the handler runs", async () => {
  const { context: ctx, delivery } = context(gated("1"));
  let calls = 0;
  await new ArtemisConsumer({ [URN]: () => void (calls += 1) }).handle(ctx);
  assert.equal(calls, 1);
  assert.equal(delivery.accepted, 1);
});

test("gate: an unsupported version is forwarded raw to the DLQ and accepted without decoding", async () => {
  for (const version of ["2", "x", "01", " 1", "\u00a0", "\ufeff"]) {
    const dlq = new FakeSender();
    const message = gated(version);
    const { context: ctx, delivery } = context(message);
    let calls = 0;
    const errors: unknown[] = [];
    await new ArtemisConsumer(
      { [URN]: () => void (calls += 1) },
      { deadLetterSender: dlq, onError: (e, env) => void errors.push([e, env]) },
    ).handle(ctx);

    assert.equal(calls, 0, JSON.stringify(version));
    assert.equal(dlq.sent.length, 1);
    assert.equal(messageBody(dlq.sent[0]!), messageBody(message));
    assert.equal(delivery.accepted, 1);
    assert.equal(delivery.released.length, 0);
    assert.equal(errors.length, 1);
    assert.equal((errors[0] as unknown[])[1], null);
  }
});

test("gate: without a DLQ sender the rejected message is only reported and accepted", async () => {
  const { context: ctx, delivery } = context(gated("2"));
  let calls = 0;
  let errors = 0;
  await new ArtemisConsumer({ [URN]: () => void (calls += 1) }, { onError: () => void (errors += 1) }).handle(ctx);
  assert.equal(calls, 0);
  assert.equal(errors, 1);
  assert.equal(delivery.accepted, 1);
});

/** A rhea-like sender: `send` returns a delivery and the broker's verdict arrives later as a sender event. */
class EventSender implements AmqpSender {
  sent: AmqpMessage[] = [];
  forwarded: [string, any][] = [];
  session = { dispatch: (event: string, context?: any): unknown => void this.forwarded.push([event, context]) };
  private listeners = new Map<string, Set<(context: any) => void>>();
  constructor(private readonly verdict: string | null, private readonly throwOnSend = false) {}
  on(event: string, handler: (context: any) => void): unknown {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    this.listeners.get(event)!.add(handler);
    return this;
  }
  removeListener(event: string, handler: (context: any) => void): unknown {
    this.listeners.get(event)?.delete(handler);
    return this;
  }
  emit(event: string, context: any): void {
    for (const handler of [...(this.listeners.get(event) ?? [])]) handler(context);
  }
  listenerCount(): number {
    let n = 0;
    for (const set of this.listeners.values()) n += set.size;
    return n;
  }
  send(message: AmqpMessage): unknown {
    if (this.throwOnSend) throw new Error("send failed");
    this.sent.push(message);
    const delivery = { settled: false };
    if (this.verdict !== null) {
      setImmediate(() => this.emit(this.verdict!, { delivery }));
    }
    return delivery;
  }
}

test("gate: the source message is accepted only after the broker accepts the DLQ delivery", async () => {
  const dlq = new EventSender("accepted");
  const { context: ctx, delivery } = context(gated("2"));
  let calls = 0;
  await new ArtemisConsumer({ [URN]: () => void (calls += 1) }, { deadLetterSender: dlq }).handle(ctx);
  assert.equal(calls, 0);
  assert.equal(dlq.sent.length, 1);
  assert.equal(delivery.accepted, 1);
  assert.equal(delivery.released.length, 0);
  assert.equal(dlq.listenerCount(), 0);
});

test("gate: a refused DLQ delivery releases the source message, is reported, and never reaches a handler", async () => {
  for (const verdict of ["rejected", "released", "modified", "sender_close", "sender_error"]) {
    const dlq = new EventSender(verdict);
    const { context: ctx, delivery } = context(gated("2"));
    let calls = 0;
    const errors: unknown[] = [];
    await new ArtemisConsumer({ [URN]: () => void (calls += 1) }, { deadLetterSender: dlq, deadLetterRetryDelayMs: 0, onError: (e) => void errors.push(e) }).handle(ctx);
    assert.equal(calls, 0, verdict);
    assert.equal(delivery.accepted, 0, verdict);
    assert.deepEqual(delivery.released, [{}], verdict); // plain release: no delivery_failed
    assert.equal(errors.length, 2, verdict); // the gate report + the DLQ failure
    assert.equal(dlq.listenerCount(), 0, verdict);
  }
});

test("gate: a throwing DLQ send releases the source message instead of accepting it", async () => {
  const { context: ctx, delivery } = context(gated("2"));
  let calls = 0;
  await new ArtemisConsumer({ [URN]: () => void (calls += 1) }, { deadLetterSender: new EventSender(null, true), deadLetterRetryDelayMs: 0 }).handle(ctx);
  assert.equal(calls, 0);
  assert.equal(delivery.accepted, 0);
  assert.deepEqual(delivery.released, [{}]);
});

test("gate: a plain sender whose send throws releases the source message instead of accepting it", async () => {
  const dlq = new FakeSender();
  dlq.send = () => {
    throw new Error("send failed");
  };
  const { context: ctx, delivery } = context(gated("2"));
  await new ArtemisConsumer({}, { deadLetterSender: dlq, deadLetterRetryDelayMs: 0 }).handle(ctx);
  assert.equal(delivery.accepted, 0);
  assert.deepEqual(delivery.released, [{}]);
});

test("gate: no broker verdict before deadLetterTimeoutMs releases the source message", async () => {
  const dlq = new EventSender(null);
  const { context: ctx, delivery } = context(gated("2"));
  let calls = 0;
  await new ArtemisConsumer({ [URN]: () => void (calls += 1) }, { deadLetterSender: dlq, deadLetterTimeoutMs: 20, deadLetterRetryDelayMs: 0 }).handle(ctx);
  assert.equal(calls, 0);
  assert.equal(delivery.accepted, 0);
  assert.deepEqual(delivery.released, [{}]);
  assert.equal(dlq.listenerCount(), 0);
});

test("gate: a failed DLQ write never uses delivery_failed and waits deadLetterRetryDelayMs before releasing", async () => {
  const { context: ctx, delivery } = context(gated("2"));
  const pending = new ArtemisConsumer({}, { deadLetterSender: new EventSender("rejected"), deadLetterRetryDelayMs: 60 }).handle(ctx);
  await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(delivery.released.length, 0, "no hot-loop release before the backoff elapsed");
  assert.equal(delivery.accepted, 0);
  await pending;
  assert.deepEqual(delivery.released, [{}]);
  assert.ok(delivery.released.every((p) => (p as { delivery_failed?: boolean }).delivery_failed === undefined));
});

test("gate: a throwing onError cannot skip the settlement (accept on success, release on DLQ failure)", async () => {
  const boom = (): void => {
    throw new Error("onError exploded");
  };
  const ok = context(gated("2"));
  await new ArtemisConsumer({}, { deadLetterSender: new EventSender("accepted"), onError: boom }).handle(ok.context);
  assert.equal(ok.delivery.accepted, 1);

  const bad = context(gated("2"));
  await new ArtemisConsumer({}, { deadLetterSender: new EventSender("rejected"), deadLetterRetryDelayMs: 0, onError: boom }).handle(bad.context);
  assert.equal(bad.delivery.accepted, 0);
  assert.deepEqual(bad.delivery.released, [{}]);
});

test("gate: events that are not this delivery's verdict are re-dispatched to the session, and link close too", async () => {
  const dlq = new EventSender(null);
  const { context: ctx, delivery } = context(gated("2"));
  const pending = new ArtemisConsumer({}, { deadLetterSender: dlq, deadLetterRetryDelayMs: 0 }).handle(ctx);
  const other = { settled: false };
  dlq.emit("accepted", { delivery: other });
  assert.deepEqual(dlq.forwarded, [["accepted", { delivery: other }]]);
  assert.equal(delivery.accepted, 0, "another delivery's accepted must not confirm ours");
  dlq.emit("sender_close", { sender: "x" });
  await pending;
  assert.deepEqual(dlq.forwarded[1], ["sender_close", { sender: "x" }]);
  assert.deepEqual(delivery.released, [{}]);
  assert.equal(dlq.listenerCount(), 0);
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

test("gate: two concurrent gate rejections on one DLQ sender forward each foreign/close event exactly once", async () => {
  const dlq = new EventSender(null);
  const a = context(gated("2"));
  const b = context(gated("2"));
  const consumer = new ArtemisConsumer({}, { deadLetterSender: dlq, deadLetterRetryDelayMs: 0 });
  const first = consumer.handle(a.context);
  const second = consumer.handle(b.context);
  assert.equal(dlq.sent.length, 2);
  assert.equal(dlq.listenerCount(), 6, "one shared listener per event, not one set per pending confirmation");

  const foreign = { settled: false };
  dlq.emit("accepted", { delivery: foreign });
  assert.equal(dlq.forwarded.length, 1, "a foreign verdict is forwarded once with two confirmations pending");
  assert.equal(a.delivery.accepted + b.delivery.accepted, 0);

  dlq.emit("sender_close", { sender: "x" });
  await Promise.all([first, second]);
  assert.equal(dlq.forwarded.length, 2, "sender_close is forwarded once");
  assert.deepEqual(dlq.forwarded[1], ["sender_close", { sender: "x" }]);
  assert.deepEqual(a.delivery.released, [{}]);
  assert.deepEqual(b.delivery.released, [{}]);
  assert.equal(dlq.listenerCount(), 0);
});

test("gate: concurrent rejections settle independently by delivery", async () => {
  const dlq = new EventSender(null);
  const sentDeliveries: { settled: boolean }[] = [];
  const origSend = dlq.send.bind(dlq);
  dlq.send = ((m: AmqpMessage) => {
    const d = origSend(m) as { settled: boolean };
    sentDeliveries.push(d);
    return d;
  }) as typeof dlq.send;
  const a = context(gated("2"));
  const b = context(gated("2"));
  const consumer = new ArtemisConsumer({}, { deadLetterSender: dlq, deadLetterRetryDelayMs: 0 });
  const first = consumer.handle(a.context);
  const second = consumer.handle(b.context);
  dlq.emit("accepted", { delivery: sentDeliveries[0] });
  dlq.emit("rejected", { delivery: sentDeliveries[1] });
  await Promise.all([first, second]);
  assert.equal(a.delivery.accepted, 1);
  assert.equal(b.delivery.accepted, 0);
  assert.deepEqual(b.delivery.released, [{}]);
  assert.equal(dlq.forwarded.length, 0);
  assert.equal(dlq.listenerCount(), 0);
});
