import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  RabbitMQConsumer,
  RabbitMQPublisher,
  amqpProperties,
  type AmqpChannel,
  type AmqpMessage,
  type AmqpProperties,
  type BabelHandlers,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

function incoming(env: Envelope): AmqpMessage {
  return {
    content: Buffer.from(EnvelopeCodec.encode(env), "utf8"),
    properties: amqpProperties(env),
    fields: { deliveryTag: 1 },
  };
}

/** A fake amqplib channel that records sends + acks; `get` serves queued messages. */
class FakeChannel implements AmqpChannel {
  sent: { queue: string; body: string; options?: AmqpProperties }[] = [];
  acked = 0;
  private queued: (AmqpMessage | false)[];

  constructor(queued: (AmqpMessage | false)[] = []) {
    this.queued = [...queued];
  }

  sendToQueue(queue: string, content: Buffer, options?: AmqpProperties): boolean {
    this.sent.push({ queue, body: content.toString("utf8"), options });
    return true;
  }
  async get(): Promise<AmqpMessage | false> {
    return this.queued.length ? this.queued.shift()! : false;
  }
  ack(): void {
    this.acked += 1;
  }
}

// --- publisher ----------------------------------------------------------------

test("publish sends the envelope with the §2 projected properties", async () => {
  const channel = new FakeChannel();
  const id = await RabbitMQPublisher.create(channel, "orders").publish(URN, { order_id: 7 }, { traceId: "trace-1" });

  assert.equal(channel.sent.length, 1);
  const sent = channel.sent[0]!;
  assert.equal(sent.queue, "orders");
  assert.equal(sent.options!.type, URN);
  assert.equal(sent.options!.correlationId, "trace-1");
  assert.equal(sent.options!.appId, "babelqueue");
  const decoded = EnvelopeCodec.decode(sent.body) as Envelope;
  assert.equal(decoded.meta.id, id);
});

// --- consumer -----------------------------------------------------------------

test("poll gets, routes by type and acks on success", async () => {
  const channel = new FakeChannel([incoming(envelope(0))]);
  let seen: Envelope | null = null;
  const handlers: BabelHandlers = { [URN]: (e) => { seen = e; } };

  const handled = await new RabbitMQConsumer(channel, "orders", handlers).poll();

  assert.ok(handled);
  assert.equal(seen!.job, URN);
  assert.equal(channel.acked, 1);
});

test("poll returns false when the queue is empty", async () => {
  assert.equal(await new RabbitMQConsumer(new FakeChannel([]), "orders", {}).poll(), false);
});

test("a throwing handler requeues with attempts + 1 and acks the original", async () => {
  const channel = new FakeChannel();
  let reported: unknown;
  const options = { maxTries: 3, onError: (e: unknown) => { reported = e; } };

  await new RabbitMQConsumer(channel, "orders", { [URN]: () => { throw new Error("boom"); } }, options).handle(
    incoming(envelope(0)),
  );

  assert.ok(reported instanceof Error);
  const requeue = channel.sent.find((s) => s.queue === "orders");
  assert.equal((EnvelopeCodec.decode(requeue!.body) as Envelope).attempts, 1);
  assert.equal(channel.acked, 1);
});

test("a terminal failure dead-letters to <queue>.dlq with a dead_letter block", async () => {
  const channel = new FakeChannel();
  await new RabbitMQConsumer(channel, "orders", { [URN]: () => { throw new Error("boom"); } }, { maxTries: 3 }).handle(
    incoming(envelope(2)),
  );
  const dlq = channel.sent.find((s) => s.queue === "orders.dlq");
  assert.equal((EnvelopeCodec.decode(dlq!.body) as Envelope).dead_letter!.reason, "failed");
  assert.equal(channel.acked, 1);
});

test("a non-conformant message is forwarded raw to the DLQ and acked", async () => {
  const channel = new FakeChannel();
  let reported: unknown;
  await new RabbitMQConsumer(channel, "orders", {}, { onError: (e: unknown) => { reported = e; } }).handle({
    content: Buffer.from("not-json", "utf8"),
  });
  assert.ok(reported);
  const dlq = channel.sent.find((s) => s.queue === "orders.dlq");
  assert.equal(dlq!.body, "not-json");
  assert.equal(channel.acked, 1);
});

test("routes by the body URN when the type property is absent", async () => {
  const channel = new FakeChannel();
  let handled = false;
  await new RabbitMQConsumer(channel, "orders", { [URN]: () => { handled = true; } }).handle({
    content: Buffer.from(EnvelopeCodec.encode(envelope(0)), "utf8"),
  });
  assert.ok(handled);
  assert.equal(channel.acked, 1);
});

// --- unknown URN --------------------------------------------------------------

test("unknown URN with fail throws and does not ack", async () => {
  const channel = new FakeChannel();
  await assert.rejects(new RabbitMQConsumer(channel, "orders", {}).handle(incoming(envelope())), UnknownUrnError);
  assert.equal(channel.acked, 0);
});

test("unknown URN with delete acks without routing", async () => {
  const channel = new FakeChannel();
  await new RabbitMQConsumer(channel, "orders", {}, { unknownUrn: UnknownUrnStrategy.DELETE }).handle(incoming(envelope()));
  assert.equal(channel.acked, 1);
  assert.equal(channel.sent.length, 0);
});

test("unknown URN with dead_letter dead-letters and acks", async () => {
  const channel = new FakeChannel();
  await new RabbitMQConsumer(channel, "orders", {}, { unknownUrn: UnknownUrnStrategy.DEAD_LETTER }).handle(
    incoming(envelope()),
  );
  const dlq = channel.sent.find((s) => s.queue === "orders.dlq");
  assert.equal((EnvelopeCodec.decode(dlq!.body) as Envelope).dead_letter!.reason, "unknown_urn");
  assert.equal(channel.acked, 1);
});

test("run stops when the supplier returns false", async () => {
  const channel = new FakeChannel();
  await new RabbitMQConsumer(channel, "orders", {}).run(() => false);
  assert.equal(channel.acked, 0);
});
