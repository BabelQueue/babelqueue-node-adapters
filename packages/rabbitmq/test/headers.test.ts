import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import {
  RabbitMQConsumer,
  RabbitMQPublisher,
  amqpProperties,
  headersOf,
  type AmqpChannel,
  type AmqpMessage,
  type AmqpProperties,
  type BabelHandlers,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

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

// --- inject: merge beside the contract x-* (contract wins) --------------------

test("amqpProperties merges a traceparent into the header table beside the x-* headers", () => {
  const props = amqpProperties(envelope(), { traceparent: TRACEPARENT, tracestate: "vendor=v" });
  assert.equal(props.headers!["traceparent"], TRACEPARENT);
  assert.equal(props.headers!["tracestate"], "vendor=v");
  // contract headers still present
  assert.equal(props.headers!["x-attempts"], 0);
});

test("a contract x-* header always wins a key collision (merge-not-clobber)", () => {
  const props = amqpProperties(envelope(3), { "x-attempts": "999", traceparent: TRACEPARENT } as HeaderCarrier);
  assert.equal(props.headers!["x-attempts"], 3); // the envelope value wins, not the rider's "999"
  assert.equal(props.headers!["traceparent"], TRACEPARENT);
});

test("a header-less publish carries no rider keys (only the contract x-*)", async () => {
  const channel = new FakeChannel();
  await RabbitMQPublisher.create(channel, "orders").publish(URN, { order_id: 7 });
  const headers = channel.sent[0]!.options!.headers!;
  assert.ok(!("traceparent" in headers));
});

test("publish with a traceparent puts it on the wire header table", async () => {
  const channel = new FakeChannel();
  await RabbitMQPublisher.create(channel, "orders").publish(URN, { order_id: 7 }, {
    headers: { traceparent: TRACEPARENT },
  });
  assert.equal(channel.sent[0]!.options!.headers!["traceparent"], TRACEPARENT);
});

// --- extract: headersOf round-trip + defensive stringify ----------------------

test("headersOf reads the inbound header table back into a Record<string,string>", () => {
  const props = amqpProperties(envelope(), { traceparent: TRACEPARENT });
  const message: AmqpMessage = { content: Buffer.from("{}"), properties: props };
  const headers = headersOf(message);
  assert.equal(headers["traceparent"], TRACEPARENT);
});

test("headersOf stringifies typed AMQP values and drops nulls; empty when no table", () => {
  const message: AmqpMessage = {
    content: Buffer.from("{}"),
    properties: { headers: { "x-attempts": 5, n: null, buf: Buffer.from("00-buf"), empty: "" } as Record<string, unknown> },
  };
  const headers = headersOf(message);
  assert.equal(headers["x-attempts"], "5");
  assert.equal(headers["buf"], "00-buf");
  assert.ok(!("n" in headers));
  assert.ok(!("empty" in headers));
  assert.deepEqual(headersOf({ content: Buffer.from("{}") }), {});
});

// --- consume surfaces headers to the handler ---------------------------------

test("the consumer surfaces the carried traceparent to the handler", async () => {
  const env = envelope();
  const message: AmqpMessage = {
    content: Buffer.from(EnvelopeCodec.encode(env), "utf8"),
    properties: amqpProperties(env, { traceparent: TRACEPARENT }),
    fields: { deliveryTag: 1 },
  };
  const channel = new FakeChannel([message]);
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _m, headers) => { seen = headers; } };

  await new RabbitMQConsumer(channel, "orders", handlers).poll();

  assert.equal(seen!["traceparent"], TRACEPARENT);
  assert.equal(channel.acked, 1);
});

// --- broker-gated integration (skips cleanly without RabbitMQ) ----------------

const AMQP_URL = process.env.BABELQUEUE_AMQP_URL;

test("integration: a published traceparent arrives on consume", { skip: !AMQP_URL }, async () => {
  // Wired only when BABELQUEUE_AMQP_URL points at a real RabbitMQ. amqplib is imported lazily via a
  // non-literal specifier so the optional peer is required neither to typecheck nor to run the
  // (skipped) suite when it is absent.
  const specifier = "amqplib";
  const amqp = (await import(specifier)) as unknown as {
    connect(url: string): Promise<{ createChannel(): Promise<AmqpChannel & { assertQueue(q: string): Promise<unknown> }>; close(): Promise<void> }>;
  };
  const conn = await amqp.connect(AMQP_URL!);
  const channel = await conn.createChannel();
  const queue = `bq-otel-it-${Date.now()}`;
  await channel.assertQueue(queue);
  try {
    await RabbitMQPublisher.create(channel, queue).publish(URN, { order_id: 7 }, {
      headers: { traceparent: TRACEPARENT },
    });
    let received: HeaderCarrier | null = null;
    await new RabbitMQConsumer(channel, queue, { [URN]: (_e, _m, headers) => { received = headers; } }).poll();
    assert.equal(received!["traceparent"], TRACEPARENT);
  } finally {
    await conn.close();
  }
});
