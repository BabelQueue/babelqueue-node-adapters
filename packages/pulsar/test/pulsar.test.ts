import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  PulsarConsumer,
  PulsarPublisher,
  pulsarProperties,
  toPulsarMessage,
  type BabelHandlers,
  type PulsarConsumerClient,
  type PulsarProducer,
  type PulsarProducerMessage,
  type PulsarReceivedMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  const env = EnvelopeCodec.make(URN, { order_id: 1042 }, { queue: "orders" });
  env.attempts = attempts;
  return env;
}

function body(attempts = 0): string {
  return EnvelopeCodec.encode(envelope(attempts));
}

class FakeProducer implements PulsarProducer {
  sent: PulsarProducerMessage[] = [];
  constructor(private readonly topic = "persistent://public/default/orders") {}
  getTopic(): string {
    return this.topic;
  }
  async send(message: PulsarProducerMessage): Promise<unknown> {
    this.sent.push(message);
    return { messageId: "m-1" };
  }
}

class FakeMessage implements PulsarReceivedMessage {
  constructor(
    private readonly raw: string,
    private readonly redeliveryCount = 0,
  ) {}
  getData(): Buffer {
    return Buffer.from(this.raw, "utf8");
  }
  getProperties(): { [key: string]: string } {
    return {};
  }
  getRedeliveryCount(): number {
    return this.redeliveryCount;
  }
}

class FakeConsumer implements PulsarConsumerClient {
  acked: PulsarReceivedMessage[] = [];
  nacked: PulsarReceivedMessage[] = [];
  constructor(private readonly messages: PulsarReceivedMessage[] = []) {}
  async receive(): Promise<PulsarReceivedMessage> {
    const message = this.messages.shift();
    if (message === undefined) throw new Error("Failed to receive message: TimeOut");
    return message;
  }
  acknowledge(message: PulsarReceivedMessage): void {
    this.acked.push(message);
  }
  negativeAcknowledge(message: PulsarReceivedMessage): void {
    this.nacked.push(message);
  }
}

// --- projection ---------------------------------------------------------------

test("projection maps contract fields onto string properties", () => {
  const env = envelope();
  const props = pulsarProperties(env);
  assert.equal(props["bq-job"], URN);
  assert.equal(props["bq-trace-id"], env.trace_id);
  assert.equal(props["bq-message-id"], env.meta.id);
  assert.equal(props["bq-schema-version"], String(env.meta.schema_version));
  assert.equal(props["bq-source-lang"], env.meta.lang);
  assert.equal(props["bq-attempts"], "0");
  assert.ok(Object.values(props).every((v) => typeof v === "string"));
});

test("toPulsarMessage carries the canonical envelope as the payload", () => {
  const env = envelope();
  const message = toPulsarMessage(env);
  assert.ok(Buffer.isBuffer(message.data));
  assert.equal(EnvelopeCodec.urn(EnvelopeCodec.decode(message.data.toString("utf8"))), URN);
  assert.equal(message.properties?.["bq-job"], URN);
});

// --- publisher ----------------------------------------------------------------

test("publish projects properties and returns message id", async () => {
  const producer = new FakeProducer();
  const id = await new PulsarPublisher(producer).publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  assert.equal(producer.sent.length, 1);
  const message = producer.sent[0]!;
  assert.equal(message.properties?.["bq-job"], URN);
  assert.equal(message.properties?.["bq-trace-id"], "trace-1");
  assert.equal(message.properties?.["bq-message-id"], id);
  assert.equal(EnvelopeCodec.urn(EnvelopeCodec.decode(message.data.toString("utf8"))), URN);
});

test("publish without a trace id mints a fresh trace", async () => {
  const producer = new FakeProducer();
  await new PulsarPublisher(producer).publish(URN, { order_id: 7 });
  assert.ok(producer.sent[0]!.properties?.["bq-trace-id"]);
});

test("publish with a delay sets deliverAfter and bq-delay", async () => {
  const producer = new FakeProducer();
  await new PulsarPublisher(producer).publish(URN, {}, { delayMs: 30000 });
  const message = producer.sent[0]!;
  assert.equal(message.deliverAfter, 30000);
  assert.equal(message.properties?.["bq-delay"], "30000");
});

// --- consumer -----------------------------------------------------------------

test("attempts is the redelivery count and the message is acknowledged", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 2)]);
  let seen = -1;
  const handlers: BabelHandlers = { [URN]: (env) => void (seen = env.attempts ?? -1) };
  const count = await new PulsarConsumer(consumer, handlers).poll();
  assert.equal(count, 1);
  assert.equal(seen, 2);
  assert.equal(consumer.acked.length, 1);
});

test("first delivery is zero attempts", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let seen = -1;
  await new PulsarConsumer(consumer, { [URN]: (env) => void (seen = env.attempts ?? -1) }).poll();
  assert.equal(seen, 0);
});

test("a higher body attempt count is never lowered by the redelivery count", async () => {
  // Republish-driven retry carried attempts=5 in the body; redelivery count is only 1.
  const consumer = new FakeConsumer([new FakeMessage(body(5), 1)]);
  let seen = -1;
  await new PulsarConsumer(consumer, { [URN]: (env) => void (seen = env.attempts ?? -1) }).poll();
  assert.equal(seen, 5);
});

test("a throwing handler negative-acknowledges and reports onError", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let reported: unknown = null;
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await new PulsarConsumer(consumer, handlers, { onError: (e) => void (reported = e) }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
  assert.equal(consumer.acked.length, 0);
});

test("an unknown URN with a hook acknowledges", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let called = false;
  await new PulsarConsumer(consumer, {}, { onUnknownUrn: () => void (called = true) }).poll();
  assert.equal(called, true);
  assert.equal(consumer.acked.length, 1);
});

test("an unknown URN without a hook negative-acknowledges and reports onError", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let reported: unknown = null;
  await new PulsarConsumer(consumer, {}, { onError: (e) => void (reported = e) }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
});

test("a non-conformant envelope negative-acknowledges and reports onError", async () => {
  const bad = JSON.stringify({
    trace_id: "t",
    data: { x: 1 },
    meta: { id: "m", queue: "q", lang: "node", schema_version: 1, created_at: 1 },
    attempts: 0,
  });
  const consumer = new FakeConsumer([new FakeMessage(bad, 0)]);
  let reported: unknown = null;
  await new PulsarConsumer(consumer, {}, { onError: (e) => void (reported = e) }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
});

test("poll returns 0 on a receive timeout", async () => {
  const consumer = new FakeConsumer([]);
  const count = await new PulsarConsumer(consumer, {}).poll();
  assert.equal(count, 0);
});

test("poll rethrows a non-timeout receive error", async () => {
  const consumer: PulsarConsumerClient = {
    async receive() {
      throw new Error("connection refused");
    },
    acknowledge() {},
    negativeAcknowledge() {},
  };
  await assert.rejects(new PulsarConsumer(consumer, {}).poll(), /connection refused/);
});

test("run stops once the signal is aborted", async () => {
  const consumer = new FakeConsumer([]);
  const controller = new AbortController();
  controller.abort();
  await new PulsarConsumer(consumer, {}).run(controller.signal); // returns immediately
  assert.equal(consumer.acked.length, 0);
});
