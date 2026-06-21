import assert from "node:assert/strict";
import { test } from "node:test";

import type { HeaderCarrier } from "@babelqueue/core";

import {
  KafkaConsumer,
  KafkaPublisher,
  headersOf,
  type BabelHandlers,
  type EachMessagePayload,
  type IncomingHeaders,
  type KafkaConsumerClient,
  type KafkaIncomingMessage,
  type KafkaProducerClient,
  type KafkaProducerMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

class FakeProducer implements KafkaProducerClient {
  sent: { topic: string; messages: KafkaProducerMessage[] }[] = [];
  async send(record: { topic: string; messages: KafkaProducerMessage[] }): Promise<unknown> {
    this.sent.push(record);
    return {};
  }
}

class FakeConsumer implements KafkaConsumerClient {
  committed: { topic: string; partition: number; offset: string }[] = [];
  eachMessage?: (payload: EachMessagePayload) => Promise<void>;
  async run(config: { autoCommit?: boolean; eachMessage: (p: EachMessagePayload) => Promise<void> }): Promise<void> {
    this.eachMessage = config.eachMessage;
  }
  async commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void> {
    this.committed.push(...offsets);
  }
}

/** Turn a produced record's string headers into KafkaJS-style Buffer headers for consume. */
function incomingFrom(sent: KafkaProducerMessage, offset = "10", topic = "orders"): EachMessagePayload {
  const headers: IncomingHeaders = {};
  for (const [k, v] of Object.entries(sent.headers ?? {})) headers[k] = Buffer.from(v, "utf8");
  return {
    topic,
    partition: 0,
    message: { value: Buffer.from(String(sent.value), "utf8"), headers, offset } as KafkaIncomingMessage,
  };
}

// --- inject: merge beside the contract bq-* (contract wins) --------------------

test("publish without headers carries only the contract bq-* record headers", async () => {
  const producer = new FakeProducer();
  await KafkaPublisher.create(producer, "orders").publish(URN, { order_id: 7 });
  const headers = producer.sent[0]!.messages[0]!.headers!;
  assert.ok(!("traceparent" in headers));
  assert.equal(headers["bq-job"], URN);
});

test("publish with a traceparent merges it beside the contract bq-* headers", async () => {
  const producer = new FakeProducer();
  await KafkaPublisher.create(producer, "orders").publish(URN, { order_id: 7 }, {
    headers: { traceparent: TRACEPARENT, tracestate: "v=1" },
  });
  const headers = producer.sent[0]!.messages[0]!.headers!;
  assert.equal(headers["traceparent"], TRACEPARENT);
  assert.equal(headers["tracestate"], "v=1");
  assert.equal(headers["bq-job"], URN);
});

test("a contract bq-* header wins a key collision (merge-not-clobber)", async () => {
  const producer = new FakeProducer();
  await KafkaPublisher.create(producer, "orders").publish(URN, { order_id: 7 }, {
    headers: { "bq-job": "HIJACK", traceparent: TRACEPARENT } as HeaderCarrier,
  });
  const headers = producer.sent[0]!.messages[0]!.headers!;
  assert.equal(headers["bq-job"], URN); // not "HIJACK"
  assert.equal(headers["traceparent"], TRACEPARENT);
});

// --- extract: headersOf round-trip --------------------------------------------

test("headersOf reads inbound Buffer record headers into a Record<string,string>", () => {
  const message: KafkaIncomingMessage = {
    value: Buffer.from("{}"),
    headers: { traceparent: Buffer.from(TRACEPARENT), "bq-job": Buffer.from(URN), empty: Buffer.from("") },
    offset: "1",
  };
  const headers = headersOf(message);
  assert.equal(headers["traceparent"], TRACEPARENT);
  assert.equal(headers["bq-job"], URN);
  assert.ok(!("empty" in headers));
  assert.deepEqual(headersOf({ value: Buffer.from("{}"), offset: "1" }), {});
});

// --- end-to-end through the fakes (produce → consume) -------------------------

test("the consumer surfaces the carried traceparent to the handler", async () => {
  const producer = new FakeProducer();
  await KafkaPublisher.create(producer, "orders").publish(URN, { order_id: 7 }, {
    headers: { traceparent: TRACEPARENT },
  });
  const payload = incomingFrom(producer.sent[0]!.messages[0]!);

  const consumer = new FakeConsumer();
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _m, headers) => { seen = headers; } };
  await new KafkaConsumer(consumer, handlers).run();
  await consumer.eachMessage!(payload);

  assert.equal(seen!["traceparent"], TRACEPARENT);
});

// --- broker-gated integration (skips cleanly without Kafka) -------------------

const KAFKA_BROKER = process.env.BABELQUEUE_KAFKA_BROKER;

test("integration: a published traceparent arrives on consume", { skip: !KAFKA_BROKER }, async () => {
  // Wired only against a real Kafka. kafkajs is imported lazily via a non-literal specifier so the
  // optional peer is needed neither to typecheck nor to run when it is absent.
  const specifier = "kafkajs";
  const mod = (await import(specifier)) as unknown as {
    Kafka: new (cfg: unknown) => {
      producer(): KafkaProducerClient & { connect(): Promise<void>; disconnect(): Promise<void> };
      consumer(cfg: unknown): KafkaConsumerClient & { connect(): Promise<void>; subscribe(cfg: unknown): Promise<void>; disconnect(): Promise<void> };
    };
  };
  const kafka = new mod.Kafka({ brokers: [KAFKA_BROKER!] });
  const topic = `bq-otel-it-${Date.now()}`;
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `g-${Date.now()}` });
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  try {
    await KafkaPublisher.create(producer, topic).publish(URN, { order_id: 7 }, { headers: { traceparent: TRACEPARENT } });
    const received = await new Promise<HeaderCarrier>((resolve) => {
      void new KafkaConsumer(consumer, { [URN]: (_e, _m, headers) => resolve(headers) }).run();
    });
    assert.equal(received["traceparent"], TRACEPARENT);
  } finally {
    await producer.disconnect();
    await consumer.disconnect();
  }
});
