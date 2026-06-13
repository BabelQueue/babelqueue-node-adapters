import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  KafkaConsumer,
  KafkaPublisher,
  RetryTopics,
  kafkaHeaders,
  type BabelHandlers,
  type EachMessagePayload,
  type IncomingHeaders,
  type KafkaConsumerClient,
  type KafkaProducerClient,
  type KafkaProducerMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

function payloadFor(env: Envelope, offset = "10", topic = "orders"): EachMessagePayload {
  const headers: IncomingHeaders = {};
  for (const [k, v] of Object.entries(kafkaHeaders(env))) headers[k] = Buffer.from(v, "utf8");
  return {
    topic,
    partition: 0,
    message: {
      value: Buffer.from(EnvelopeCodec.encode(env), "utf8"),
      headers,
      offset,
      timestamp: String(env.meta.created_at),
    },
  };
}

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

function topology(): RetryTopics {
  return new RetryTopics("orders", [5000, 60000]);
}

// --- projection ---------------------------------------------------------------

test("kafkaHeaders projects the contract fields as strings", () => {
  const env = envelope();
  const h = kafkaHeaders(env);
  assert.equal(h["bq-job"], URN);
  assert.equal(h["bq-trace-id"], env.trace_id);
  assert.equal(h["bq-message-id"], env.meta.id);
  assert.equal(h["bq-schema-version"], "1");
  assert.equal(h["bq-source-lang"], env.meta.lang);
  assert.equal(h["bq-attempts"], "0");
  assert.ok(Object.values(h).every((v) => typeof v === "string"));
});

// --- RetryTopics --------------------------------------------------------------

test("RetryTopics names tiers ascending and maps delay/attempt", () => {
  const rt = new RetryTopics("orders", [60000, 5000]);
  assert.equal(rt.tiers[0]!.topic, "orders.retry.1");
  assert.equal(rt.tiers[0]!.delayMs, 5000);
  assert.equal(rt.tiers[1]!.topic, "orders.retry.2");
  assert.equal(rt.dlqTopic, "orders.dlq");
  assert.equal(rt.tierForDelay(3000).topic, "orders.retry.1");
  assert.equal(rt.tierForDelay(10000).topic, "orders.retry.2");
  assert.equal(rt.tierForAttempt(0).topic, "orders.retry.1");
  assert.equal(rt.tierForAttempt(9).topic, "orders.retry.2");
});

test("RetryTopics raises on too-large delay or no tiers", () => {
  assert.throws(() => new RetryTopics("orders", [5000]).tierForDelay(60000));
  assert.throws(() => new RetryTopics("orders", []).tierForAttempt(0));
  assert.equal(new RetryTopics("orders", [], null).dlqTopic, null);
});

// --- publisher ----------------------------------------------------------------

test("publish projects value, headers and timestamp", async () => {
  const producer = new FakeProducer();
  const id = await KafkaPublisher.create(producer, "orders").publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  assert.equal(producer.sent.length, 1);
  const msg = producer.sent[0]!.messages[0]!;
  assert.equal(producer.sent[0]!.topic, "orders");
  assert.equal(msg.headers!["bq-job"], URN);
  assert.equal(msg.headers!["bq-message-id"], id);
  assert.equal(msg.headers!["bq-attempts"], "0");
  const decoded = EnvelopeCodec.decode(String(msg.value));
  assert.equal(EnvelopeCodec.urn(decoded), URN);
  assert.equal(msg.timestamp, String((decoded as Envelope).meta.created_at));
});

test("publish delay without retry topics raises", async () => {
  const producer = new FakeProducer();
  await assert.rejects(
    KafkaPublisher.create(producer, "orders").publish(URN, {}, { delayMs: 30000 }),
    /retry topics/,
  );
});

test("publish delay routes to the smallest sufficient retry tier", async () => {
  const producer = new FakeProducer();
  await KafkaPublisher.withRetryTopics(producer, topology()).publish(URN, {}, { delayMs: 30000 });
  const msg = producer.sent[0]!;
  assert.equal(msg.topic, "orders.retry.2");
  assert.equal(msg.messages[0]!.headers!["bq-delay"], "30000");
  assert.equal(msg.messages[0]!.headers!["bq-original-topic"], "orders");
});

// --- consumer -----------------------------------------------------------------

test("success processes then commits", async () => {
  const consumer = new FakeConsumer();
  let seen = -1;
  const handlers: BabelHandlers = { [URN]: (env) => void (seen = env.attempts) };
  await new KafkaConsumer(consumer, handlers).handle(payloadFor(envelope(0), "41"));
  assert.equal(seen, 0);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "42" }]);
});

test("attempts header is authoritative", async () => {
  const consumer = new FakeConsumer();
  let seen = -1;
  await new KafkaConsumer(consumer, { [URN]: (env) => void (seen = env.attempts) }).handle(payloadFor(envelope(2)));
  assert.equal(seen, 2);
});

test("throwing handler republishes to retry with attempts+1", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  let reported: unknown = null;
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await new KafkaConsumer(consumer, handlers, {
    producer,
    retryTopics: topology(),
    maxTries: 3,
    onError: (e) => void (reported = e),
  }).handle(payloadFor(envelope(0), "10"));

  assert.ok(reported instanceof Error);
  const retry = producer.sent[0]!;
  assert.equal(retry.topic, "orders.retry.1");
  assert.equal(retry.messages[0]!.headers!["bq-attempts"], "1");
  assert.equal(retry.messages[0]!.headers!["bq-delay"], "5000");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "11" }]);
});

test("terminal failure goes to the DLQ with a dead_letter block", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await new KafkaConsumer(consumer, handlers, { producer, retryTopics: topology(), maxTries: 3 }).handle(
    payloadFor(envelope(2), "7"),
  );
  const dlq = producer.sent[0]!;
  assert.equal(dlq.topic, "orders.dlq");
  const dead = EnvelopeCodec.decode(String(dlq.messages[0]!.value)) as Envelope;
  assert.equal(dead.dead_letter!.reason, "failed");
});

test("retry without topics or DLQ raises", async () => {
  const consumer = new FakeConsumer();
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await assert.rejects(new KafkaConsumer(consumer, handlers).handle(payloadFor(envelope(0))));
});

test("unknown URN fail throws and does not commit", async () => {
  const consumer = new FakeConsumer();
  await assert.rejects(new KafkaConsumer(consumer, {}).handle(payloadFor(envelope(0))));
  assert.equal(consumer.committed.length, 0);
});

test("unknown URN delete commits", async () => {
  const consumer = new FakeConsumer();
  await new KafkaConsumer(consumer, {}, { unknownUrn: UnknownUrnStrategy.DELETE }).handle(payloadFor(envelope(0), "8"));
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "9" }]);
});

test("unknown URN dead_letter goes to the DLQ", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  await new KafkaConsumer(consumer, {}, {
    producer,
    retryTopics: topology(),
    unknownUrn: UnknownUrnStrategy.DEAD_LETTER,
  }).handle(payloadFor(envelope(0)));
  const dlq = producer.sent[0]!;
  assert.equal(dlq.topic, "orders.dlq");
  const dead = EnvelopeCodec.decode(String(dlq.messages[0]!.value)) as Envelope;
  assert.equal(dead.dead_letter!.reason, "unknown_urn");
});

test("unknown URN release republishes to retry", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  await new KafkaConsumer(consumer, {}, {
    producer,
    retryTopics: topology(),
    unknownUrn: UnknownUrnStrategy.RELEASE,
  }).handle(payloadFor(envelope(0)));
  assert.equal(producer.sent[0]!.topic, "orders.retry.1");
});

test("poison body is forwarded raw to the DLQ", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  let reported: unknown = null;
  const payload: EachMessagePayload = {
    topic: "orders",
    partition: 0,
    message: { value: Buffer.from("not-json", "utf8"), headers: {}, offset: "4" },
  };
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology(), onError: (e) => void (reported = e) }).handle(
    payload,
  );
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "5" }]);
  assert.ok(reported);
});

test("run wires a manual-commit eachMessage loop", async () => {
  const consumer = new FakeConsumer();
  let seen = -1;
  await new KafkaConsumer(consumer, { [URN]: (env) => void (seen = env.attempts) }).run();
  assert.ok(consumer.eachMessage, "run should register eachMessage");
  await consumer.eachMessage!(payloadFor(envelope(0)));
  assert.equal(seen, 0);
});
