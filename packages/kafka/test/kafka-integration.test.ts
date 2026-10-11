/**
 * Live-broker integration tests for the Kafka adapter (§6) — opt-in.
 *
 * Skipped in the normal suite. Enable with `BQ_INTEGRATION=1` and (optionally) `BQ_KAFKA_BROKERS`
 * (comma-separated, default `localhost:9092`). `kafkajs` is an optional peer and is not a workspace
 * dependency, so it is loaded lazily (the CI `integration` job installs it with `npm install --no-save`).
 * Every test uses unique topic names.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import { EnvelopeCodec, redrive } from "@babelqueue/core";

import {
  KafkaConsumer,
  KafkaPublisher,
  KafkaRedriveIO,
  RetryTopics,
  kafkaHeaders,
  type EachMessagePayload,
  type KafkaConsumerClient,
  type KafkaProducerClient,
  type KafkaRedriveMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const BROKERS = (process.env["BQ_KAFKA_BROKERS"] ?? "localhost:9092").split(",");
const SKIP = process.env["BQ_INTEGRATION"] === "1" ? false : "live-broker test: set BQ_INTEGRATION=1 (and BQ_KAFKA_BROKERS) to run";
const DEADLINE_MS = 60_000;

// --- Minimal kafkajs shapes (the package is an optional peer, so no static import) ---

interface KjsAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  createTopics(options: { waitForLeaders?: boolean; topics: { topic: string; numPartitions?: number; replicationFactor?: number }[] }): Promise<boolean>;
}
interface KjsProducer extends KafkaProducerClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
}
interface KjsConsumer extends KafkaConsumerClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  stop(): Promise<void>;
  subscribe(options: { topics: string[]; fromBeginning?: boolean }): Promise<void>;
}
interface KjsClient {
  admin(): KjsAdmin;
  producer(): KjsProducer;
  consumer(options: { groupId: string }): KjsConsumer;
}
type KjsModule = { Kafka?: new (config: { clientId: string; brokers: string[] }) => KjsClient; default?: { Kafka: new (config: { clientId: string; brokers: string[] }) => KjsClient } };

async function newClient(): Promise<KjsClient> {
  const specifier = "kafkajs"; // non-literal: keeps `tsc` from resolving an optional peer that may be absent
  const mod = (await import(specifier)) as KjsModule;
  const Kafka = mod.Kafka ?? mod.default?.Kafka;
  assert.ok(Kafka, "kafkajs must be installed to run the live-broker tests");
  return new Kafka({ clientId: `bq-it-${randomUUID().slice(0, 8)}`, brokers: BROKERS });
}

async function createTopics(client: KjsClient, topics: string[]): Promise<void> {
  const admin = client.admin();
  await admin.connect();
  try {
    await admin.createTopics({ waitForLeaders: true, topics: topics.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })) });
  } finally {
    await admin.disconnect();
  }
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const stop = Date.now() + DEADLINE_MS;
  while (!condition()) {
    if (Date.now() > stop) assert.fail(`timed out after ${DEADLINE_MS}ms waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/** Read every record that lands on `topic` (from the beginning) until `stop()` is called. */
async function collect(client: KjsClient, topic: string): Promise<{ records: EachMessagePayload[]; stop: () => Promise<void> }> {
  const consumer = client.consumer({ groupId: `bq-it-dlq-${randomUUID()}` });
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  const records: EachMessagePayload[] = [];
  await consumer.run({
    eachMessage: async (payload) => {
      records.push(payload);
    },
  });
  return {
    records,
    stop: async () => {
      await consumer.stop();
      await consumer.disconnect();
    },
  };
}

function headerText(payload: EachMessagePayload, name: string): string | undefined {
  const value = payload.message.headers?.[name];
  if (value === undefined || value === null) return undefined;
  const first = Array.isArray(value) ? value[0] : value;
  return first === undefined ? undefined : first.toString();
}

test("handler error retries, then dead-letters after maxTries with a dead_letter block", { skip: SKIP }, async () => {
  const suffix = randomUUID().replace(/-/g, "").slice(0, 8);
  const work = `bq-it-orders-${suffix}`;
  const topology = new RetryTopics(work, [1000]);
  const retry = topology.tiers[0]!.topic;
  const dlq = topology.dlqTopic!;
  const client = await newClient();
  await createTopics(client, [work, retry, dlq]);

  const producer = client.producer();
  const raw = client.consumer({ groupId: `bq-it-group-${suffix}` });
  await producer.connect();
  await raw.connect();
  await raw.subscribe({ topics: [work, retry], fromBeginning: true });
  const dead = await collect(client, dlq);

  let calls = 0;
  const consumer = new KafkaConsumer(
    raw,
    {
      [URN]: () => {
        calls += 1;
        throw new Error("boom");
      },
    },
    { producer, retryTopics: topology, maxTries: 3 },
  );

  try {
    await consumer.run();
    await KafkaPublisher.withRetryTopics(producer, topology).publish(URN, { order_id: 7 }, { traceId: "trace-it-1" });

    await waitFor(() => dead.records.length >= 1, `a record on ${dlq}`);
    assert.equal(calls, 3);

    const record = dead.records[0]!;
    const envelope = JSON.parse(record.message.value!.toString("utf8")) as {
      trace_id: string;
      dead_letter?: { reason: string; original_queue: string };
    };
    assert.ok(envelope.dead_letter, "the DLQ record must carry a dead_letter block");
    assert.equal(envelope.dead_letter.reason, "failed");
    assert.equal(envelope.dead_letter.original_queue, work);
    assert.equal(envelope.trace_id, "trace-it-1");
  } finally {
    await dead.stop();
    await raw.stop();
    await raw.disconnect();
    await producer.disconnect();
  }
});

test("schema-version gate rejection forwards the raw record to the DLQ", { skip: SKIP }, async () => {
  const suffix = randomUUID().replace(/-/g, "").slice(0, 8);
  const work = `bq-it-gate-${suffix}`;
  const topology = new RetryTopics(work, [1000]);
  const dlq = topology.dlqTopic!;
  const client = await newClient();
  await createTopics(client, [work, topology.tiers[0]!.topic, dlq]);

  const producer = client.producer();
  const raw = client.consumer({ groupId: `bq-it-group-${suffix}` });
  await producer.connect();
  await raw.connect();
  await raw.subscribe({ topics: [work], fromBeginning: true });
  const dead = await collect(client, dlq);

  // Deliberately not a valid v1 envelope: the gate must reject on the header alone (§6.7).
  const body = '{"job":"urn:babel:orders:created","data":{},"future":true}';
  let handled = 0;
  let rejected = 0;
  const consumer = new KafkaConsumer(
    raw,
    {
      [URN]: () => {
        handled += 1;
      },
    },
    {
      producer,
      retryTopics: topology,
      onError: () => {
        rejected += 1;
      },
    },
  );

  try {
    await consumer.run();
    await producer.send({
      topic: work,
      messages: [{ value: body, headers: { "bq-job": URN, "bq-schema-version": "2" } }],
    });

    await waitFor(() => dead.records.length >= 1, `a record on ${dlq}`);
    const record = dead.records[0]!;
    assert.equal(record.message.value!.toString("utf8"), body);
    assert.equal(headerText(record, "bq-schema-version"), "2");
    assert.equal(handled, 0);
    assert.ok(rejected >= 1, "the gate must report the rejection");
  } finally {
    await dead.stop();
    await raw.stop();
    await raw.disconnect();
    await producer.disconnect();
  }
});

// --- Record key carry (contract §6.5): null stays null, empty stays empty, non-empty stays distinct ---

/** The three key shapes of §6.5. `null` = no key on the record (length -1), `empty` = zero-length key. */
const KEY_CASES: { name: string; key: Buffer | null }[] = [
  { name: "null", key: null },
  { name: "empty", key: Buffer.alloc(0) },
  { name: "non-empty", key: Buffer.from("order-7", "utf8") },
];

/** Assert a record's key equals the expected one, comparing null-ness first (KafkaJS: -1 → null, 0 → empty Buffer). */
function assertSameKey(actual: Buffer | null | undefined, expected: Buffer | null, where: string): void {
  const got = actual ?? null;
  if (expected === null) {
    assert.equal(got, null, `${where}: a null key must stay null, got ${got === null ? "null" : `${got.length} bytes`}`);
    return;
  }
  assert.notEqual(got, null, `${where}: a ${expected.length === 0 ? "empty" : "non-null"} key must not become null`);
  assert.equal(got!.length, expected.length, `${where}: key length`);
  assert.ok(got!.equals(expected), `${where}: key bytes`);
}

/** Produce one keyed (or key-less) envelope record to `topic`, as an external producer would. */
async function seedRecord(producer: KafkaProducerClient, topic: string, key: Buffer | null, envelope = EnvelopeCodec.make(URN, { order_id: 7 }, { queue: topic, traceId: "trace-key" })): Promise<void> {
  const message: { key?: Buffer; value: string; headers: { [name: string]: string } } = {
    value: EnvelopeCodec.encode(envelope),
    headers: kafkaHeaders(envelope),
  };
  if (key !== null) message.key = Buffer.from(key);
  await producer.send({ topic, messages: [message] });
}

for (const { name, key } of KEY_CASES) {
  test(`${name} record key survives retry and DLQ`, { skip: SKIP }, async () => {
    const suffix = randomUUID().replace(/-/g, "").slice(0, 8);
    const work = `bq-it-key-${suffix}`;
    const topology = new RetryTopics(work, [1000]);
    const retry = topology.tiers[0]!.topic;
    const dlq = topology.dlqTopic!;
    const client = await newClient();
    await createTopics(client, [work, retry, dlq]);

    const producer = client.producer();
    const raw = client.consumer({ groupId: `bq-it-group-${suffix}` });
    await producer.connect();
    await raw.connect();
    await raw.subscribe({ topics: [work, retry], fromBeginning: true });
    const onWork = await collect(client, work);
    const onRetry = await collect(client, retry);
    const onDlq = await collect(client, dlq);

    const consumer = new KafkaConsumer(
      raw,
      {
        [URN]: () => {
          throw new Error("boom");
        },
      },
      { producer, retryTopics: topology, maxTries: 2 },
    );

    try {
      await consumer.run();
      await seedRecord(producer, work, key);

      await waitFor(() => onDlq.records.length >= 1, `a record on ${dlq}`);
      assert.equal(onWork.records.length, 1, "exactly one record on the work topic");
      assert.equal(onRetry.records.length, 1, "exactly one record on the retry tier");
      assert.equal(onDlq.records.length, 1, "exactly one record on the DLQ");
      assertSameKey(onWork.records[0]!.message.key, key, "work record (as produced)");
      assertSameKey(onRetry.records[0]!.message.key, key, "retry republish");
      assertSameKey(onDlq.records[0]!.message.key, key, "DLQ write");
    } finally {
      await onWork.stop();
      await onRetry.stop();
      await onDlq.stop();
      await raw.stop();
      await raw.disconnect();
      await producer.disconnect();
    }
  });
}

/**
 * A bounded pull over a push-based KafkaJS consumer (manual commit): buffers delivered records and hands
 * out one per `pop`, returning `null` when nothing arrives within `timeoutMs`.
 */
async function pullFrom(client: KjsClient, topic: string, timeoutMs: number): Promise<{ pop: (queue: string) => Promise<KafkaRedriveMessage | null>; stop: () => Promise<void> }> {
  const consumer = client.consumer({ groupId: `bq-it-redrive-${randomUUID()}` }) as KjsConsumer & KafkaConsumerClient;
  await consumer.connect();
  await consumer.subscribe({ topics: [topic], fromBeginning: true });
  const pending: EachMessagePayload[] = [];
  await consumer.run({
    autoCommit: false,
    eachMessage: async (payload) => {
      pending.push(payload);
    },
  });
  return {
    pop: async () => {
      const stop = Date.now() + timeoutMs;
      while (pending.length === 0) {
        if (Date.now() > stop) return null;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      const payload = pending.shift()!;
      return {
        body: payload.message.value!.toString("utf8"),
        key: payload.message.key ?? null,
        // The consumed record as received, so a restore write can re-write it verbatim.
        value: payload.message.value,
        headers: payload.message.headers,
        topic: payload.topic,
        partition: payload.partition,
        offset: payload.message.offset,
        ack: () => consumer.commitOffsets([{ topic: payload.topic, partition: payload.partition, offset: String(Number(payload.message.offset) + 1) }]),
      };
    },
    stop: async () => {
      await consumer.stop();
      await consumer.disconnect();
    },
  };
}

for (const { name, key } of KEY_CASES) {
  test(`${name} record key survives DLQ redrive`, { skip: SKIP }, async () => {
    const suffix = randomUUID().replace(/-/g, "").slice(0, 8);
    const work = `bq-it-redrive-${suffix}`;
    const topology = new RetryTopics(work, [1000]);
    const dlq = topology.dlqTopic!;
    const sandbox = `bq-it-sandbox-${suffix}`;
    const client = await newClient();
    await createTopics(client, [work, topology.tiers[0]!.topic, dlq, sandbox]);

    const producer = client.producer();
    await producer.connect();
    const onDlq = await collect(client, dlq);
    const onSandbox = await collect(client, sandbox);
    const pull = await pullFrom(client, dlq, 30_000);
    const io = new KafkaRedriveIO({ producer, pop: pull.pop });

    try {
      // A dead-lettered record, as the consumer would have written it (carrying the key under test).
      const dead = EnvelopeCodec.make(URN, { order_id: 7 }, { queue: work, traceId: "trace-key" });
      dead.dead_letter = { reason: "failed", error: "boom", exception: "Error", failed_at: Date.now(), original_queue: work, attempts: 2, lang: "node" };
      await seedRecord(producer, dlq, key, dead);
      await waitFor(() => onDlq.records.length >= 1, `the seeded record on ${dlq}`);
      assertSameKey(onDlq.records[0]!.message.key, key, "DLQ record (as seeded)");

      // Dry run: the record is popped and restored to the DLQ unchanged — the restore must carry the key.
      const dry = await redrive(io, dlq, { dryRun: true, max: 1 });
      assert.equal(dry.skipped, 1);
      await waitFor(() => onDlq.records.length >= 2, `the dry-run restore on ${dlq}`);
      assertSameKey(onDlq.records[1]!.message.key, key, "dry-run restore to the DLQ");
      // ... and the rest of the record verbatim: value bytes and every header as consumed.
      assert.deepEqual(onDlq.records[1]!.message.value, onDlq.records[0]!.message.value, "dry-run restore: value bytes");
      assert.deepEqual(onDlq.records[1]!.message.headers, onDlq.records[0]!.message.headers, "dry-run restore: headers");

      // Real redrive of the restored record into a sandbox topic — the redrive write must carry the key.
      const real = await redrive(io, dlq, { toQueue: sandbox, max: 1 });
      assert.equal(real.redriven, 1);
      await waitFor(() => onSandbox.records.length >= 1, `a record on ${sandbox}`);
      assert.equal(onSandbox.records.length, 1);
      assertSameKey(onSandbox.records[0]!.message.key, key, "redrive write");
    } finally {
      await pull.stop();
      await onDlq.stop();
      await onSandbox.stop();
      await producer.disconnect();
    }
  });
}
