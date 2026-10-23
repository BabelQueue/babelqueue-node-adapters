import assert from "node:assert/strict";
import { test } from "node:test";

import { BabelQueueError } from "@babelqueue/core";

import {
  type EachMessagePayload,
  type IncomingHeaders,
  type KafkaProducerClient,
  type KafkaProducerMessage,
  type KafkaReinjectClient,
  RetryReinjector,
  RetryTopics,
} from "../src/index.js";

const NOW = 1_700_000_000_000;

class FakeReinjectConsumer implements KafkaReinjectClient {
  committed: { topic: string; partition: number; offset: string }[] = [];
  seeks: { topic: string; partition: number; offset: string }[] = [];
  paused: { topic: string; partitions?: number[] }[][] = [];
  resumed: { topic: string; partitions?: number[] }[][] = [];
  runConfig: { autoCommit?: boolean } | undefined;
  async run(config: { autoCommit?: boolean }): Promise<void> {
    this.runConfig = config;
  }
  async commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void> {
    this.committed.push(...offsets);
  }
  seek(position: { topic: string; partition: number; offset: string }): void {
    this.seeks.push(position);
  }
  pause(topics: { topic: string; partitions?: number[] }[]): void {
    this.paused.push(topics);
  }
  resume(topics: { topic: string; partitions?: number[] }[]): void {
    this.resumed.push(topics);
  }
}

class FakeProducer implements KafkaProducerClient {
  sent: { topic: string; messages: KafkaProducerMessage[] }[] = [];
  failWith: Error | null = null;
  async send(record: { topic: string; messages: KafkaProducerMessage[] }): Promise<unknown> {
    if (this.failWith) throw this.failWith;
    this.sent.push(record);
    return {};
  }
}

interface Scheduled {
  fn: () => void;
  ms: number;
  cancelled: boolean;
}

function fixture() {
  const consumer = new FakeReinjectConsumer();
  const producer = new FakeProducer();
  const scheduled: Scheduled[] = [];
  const errors: unknown[] = [];
  const reinjector = new RetryReinjector(consumer, {
    producer,
    retryTopics: new RetryTopics("orders", [5000, 60000]),
    clock: () => NOW,
    schedule: (fn, ms) => {
      const entry: Scheduled = { fn, ms, cancelled: false };
      scheduled.push(entry);
      return () => {
        entry.cancelled = true;
      };
    },
    onError: (error) => void errors.push(error),
  });
  return { consumer, producer, scheduled, errors, reinjector };
}

function record(topic: string, timestamp: number | undefined, headers: IncomingHeaders = {}, key?: Buffer): EachMessagePayload {
  return {
    topic,
    partition: 2,
    message: {
      value: Buffer.from('{"job":"urn:x","data":{"a":1},"keep":"unknown"}', "utf8"),
      headers,
      offset: "41",
      ...(timestamp === undefined ? {} : { timestamp: String(timestamp) }),
      ...(key === undefined ? {} : { key }),
    },
  };
}

const text = (value: unknown): string => (Buffer.isBuffer(value) ? value.toString("utf8") : String(value));

test("RetryReinjector subscribes to every retry tier topic", () => {
  assert.deepEqual(fixture().reinjector.topics, ["orders.retry.1", "orders.retry.2"]);
});

test("a record that is not yet due is held: seek, pause, scheduled resume, no write, no commit", async () => {
  const { consumer, producer, scheduled, reinjector } = fixture();
  await reinjector.handle(record("orders.retry.1", NOW - 1000));

  assert.deepEqual(consumer.seeks, [{ topic: "orders.retry.1", partition: 2, offset: "41" }]);
  assert.deepEqual(consumer.paused, [[{ topic: "orders.retry.1", partitions: [2] }]]);
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0]!.ms, 4000);
  assert.equal(producer.sent.length, 0);
  assert.equal(consumer.committed.length, 0);

  scheduled[0]!.fn();
  assert.deepEqual(consumer.resumed, [[{ topic: "orders.retry.1", partitions: [2] }]]);
});

test("holding the same partition again cancels the earlier resume timer", async () => {
  const { scheduled, reinjector } = fixture();
  await reinjector.handle(record("orders.retry.1", NOW));
  await reinjector.handle(record("orders.retry.1", NOW));
  assert.equal(scheduled.length, 2);
  assert.equal(scheduled[0]!.cancelled, true);
  assert.equal(scheduled[1]!.cancelled, false);
  reinjector.stop();
  assert.equal(scheduled[1]!.cancelled, true);
});

test("a due record is re-injected byte for byte, internal headers dropped, offset+1 committed after the write", async () => {
  const { consumer, producer, scheduled, reinjector } = fixture();
  const key = Buffer.from([0x00, 0xff, 0x10]);
  const payload = record(
    "orders.retry.1",
    NOW - 5000,
    {
      "bq-delay": Buffer.from("5000"),
      "bq-original-topic": Buffer.from("orders"),
      "bq-attempts": Buffer.from("1"),
      traceparent: Buffer.from("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
    },
    key,
  );
  await reinjector.handle(payload);

  assert.equal(scheduled.length, 0);
  assert.equal(producer.sent.length, 1);
  const sent = producer.sent[0]!;
  assert.equal(sent.topic, "orders");
  const message = sent.messages[0]!;
  assert.deepEqual(Buffer.from(message.value as Buffer), payload.message.value);
  assert.deepEqual(Buffer.from(message.key as Buffer), key);
  const headers = message.headers as unknown as Record<string, unknown>;
  assert.equal("bq-delay" in headers, false);
  assert.equal("bq-original-topic" in headers, false);
  assert.equal(text(headers["bq-attempts"]), "1");
  assert.equal(text(headers["traceparent"]), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
  assert.deepEqual(consumer.committed, [{ topic: "orders.retry.1", partition: 2, offset: "42" }]);
});

test("without bq-original-topic the record returns to the topology's work topic", async () => {
  const { producer, reinjector } = fixture();
  await reinjector.handle(record("orders.retry.2", NOW - 60000));
  assert.equal(producer.sent[0]!.topic, "orders");
  assert.equal("key" in producer.sent[0]!.messages[0]!, false, "a keyless record stays keyless");
});

test("the due time is clamped: a future timestamp never waits longer than the tier, a stale one is due now", async () => {
  const future = fixture();
  await future.reinjector.handle(record("orders.retry.1", NOW + 3_600_000));
  assert.equal(future.scheduled[0]!.ms, 5000, "clamped to now + tier");
  assert.equal(future.producer.sent.length, 0);

  const stale = fixture();
  await stale.reinjector.handle(record("orders.retry.2", NOW - 86_400_000));
  assert.equal(stale.producer.sent.length, 1, "long past due");
  assert.equal(stale.scheduled.length, 0);
});

test("a record without a usable timestamp waits one full tier", async () => {
  for (const timestamp of [undefined, "not-a-number"]) {
    const { scheduled, producer, reinjector } = fixture();
    const payload = record("orders.retry.1", undefined);
    if (timestamp !== undefined) payload.message.timestamp = timestamp;
    await reinjector.handle(payload);
    assert.equal(scheduled.length, 1);
    assert.equal(scheduled[0]!.ms, 5000);
    assert.equal(producer.sent.length, 0);
  }
});

test("a failed write is reported, rethrown and never committed", async () => {
  const { consumer, producer, errors, reinjector } = fixture();
  producer.failWith = new Error("broker down");
  await assert.rejects(reinjector.handle(record("orders.retry.1", NOW - 5000)), /broker down/);
  assert.equal(errors.length, 1);
  assert.equal(consumer.committed.length, 0);
});

test("a throwing onError does not replace the write failure", async () => {
  const consumer = new FakeReinjectConsumer();
  const producer = new FakeProducer();
  producer.failWith = new Error("broker down");
  const reinjector = new RetryReinjector(consumer, {
    producer,
    retryTopics: new RetryTopics("orders", [5000]),
    clock: () => NOW,
    onError: () => {
      throw new Error("callback failed");
    },
  });
  await assert.rejects(reinjector.handle(record("orders.retry.1", NOW - 5000)), /broker down/);
  assert.equal(consumer.committed.length, 0);
});

test("a record from a topic that is not a retry tier is refused", async () => {
  const { producer, reinjector } = fixture();
  await assert.rejects(reinjector.handle(record("orders", NOW)), BabelQueueError);
  assert.equal(producer.sent.length, 0);
});

test("run starts the consumer with manual commit", async () => {
  const consumer = new FakeReinjectConsumer();
  await new RetryReinjector(consumer, { producer: new FakeProducer(), retryTopics: new RetryTopics("orders", [5000]) }).run();
  assert.equal(consumer.runConfig?.autoCommit, false);
});

test("a future-dated record is held for one tier in total: the redelivery after the resume is re-injected, not re-held", async () => {
  const consumer = new FakeReinjectConsumer();
  const producer = new FakeProducer();
  const scheduled: Scheduled[] = [];
  let now = NOW;
  const reinjector = new RetryReinjector(consumer, {
    producer,
    retryTopics: new RetryTopics("orders", [5000, 60000]),
    clock: () => now,
    schedule: (fn, ms) => {
      scheduled.push({ fn, ms, cancelled: false });
      return () => undefined;
    },
  });
  const payload = record("orders.retry.1", NOW + 3_600_000);

  await reinjector.handle(payload);
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0]!.ms, 5000, "first delivery: clamped to now + tier");
  assert.equal(producer.sent.length, 0);

  // the tier elapses, the partition resumes, the very same record (same offset) is delivered again
  now = NOW + 5000;
  scheduled[0]!.fn();
  await reinjector.handle(payload);

  assert.equal(scheduled.length, 1, "no second hold");
  assert.equal(producer.sent.length, 1, "re-injected on the redelivery");
  assert.deepEqual(consumer.committed, [{ topic: "orders.retry.1", partition: 2, offset: "42" }]);
});

test("the pinned due time is per offset and is dropped once the record is committed", async () => {
  const consumer = new FakeReinjectConsumer();
  const producer = new FakeProducer();
  const scheduled: Scheduled[] = [];
  let now = NOW;
  const reinjector = new RetryReinjector(consumer, {
    producer,
    retryTopics: new RetryTopics("orders", [5000]),
    clock: () => now,
    schedule: (fn, ms) => {
      scheduled.push({ fn, ms, cancelled: false });
      return () => undefined;
    },
  });
  const first = record("orders.retry.1", NOW + 1_000_000);
  await reinjector.handle(first);
  now = NOW + 5000;
  await reinjector.handle(first);
  assert.equal(producer.sent.length, 1);

  // the SAME offset delivered again after the commit (a replay): the pin is gone, so it is clamped
  // afresh against the current clock and held, instead of being re-injected on the stale due time
  await reinjector.handle(first);
  assert.equal(scheduled.length, 2, "a fresh hold, not a re-injection on the dropped pin");
  assert.equal(scheduled[1]!.ms, 5000);
  assert.equal(producer.sent.length, 1);

  // a different offset on the same partition is clamped afresh against the current clock
  const second = record("orders.retry.1", now + 1_000_000);
  second.message.offset = "42";
  await reinjector.handle(second);
  assert.equal(scheduled.length, 3);
  assert.equal(scheduled[2]!.ms, 5000);
  assert.equal(producer.sent.length, 1);
});

test("a pin stranded by a rebalance is replaced by the next record on that partition, never accumulated", async () => {
  const consumer = new FakeReinjectConsumer();
  const producer = new FakeProducer();
  let now = NOW;
  const reinjector = new RetryReinjector(consumer, {
    producer,
    retryTopics: new RetryTopics("orders", [5000]),
    clock: () => now,
    schedule: () => () => undefined,
  });
  const pins = (): number => (reinjector as unknown as { dueAt: Map<string, unknown> }).dueAt.size;

  // offset 41 is held, then the partition moves to another instance: it never commits here
  await reinjector.handle(record("orders.retry.1", NOW + 1_000_000));
  assert.equal(pins(), 1);

  // the partition comes back later and delivers further offsets; the stale pin does not pile up
  now = NOW + 60_000;
  for (const offset of ["50", "51", "52"]) {
    const next = record("orders.retry.1", NOW - 1000);
    next.message.offset = offset;
    await reinjector.handle(next);
    assert.equal(pins(), 0, `offset ${offset} was re-injected and its pin dropped`);
  }
  assert.equal(producer.sent.length, 3);

  // a still-held record on another partition keeps its own single pin
  const other = record("orders.retry.1", now + 1_000_000);
  other.partition = 3;
  await reinjector.handle(other);
  assert.equal(pins(), 1);
});

test("a null value (tombstone) is forwarded as null, not as an empty byte string", async () => {
  const { producer, reinjector } = fixture();
  const payload = record("orders.retry.1", NOW - 5000);
  (payload.message as { value: Buffer | null }).value = null;
  await reinjector.handle(payload);
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.messages[0]!.value, null);
});
