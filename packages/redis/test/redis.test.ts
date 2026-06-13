import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  RedisConsumer,
  RedisPublisher,
  redisValue,
  type BabelHandlers,
  type RedisClient,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

/** A fake ioredis client that records every command. `reserve` is the queued reserve results. */
class FakeRedis implements RedisClient {
  pushed: { key: string; value: string }[] = [];
  removed: { key: string; value: string }[] = [];
  private reserve: (string | null)[];

  constructor(reserve: (string | null)[] = []) {
    this.reserve = [...reserve];
  }

  async rpush(key: string, value: string): Promise<number> {
    this.pushed.push({ key, value });
    return this.pushed.length;
  }
  async brpoplpush(): Promise<string | null> {
    return this.reserve.length ? this.reserve.shift()! : null;
  }
  async lrem(key: string, count: number, value: string): Promise<number> {
    void count;
    this.removed.push({ key, value });
    return 1;
  }
}

// --- publisher / payload identity ---------------------------------------------

test("publish RPUSHes the byte-identical envelope and returns the message id", async () => {
  const client = new FakeRedis();
  const id = await RedisPublisher.create(client, "orders").publish(URN, { order_id: 7 }, { traceId: "trace-1" });

  assert.equal(client.pushed.length, 1);
  assert.equal(client.pushed[0]!.key, "orders");
  const decoded = EnvelopeCodec.decode(client.pushed[0]!.value) as Envelope;
  assert.equal(decoded.job, URN);
  assert.equal(decoded.meta.queue, "orders");
  assert.equal(decoded.meta.id, id);
});

test("redisValue is the encoded envelope verbatim (no wrapping)", () => {
  const env = envelope();
  assert.equal(redisValue(env), EnvelopeCodec.encode(env));
});

// --- consumer: reserve / route / ack ------------------------------------------

test("poll reserves, routes by URN and LREMs from the processing list on success", async () => {
  const env = envelope(0);
  const client = new FakeRedis([EnvelopeCodec.encode(env)]);
  let seen: Envelope | null = null;
  const handlers: BabelHandlers = { [URN]: (e) => { seen = e; } };

  const handled = await new RedisConsumer(client, "orders", handlers).poll();

  assert.ok(handled);
  assert.equal(seen!.job, URN);
  assert.equal(client.removed.length, 1);
  assert.equal(client.removed[0]!.key, "orders:processing");
});

test("poll returns false when the reserve times out", async () => {
  const client = new FakeRedis([]);
  assert.equal(await new RedisConsumer(client, "orders", {}).poll(), false);
});

test("a throwing handler requeues with attempts + 1 and acks the reservation", async () => {
  const env = envelope(0);
  const client = new FakeRedis();
  let reported: unknown;
  const options = { maxTries: 3, onError: (e: unknown) => { reported = e; } };

  await new RedisConsumer(client, "orders", { [URN]: () => { throw new Error("boom"); } }, options).handle(
    EnvelopeCodec.encode(env),
  );

  assert.ok(reported instanceof Error);
  // requeued onto the work list with attempts incremented...
  assert.equal(client.pushed.length, 1);
  assert.equal(client.pushed[0]!.key, "orders");
  assert.equal((EnvelopeCodec.decode(client.pushed[0]!.value) as Envelope).attempts, 1);
  // ...and removed from the reservation list.
  assert.equal(client.removed[0]!.key, "orders:processing");
});

test("a terminal failure dead-letters to <queue>.dlq with a dead_letter block", async () => {
  const env = envelope(2); // attempts 2, next 3 == maxTries
  const client = new FakeRedis();

  await new RedisConsumer(client, "orders", { [URN]: () => { throw new Error("boom"); } }, { maxTries: 3 }).handle(
    EnvelopeCodec.encode(env),
  );

  const dlqPush = client.pushed.find((p) => p.key === "orders.dlq");
  assert.ok(dlqPush);
  assert.equal((EnvelopeCodec.decode(dlqPush!.value) as Envelope).dead_letter!.reason, "failed");
});

test("a non-conformant message is forwarded raw to the DLQ and acked", async () => {
  const client = new FakeRedis();
  let reported: unknown;

  await new RedisConsumer(client, "orders", {}, { onError: (e: unknown) => { reported = e; } }).handle("not-json");

  assert.ok(reported);
  const dlqPush = client.pushed.find((p) => p.key === "orders.dlq");
  assert.equal(dlqPush!.value, "not-json");
  assert.equal(client.removed.length, 1);
});

// --- consumer: unknown URN ----------------------------------------------------

test("unknown URN with fail throws and leaves the reservation recoverable", async () => {
  const client = new FakeRedis();
  await assert.rejects(new RedisConsumer(client, "orders", {}).handle(EnvelopeCodec.encode(envelope())), UnknownUrnError);
  assert.equal(client.removed.length, 0);
});

test("unknown URN with delete acks without routing", async () => {
  const client = new FakeRedis();
  await new RedisConsumer(client, "orders", {}, { unknownUrn: UnknownUrnStrategy.DELETE }).handle(
    EnvelopeCodec.encode(envelope()),
  );
  assert.equal(client.removed.length, 1);
  assert.equal(client.pushed.length, 0);
});

test("unknown URN with dead_letter dead-letters and acks", async () => {
  const client = new FakeRedis();
  await new RedisConsumer(client, "orders", {}, { unknownUrn: UnknownUrnStrategy.DEAD_LETTER }).handle(
    EnvelopeCodec.encode(envelope()),
  );
  const dlqPush = client.pushed.find((p) => p.key === "orders.dlq");
  assert.equal((EnvelopeCodec.decode(dlqPush!.value) as Envelope).dead_letter!.reason, "unknown_urn");
  assert.equal(client.removed.length, 1);
});

test("run stops when the supplier returns false", async () => {
  const client = new FakeRedis();
  await new RedisConsumer(client, "orders", {}).run(() => false);
  assert.equal(client.pushed.length, 0);
});
