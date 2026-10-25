// Lua / layout integration tests against a REAL Redis (the mock-based tests cannot prove the
// scripts). Reachability follows the conformance runner contract: with BQ_REQUIRE_REDIS set an
// unreachable Redis fails the run, otherwise these tests are skipped with a log message.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, before, test } from "node:test";
import type { TestContext } from "node:test";
import { fileURLToPath } from "node:url";

import { BabelQueueError, EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";
import type { Redis } from "ioredis";

import {
  RedisConsumer,
  RedisPublisher,
  RedisQueue,
  frameValue,
  newReservationToken,
  zsetKeys,
  type BabelHandlers,
  type RedisQueueOptions,
} from "../src/index.js";
import { assertPoisonRecord } from "./poison-assert.js";
import { asClient, cleanup, sleep, tryConnect, uniqueQueue } from "./redis-live.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

let redis: Redis | null = null;

before(async () => {
  redis = await tryConnect();
});

after(() => {
  redis?.disconnect();
});

/** A test that needs a live Redis: skipped (with the reason logged by tryConnect) when absent. */
function live(name: string, fn: (client: Redis, queueName: string, t: TestContext) => Promise<void>): void {
  test(name, async (t) => {
    if (redis === null) {
      t.skip("live Redis unreachable (BQ_REQUIRE_REDIS unset)");
      return;
    }
    const queueName = uniqueQueue("it");
    try {
      await fn(redis, queueName, t);
    } finally {
      await cleanup(redis, queueName);
    }
  });
}

function body(attempts = 0, extra: Record<string, unknown> = {}): string {
  const envelope = EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" });
  return JSON.stringify({ ...envelope, attempts, ...extra });
}

/** A queue over a pinned, mutable clock (epoch seconds). */
function pinned(client: Redis, name: string, clock: { now: number }, options: RedisQueueOptions = {}): RedisQueue {
  return new RedisQueue(asClient(client), name, { ...options, clock: () => clock.now });
}

// --- zset: recovery ------------------------------------------------------------------

live("zset: an unacked reservation is recovered after retryAfter and delivered with attempts + 1", async (client, name) => {
  const clock = { now: 1000 };
  const first = pinned(client, name, clock, { layout: "zset", retryAfter: 90 });
  await first.push(body(0));

  const reserved = await first.reserve();
  assert.equal(reserved.outcome, "delivered");
  assert.equal(reserved.attempts, 1);

  // A second consumer sees nothing before the reservation expires...
  const second = pinned(client, name, clock, { layout: "zset", retryAfter: 90 });
  assert.equal((await second.reserve()).outcome, "empty");

  // ...and recovers it (never acked) once `floor(now) >= score`.
  clock.now = 1090;
  const recovered = await second.reserve();
  assert.equal(recovered.outcome, "delivered");
  assert.equal(recovered.attempts, 2);
  assert.equal((JSON.parse(recovered.raw as string) as Envelope).attempts, 1);
});

// --- zset: A1 poison -----------------------------------------------------------------

live("zset: undecodable bodies and non-numeric attempts are dead-lettered with no :reserved residue", async (client, name) => {
  const keys = zsetKeys(name);
  const poison = ["not-json", body(0).replace('"attempts":0', '"attempts":"3"'), "[1,2]", ""];
  await client.rpush(keys.ready, ...poison);
  await client.rpush(keys.notify, ...poison.map(() => "1"));

  const errors: unknown[] = [];
  const consumer = new RedisConsumer(asClient(client), name, {}, {
    layout: "zset",
    blockTimeout: 0,
    onError: (error) => errors.push(error),
  });
  for (let i = 0; i < poison.length; i++) assert.equal(await consumer.poll(), true);

  assert.equal(errors.length, poison.length);
  const dlq = await client.lrange(`${name}.dlq`, 0, -1);
  assert.equal(dlq.length, poison.length);
  poison.forEach((body, i) => assertPoisonRecord(dlq[i]!, body, name));
  assert.equal(await client.zcard(keys.reserved), 0, "no residue in :reserved");
  assert.equal(await client.llen(keys.ready), 0);
});

live("zset: bodies that cjson decodes but cannot re-encode are dead-lettered, never lost (A1)", async (client, name) => {
  const keys = zsetKeys(name);
  // lua-cjson accepts these numbers on decode and throws on encode (encode_invalid_numbers is off)
  const poison = ['{"job":"x","attempts":1e999}', '{"job":"x","attempts":0,"data":{"n":-1e999}}'];
  await client.rpush(keys.ready, ...poison);
  await client.rpush(keys.notify, ...poison.map(() => "1"));

  const errors: unknown[] = [];
  const consumer = new RedisConsumer(asClient(client), name, {}, {
    layout: "zset",
    blockTimeout: 0,
    onError: (error) => errors.push(error),
  });
  for (let i = 0; i < poison.length; i++) assert.equal(await consumer.poll(), true);

  assert.equal(errors.length, poison.length);
  const dlq = await client.lrange(`${name}.dlq`, 0, -1);
  assert.equal(dlq.length, poison.length, "none lost");
  poison.forEach((body, i) => assertPoisonRecord(dlq[i]!, body, name)); // spliced: every original byte kept
  assert.equal(await client.zcard(keys.reserved), 0, "no residue in :reserved");
  assert.equal(await client.llen(keys.ready), 0);
});

live("zset: two byte-equal poison bodies reserved together both reach the DLQ (section 1.1, unconditional)", async (client, name) => {
  const keys = zsetKeys(name);
  const twins = ["not-json", "not-json"];
  await client.rpush(keys.ready, ...twins);
  await client.rpush(keys.notify, ...twins.map(() => "1"));

  // two workers reserve before either dead-letters: both ZADD the same member, `:reserved` holds one
  const a = new RedisQueue(asClient(client), name, { layout: "zset" });
  const b = new RedisQueue(asClient(client), name, { layout: "zset" });
  const first = await a.reserve();
  const second = await b.reserve();
  assert.equal(first.outcome, "poison");
  assert.equal(second.outcome, "poison");
  assert.equal(await client.zcard(keys.reserved), 1, "byte-equal copies share one :reserved member");

  assert.equal(await a.deadLetter(first.reservation!), true);
  assert.equal(await b.deadLetter(second.reservation!), true, "the second copy is written even though its ZREM finds nothing");
  assert.deepEqual(await client.lrange(`${name}.dlq`, 0, -1), twins, "neither copy is lost");
  assert.equal(await client.zcard(keys.reserved), 0);
  assert.equal(await client.llen(keys.ready), 0);
});

live("zset: the reserve script flags a non-JSON body as poison explicitly", async (client, name) => {
  const keys = zsetKeys(name);
  const queue = new RedisQueue(asClient(client), name, { layout: "zset" });
  await client.rpush(keys.ready, "not-json");
  await client.rpush(keys.notify, "1");
  const result = await queue.reserve();
  assert.equal(result.outcome, "poison");
  assert.equal(result.attempts, undefined);
});

live("zset: a late dead-letter after the reservation was recovered does not duplicate the message", async (client, name) => {
  const clock = { now: 1000 };
  const keys = zsetKeys(name);
  const first = pinned(client, name, clock, { layout: "zset", retryAfter: 10 });
  await first.push(body(0));
  const stale = await first.reserve();
  assert.equal(stale.outcome, "delivered");

  clock.now = 1010; // the reservation expires and a second consumer recovers it
  const second = pinned(client, name, clock, { layout: "zset", retryAfter: 10 });
  const recovered = await second.reserve();
  assert.equal(recovered.outcome, "delivered");

  // the first consumer, late, dead-letters its stale reservation: nothing may be written
  assert.equal(await first.deadLetter(stale.reservation!), false);
  assert.equal(await client.llen(`${name}.dlq`), 0, "no duplicate in the DLQ");
  assert.equal(await second.deadLetter(recovered.reservation!), true);
  assert.deepEqual(await client.lrange(`${name}.dlq`, 0, -1), [recovered.raw]);
  assert.equal(await client.zcard(keys.reserved), 0);
});

// --- zset: release (A2) --------------------------------------------------------------

live("zset: release changes only attempts and schedules floor(now)+ceil(delay)", async (client, name) => {
  const clock = { now: 1000.7 };
  const queue = pinned(client, name, clock, { layout: "zset" });
  const original = body(2, { x_ext: "kept", x_empty_obj: {}, x_empty_arr: [], x_big: 123456789012345 });
  await queue.push(original);

  const reserved = await queue.reserve();
  assert.equal(reserved.outcome, "delivered");
  assert.equal(reserved.raw, original, "the handler gets the raw body untouched");
  await queue.release(reserved.reservation!, 1.5);

  const keys = zsetKeys(name);
  const delayed = await client.zrange(keys.delayed, 0, -1, "WITHSCORES");
  assert.equal(delayed.length, 2);
  assert.equal(Number(delayed[1]), 1002, "floor(1000.7) + ceil(1.5)");
  const released = JSON.parse(delayed[0]!) as Record<string, unknown>;
  assert.deepEqual(released, { ...JSON.parse(original), attempts: 3 });
  assert.ok(delayed[0]!.includes('"x_empty_obj":{}') && delayed[0]!.includes('"x_empty_arr":[]'));
  assert.ok(delayed[0]!.includes('"x_big":123456789012345'));

  assert.equal(await client.zcard(keys.reserved), 0, "the reservation is gone");
  assert.equal(await client.llen(keys.notify), 0, "a delayed publish/release writes no notify token");
});

live("zset: release keeps integers above 2^53 in unknown keys exact", async (client, name) => {
  const clock = { now: 1000 };
  const queue = pinned(client, name, clock, { layout: "zset" });
  const original = body(2).replace(/\}$/, ',"x_huge":9007199254740993,"x_neg":-9007199254740993}');
  assert.ok(original.includes('"x_huge":9007199254740993'));
  await queue.push(original);

  const reserved = await queue.reserve();
  assert.equal(reserved.raw, original);
  await queue.release(reserved.reservation!, 0);

  const delayed = await client.zrange(zsetKeys(name).delayed, 0, -1);
  assert.equal(delayed.length, 1);
  assert.ok(delayed[0]!.includes('"x_huge":9007199254740993'), "not rounded by a JSON round trip");
  assert.ok(delayed[0]!.includes('"x_neg":-9007199254740993'));
  assert.ok(delayed[0]!.includes('"attempts":3'));
});

// --- zset: A3 + delayed delivery window ------------------------------------------------

live("zset: no __bq_frame, headers are dropped (A3)", async (client, name) => {
  await RedisPublisher.create(asClient(client), name, { layout: "zset" }).publish(
    URN,
    { order_id: 7 },
    { headers: { traceparent: TRACEPARENT } },
  );
  const stored = await client.lrange(zsetKeys(name).ready, 0, -1);
  assert.equal(stored.length, 1);
  assert.ok(!stored[0]!.includes("__bq_frame"));
  assert.ok(!stored[0]!.includes("traceparent"));
  assert.equal((JSON.parse(stored[0]!) as Envelope).job, URN);
  assert.equal(await client.llen(zsetKeys(name).notify), 1, "an immediate push writes one notify token");

  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _raw, headers) => { seen = headers; } };
  await new RedisConsumer(asClient(client), name, handlers, { layout: "zset", blockTimeout: 1 }).poll();
  assert.deepEqual(seen, {});
});

live("zset: a 2 s delayed publish arrives within [1.0, 3.5] s", async (client, name) => {
  const startedAt = Date.now();
  await RedisPublisher.create(asClient(client), name, { layout: "zset" }).publish(URN, { order_id: 7 }, { delay: 2 });
  assert.equal(await client.llen(zsetKeys(name).ready), 0);

  let arrivedAt = 0;
  const handlers: BabelHandlers = { [URN]: () => { arrivedAt = Date.now(); } };
  const consumer = new RedisConsumer(asClient(client), name, handlers, { layout: "zset", blockTimeout: 6 });
  assert.equal(await consumer.poll(), true);

  const elapsed = (arrivedAt - startedAt) / 1000;
  assert.ok(elapsed >= 1.0 && elapsed <= 3.5, `arrived after ${elapsed}s`);
});

live("zset: a failing handler is released with attempts + 1 and dead-lettered at maxTries", async (client, name) => {
  await RedisPublisher.create(asClient(client), name, { layout: "zset" }).publish(URN, { order_id: 7 });
  const attemptsSeen: number[] = [];
  const handlers: BabelHandlers = {
    [URN]: (envelope) => {
      attemptsSeen.push(envelope.attempts ?? 0);
      throw new Error("boom");
    },
  };
  const consumer = new RedisConsumer(asClient(client), name, handlers, { layout: "zset", blockTimeout: 3, maxTries: 3 });
  for (let i = 0; i < 3; i++) assert.equal(await consumer.poll(), true);

  assert.deepEqual(attemptsSeen, [0, 1, 2]);
  const keys = zsetKeys(name);
  assert.equal(await client.llen(`${name}.dlq`), 1);
  // ADR-0036 decision 8: the third delivery (job.attempts 2) is written with the counted value 3.
  const record = JSON.parse((await client.lindex(`${name}.dlq`, 0)) as string) as Envelope;
  assert.equal(record.dead_letter?.reason, "failed");
  assert.equal(record.dead_letter?.attempts, 3);
  assert.equal(record.attempts, 3);
  assert.equal(await client.zcard(keys.reserved), 0);
  assert.equal(await client.zcard(keys.delayed), 0);
  assert.equal(await client.llen(keys.ready), 0);
});

// --- legacy :processing -----------------------------------------------------------------

live("legacy: a killed consumer's message is swept back byte-identical, attempts unchanged", async (client, name) => {
  const clock = { now: 5000 };
  const crashed = pinned(client, name, clock, { retryAfter: 10 });
  const payload = body(1, { x_ext: { nested: [1, 2, 3] } });
  await crashed.push(payload);
  // An item without a deadline (e.g. from an older consumer) must stay where it is.
  const foreign = body(0, { x_foreign: true });
  await client.rpush(`${name}:processing`, foreign);

  const reserved = await crashed.reserve();
  assert.equal(reserved.raw, payload, "legacy delivers the raw body as-is");
  assert.equal(reserved.attempts, undefined, "legacy reports no delivery count");
  // kill -9: neither ack nor release.

  clock.now = 5011;
  const survivor = pinned(client, name, clock, { retryAfter: 10 });
  const again = await survivor.reserve();
  assert.equal(again.outcome, "delivered");
  assert.equal(again.raw, payload, "byte-identical after the sweep");
  assert.equal((JSON.parse(again.raw as string) as Envelope).attempts, 1, "the sweep does not increment attempts");
  assert.deepEqual(await client.lrange(`${name}:processing`, 0, -1), [foreign, payload]);
  assert.equal(await client.zcard(`${name}:processing:deadlines`), 1, "only the new reservation holds a deadline");
});

live("legacy: FIFO order is preserved across reserve/ack", async (client, name) => {
  const queue = new RedisQueue(asClient(client), name, {});
  const items = ["one", "two", "three"].map((id) => body(0, { x_id: id }));
  for (const item of items) await queue.push(item);

  const order: string[] = [];
  for (let i = 0; i < items.length; i++) {
    const r = await queue.reserve();
    order.push(r.raw as string);
    assert.equal(await queue.ack(r.reservation!), true);
  }
  assert.deepEqual(order, items);
  assert.equal(await client.llen(`${name}:processing`), 0);
  assert.equal(await client.zcard(`${name}:processing:deadlines`), 0);
});

live("legacy: retryAfter null stores +inf, acks cleanly and leaves nothing behind", async (client, name) => {
  const clock = { now: 100 };
  const warnings: string[] = [];
  const queue = pinned(client, name, clock, { retryAfter: null, onWarn: (m) => warnings.push(m) });
  await queue.push(body(0));

  const reserved = await queue.reserve();
  const deadlines = await client.zrange(`${name}:processing:deadlines`, 0, -1, "WITHSCORES");
  assert.equal(deadlines.length, 2);
  assert.equal(deadlines[1], "inf");

  clock.now = 9_999_999_999;
  assert.equal(await queue.sweep(), 0, "a +inf deadline is never selected");
  assert.equal(await queue.ack(reserved.reservation!), true);

  assert.deepEqual(warnings, [], "no reservation-expired warning after a clean ack");
  assert.equal(await client.llen(`${name}:processing`), 0);
  assert.equal(await client.zcard(`${name}:processing:deadlines`), 0);
});

live("legacy: a late ack of a swept reservation touches nothing and warns", async (client, name) => {
  const clock = { now: 1000 };
  const warnings: string[] = [];
  const queue = pinned(client, name, clock, { retryAfter: 10, onWarn: (m) => warnings.push(m) });
  const payload = body(0);
  await queue.push(payload);

  const first = await queue.reserve();
  clock.now = 1011;
  const second = await queue.reserve(); // sweeps `first`, re-reserves the same bytes
  assert.equal(second.raw, payload);

  assert.equal(await queue.ack(first.reservation!), false, "the stale token must not ack the new reservation");
  assert.equal(await client.llen(`${name}:processing`), 1);
  const secondToken = (second.reservation as { token: string }).token;
  assert.notEqual(
    await client.zscore(`${name}:processing:deadlines`, `${secondToken}:${second.reservation!.raw}`),
    null,
    "the live reservation's deadline member survives the stale ack",
  );
  assert.equal(await client.zcard(`${name}:processing:deadlines`), 1);
  assert.equal(await queue.ack(second.reservation!), true);
  assert.equal(await client.llen(`${name}:processing`), 0);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0]!, /reservation expired/);
});

live("legacy: identical copies get distinct tokens and are acked independently", async (client, name) => {
  const queue = new RedisQueue(asClient(client), name, {});
  const payload = body(0);
  await queue.push(payload);
  await queue.push(payload);

  const a = await queue.reserve();
  const b = await queue.reserve();
  assert.equal(a.reservation!.layout, "legacy");
  assert.notEqual((a.reservation as { token: string }).token, (b.reservation as { token: string }).token);
  assert.equal(await queue.ack(a.reservation!), true);
  assert.equal(await client.llen(`${name}:processing`), 1, "acking one copy leaves the other");
  const bToken = (b.reservation as { token: string }).token;
  assert.equal(await client.zcard(`${name}:processing:deadlines`), 1, "one deadline member remains");
  assert.notEqual(
    await client.zscore(`${name}:processing:deadlines`, `${bToken}:${b.reservation!.raw}`),
    null,
    "the un-acked copy keeps its own deadline member",
  );
  assert.equal(await queue.ack(b.reservation!), true);
  assert.equal(await client.llen(`${name}:processing`), 0);
});

live("legacy: a published traceparent frame arrives on consume", async (client, name) => {
  await RedisPublisher.create(asClient(client), name).publish(URN, { order_id: 7 }, { headers: { traceparent: TRACEPARENT } });
  let received: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _raw, headers) => { received = headers; } };
  assert.equal(await new RedisConsumer(asClient(client), name, handlers, { blockTimeout: 2 }).poll(), true);
  assert.deepEqual(received, { traceparent: TRACEPARENT });
  assert.equal(await client.llen(`${name}:processing`), 0);
});

live("legacy: the consumer's sleep-poll returns false on an empty queue within the timeout", async (client, name) => {
  const startedAt = Date.now();
  const handled = await new RedisConsumer(asClient(client), name, {}, { blockTimeout: 1 }).poll();
  assert.equal(handled, false);
  assert.ok(Date.now() - startedAt < 2000);
  await sleep(0);
});

// --- no Redis needed ----------------------------------------------------------------------

test("reservation tokens are 32 lowercase hex characters from crypto.randomBytes", () => {
  const tokens = new Set<string>();
  for (let i = 0; i < 50; i++) {
    const token = newReservationToken();
    assert.match(token, /^[0-9a-f]{32}$/);
    tokens.add(token);
  }
  assert.equal(tokens.size, 50);

  const source = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "src", "index.ts"), "utf8");
  assert.ok(source.includes("randomBytes"));
  assert.ok(!source.includes("Math." + "random"), "tokens must never come from a non-cryptographic RNG");
});

test("a negative or non-numeric retryAfter is a configuration error; null and 0 are accepted", () => {
  const client = asClient({} as Redis);
  for (const retryAfter of [-1, Number.NaN, -0.5]) {
    assert.throws(() => new RedisQueue(client, "q", { retryAfter }), BabelQueueError);
    assert.throws(() => new RedisQueue(client, "q", { layout: "zset", retryAfter }), BabelQueueError);
  }
  assert.doesNotThrow(() => new RedisQueue(client, "q", { retryAfter: null }));
  assert.doesNotThrow(() => new RedisQueue(client, "q", { retryAfter: 0 }));
});

test("delayed operations are rejected on the legacy layout and for invalid delays", async () => {
  const client = asClient({} as Redis);
  await assert.rejects(RedisPublisher.create(client, "q").publish(URN, {}, { delay: 5 }), BabelQueueError);
  await assert.rejects(RedisPublisher.create(client, "q", { layout: "zset" }).publish(URN, {}, { delay: -1 }), BabelQueueError);
  await assert.rejects(new RedisQueue(client, "q", {}).pushDelayed("x", 5), BabelQueueError);
  assert.equal(frameValue("x", {}), "x");
});
