import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  RedisConsumer,
  RedisPublisher,
  RedisQueue,
  redisValue,
  type BabelHandlers,
  type Reservation,
} from "../src/index.js";
import { FakeRedis } from "./fake-redis.js";

const TOKEN = "0123456789abcdef0123456789abcdef";

/** A legacy reservation over `raw`, as the legacy reserve script would have produced. */
function legacy(raw: string): Reservation {
  return { layout: "legacy", raw, token: TOKEN };
}

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
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

test("poll reserves, routes by URN and acks through the deadline-gated script on success", async () => {
  const env = envelope(0);
  const client = new FakeRedis([EnvelopeCodec.encode(env)]);
  let seen: Envelope | null = null;
  const handlers: BabelHandlers = { [URN]: (e) => { seen = e; } };

  const handled = await new RedisConsumer(client, "orders", handlers).poll();

  assert.ok(handled);
  assert.equal(seen!.job, URN);
  const ack = client.called("legacyAck");
  assert.equal(ack.length, 1);
  assert.deepEqual(ack[0]!.keys, ["orders:processing", "orders:processing:deadlines"]);
  assert.deepEqual(ack[0]!.args, [client.called("legacyReserve")[0]!.args[1], EnvelopeCodec.encode(env)]);
});

test("poll returns false when the reserve times out", async () => {
  const client = new FakeRedis([]);
  assert.equal(await new RedisConsumer(client, "orders", {}, { blockTimeout: 0 }).poll(), false);
});

test("a throwing handler requeues with attempts + 1 and acks the reservation", async () => {
  const env = envelope(0);
  const client = new FakeRedis();
  let reported: unknown;
  const options = { maxTries: 3, onError: (e: unknown) => { reported = e; } };

  await new RedisConsumer(client, "orders", { [URN]: () => { throw new Error("boom"); } }, options).handle(
    legacy(EnvelopeCodec.encode(env)),
  );

  assert.ok(reported instanceof Error);
  // one gated script puts the message back with attempts incremented and settles the reservation
  const release = client.called("legacyRelease");
  assert.equal(release.length, 1);
  assert.equal(release[0]!.keys[0], "orders");
  assert.equal((EnvelopeCodec.decode(release[0]!.args[2]!) as Envelope).attempts, 1);
});

test("a terminal failure dead-letters to <queue>.dlq with a dead_letter block", async () => {
  const env = envelope(2); // attempts 2, next 3 == maxTries
  const client = new FakeRedis();

  await new RedisConsumer(client, "orders", { [URN]: () => { throw new Error("boom"); } }, { maxTries: 3 }).handle(
    legacy(EnvelopeCodec.encode(env)),
  );

  const dlq = client.called("legacyDeadLetter");
  assert.equal(dlq.length, 1);
  assert.equal(dlq[0]!.keys[2], "orders.dlq");
  assert.equal((EnvelopeCodec.decode(dlq[0]!.args[2]!) as Envelope).dead_letter!.reason, "failed");
});

test("a non-conformant message is forwarded raw to the DLQ and acked", async () => {
  const client = new FakeRedis();
  let reported: unknown;

  await new RedisConsumer(client, "orders", {}, { onError: (e: unknown) => { reported = e; } }).handle(legacy("not-json"));

  assert.ok(reported);
  const dlq = client.called("legacyDeadLetter");
  assert.equal(dlq[0]!.args[2], "not-json");
});

// --- consumer: unknown URN ----------------------------------------------------

test("unknown URN with fail throws and leaves the reservation recoverable", async () => {
  const client = new FakeRedis();
  await assert.rejects(new RedisConsumer(client, "orders", {}).handle(legacy(EnvelopeCodec.encode(envelope()))), UnknownUrnError);
  assert.equal(client.calls.length, 0);
});

test("unknown URN with delete acks without routing", async () => {
  const client = new FakeRedis();
  await new RedisConsumer(client, "orders", {}, { unknownUrn: UnknownUrnStrategy.DELETE }).handle(
    legacy(EnvelopeCodec.encode(envelope())),
  );
  assert.equal(client.called("legacyAck").length, 1);
  assert.equal(client.pushed.length, 0);
});

test("unknown URN with dead_letter dead-letters and acks", async () => {
  const client = new FakeRedis();
  await new RedisConsumer(client, "orders", {}, { unknownUrn: UnknownUrnStrategy.DEAD_LETTER }).handle(
    legacy(EnvelopeCodec.encode(envelope())),
  );
  const dlq = client.called("legacyDeadLetter");
  assert.equal((EnvelopeCodec.decode(dlq[0]!.args[2]!) as Envelope).dead_letter!.reason, "unknown_urn");
});

test("run stops when the supplier returns false", async () => {
  const client = new FakeRedis();
  await new RedisConsumer(client, "orders", {}).run(() => false);
  assert.equal(client.pushed.length, 0);
});

// --- zset: lossless release, explicit poison flag, gated dead-letter ----------------

function zset(raw: string): Reservation {
  return { layout: "zset", raw, reserved: raw };
}

test("zset release raises only attempts and keeps every other byte, including integers above 2^53", async () => {
  const client = new FakeRedis();
  const queue = new RedisQueue(client, "orders", { layout: "zset" });
  const raw =
    '{ "job" : "urn:babel:orders:created", "big": 9007199254740993, "attempts" : 2 ,"nested":{"attempts":7,"s":"a\\"}b"},"tail":[1,2,{"x":1}] }';
  await queue.release(zset(raw), 0);
  const sent = client.called("zsetRelease")[0]!.args[2]!;
  assert.equal(
    sent,
    '{ "job" : "urn:babel:orders:created", "big": 9007199254740993, "attempts" : 3 ,"nested":{"attempts":7,"s":"a\\"}b"},"tail":[1,2,{"x":1}] }',
  );
  assert.ok(sent.includes("9007199254740993"));
});

test("zset release of a big attempts literal is exact, a missing attempts is appended as 1", async () => {
  const client = new FakeRedis();
  const queue = new RedisQueue(client, "orders", { layout: "zset" });
  await queue.release(zset('{"a":1,"attempts":9007199254740993}'), 0);
  await queue.release(zset('{"a":1}'), 0);
  await queue.release(zset("{}"), 0);
  await queue.release(zset('{"attempts":1,"attempts":4}'), 0);
  await queue.release(zset("not-json"), 0);
  const sent = client.called("zsetRelease").map((c) => c.args[2]);
  assert.deepEqual(sent, [
    '{"a":1,"attempts":9007199254740994}',
    '{"a":1,"attempts":1}',
    '{"attempts":1}',
    '{"attempts":5,"attempts":5}',
    "not-json",
  ]);
});

test("zset reserve trusts the script's poison flag, and falls back to reserved == job without one", async () => {
  const client = new FakeRedis();
  const queue = new RedisQueue(client, "orders", { layout: "zset" });
  const a = '{"attempts":1e999}';
  // the flag says poison even though reserved differs from the body ...
  client.zsetReserves.push([a, "other", "1"]);
  assert.equal((await queue.reserve()).outcome, "poison");
  // ... and a delivered flag wins even for identical strings
  client.zsetReserves.push([a, a, "0"]);
  assert.equal((await queue.reserve()).outcome, "delivered");
  // a two-element reply (older double) keeps the equality signal
  client.zsetReserves.push([a, a]);
  assert.equal((await queue.reserve()).outcome, "poison");
});

test("a zset dead-letter whose reservation was already recovered writes nothing and warns", async () => {
  const warnings: string[] = [];
  const client = new FakeRedis();
  client.gateReply = 0;
  const queue = new RedisQueue(client, "orders", { layout: "zset", onWarn: (m) => warnings.push(m) });
  assert.equal(await queue.deadLetter(zset('{"attempts":1}')), false);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0]!, /reservation expired/);
  client.gateReply = 1;
  assert.equal(await queue.deadLetter(zset('{"attempts":1}')), true);
});

test("a zset poison dead-letter is unconditional: it writes even when a byte-equal twin already removed the shared member", async () => {
  const client = new FakeRedis();
  // the ZREM-gated script would answer 0 here (the twin took the shared `:reserved` member)
  client.gateReply = 0;
  const queue = new RedisQueue(client, "orders", { layout: "zset" });

  client.zsetReserves.push(["not-json", "not-json", "1"]);
  const first = await queue.reserve();
  client.zsetReserves.push(["not-json", "not-json", "1"]);
  const second = await queue.reserve();
  assert.equal(first.outcome, "poison");
  assert.equal(second.outcome, "poison");

  assert.equal(await queue.deadLetter(first.reservation!), true);
  assert.equal(await queue.deadLetter(second.reservation!), true, "the second twin is not dropped");
  assert.equal(client.called("zsetDeadLetterPoison").length, 2);
  assert.equal(client.called("zsetDeadLetter").length, 0);
});

test("a terminal-failure dead-letter of a non-poison reservation keeps the ZREM gate", async () => {
  const client = new FakeRedis();
  client.gateReply = 0;
  const queue = new RedisQueue(client, "orders", { layout: "zset", onWarn: () => undefined });
  client.zsetReserves.push(['{"attempts":1}', '{"attempts":2}', "0"]);
  const delivered = await queue.reserve();
  assert.equal(delivered.outcome, "delivered");
  assert.equal(await queue.deadLetter(delivered.reservation!), false);
  assert.equal(client.called("zsetDeadLetterPoison").length, 0);
});
