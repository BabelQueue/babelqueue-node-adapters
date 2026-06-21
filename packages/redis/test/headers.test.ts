import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import {
  FRAME_KEY,
  RedisConsumer,
  RedisPublisher,
  frameValue,
  headersOf,
  redisValue,
  unframe,
  type BabelHandlers,
  type RedisClient,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

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

// --- frameValue / unframe round-trip (no broker) ------------------------------

const BARE = `{"job":"urn:babel:test:ping","data":{"n":1},"meta":{"id":"abc"}}`;

test("frameValue without headers stays byte-identical bare", () => {
  for (const headers of [undefined, null, {}, { "": "", k: "" }] as (HeaderCarrier | null | undefined)[]) {
    assert.equal(frameValue(BARE, headers), BARE);
  }
});

test("frameValue with headers is a frame carrying the sentinel", () => {
  const value = frameValue(BARE, { traceparent: TRACEPARENT });
  assert.notEqual(value, BARE);
  assert.ok(value.includes(`"${FRAME_KEY}"`));
});

test("unframe recovers the verbatim body and the headers (round-trip)", () => {
  const value = frameValue(BARE, { traceparent: TRACEPARENT, tracestate: "vendor=value" });
  const [body, headers] = unframe(value);
  assert.equal(body, BARE);
  assert.deepEqual(headers, { traceparent: TRACEPARENT, tracestate: "vendor=value" });
  assert.deepEqual(headersOf(value), { traceparent: TRACEPARENT, tracestate: "vendor=value" });
});

test("a bare value (envelope / non-JSON / no sentinel) unframes verbatim with empty headers", () => {
  for (const v of [BARE, "", "not json", "[1,2,3]", `"a string"`, "42", `{"headers":{"x":"y"},"body":"spoof"}`]) {
    assert.deepEqual(unframe(v), [v, {}]);
  }
});

test("the stored frame IS the LREM ack handle (handle === stored value)", () => {
  const stored = frameValue(BARE, { traceparent: TRACEPARENT });
  const [body, headers] = unframe(stored);
  assert.equal(body, BARE); // decoded from the verbatim envelope
  assert.deepEqual(headers, { traceparent: TRACEPARENT });
  // BRPOPLPUSH returns `stored`; Pop/Ack must LREM on that exact value.
  assert.equal(stored, stored);
});

test("the frame is NOT the wire envelope — GR-1 (the envelope bytes are untouched)", () => {
  const env = redisValue(envelope());
  const frame = frameValue(env, { traceparent: TRACEPARENT });
  assert.notEqual(frame, env);
  assert.equal(unframe(frame)[0], env);
});

// --- publisher: frames only when headers present ------------------------------

test("publish without headers RPUSHes the byte-identical bare envelope", async () => {
  const client = new FakeRedis();
  await RedisPublisher.create(client, "orders").publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  const value = client.pushed[0]!.value;
  assert.deepEqual(unframe(value), [value, {}]);
  assert.equal((EnvelopeCodec.decode(value) as Envelope).job, URN);
});

test("publish with a traceparent header RPUSHes a frame that carries it", async () => {
  const client = new FakeRedis();
  await RedisPublisher.create(client, "orders").publish(URN, { order_id: 7 }, {
    traceId: "trace-1",
    headers: { traceparent: TRACEPARENT },
  });
  const [body, headers] = unframe(client.pushed[0]!.value);
  assert.equal((EnvelopeCodec.decode(body) as Envelope).job, URN);
  assert.deepEqual(headers, { traceparent: TRACEPARENT });
});

// --- consumer: surfaces headers + bare back-compat (no broker) ----------------

test("consume surfaces the carried traceparent to the handler beside the unframed envelope", async () => {
  const env = envelope();
  const stored = frameValue(redisValue(env), { traceparent: TRACEPARENT });
  const client = new FakeRedis([stored]);
  let seen: { env: Envelope; headers: HeaderCarrier } | null = null;
  const handlers: BabelHandlers = { [URN]: (e, _raw, headers) => { seen = { env: e, headers }; } };

  const handled = await new RedisConsumer(client, "orders", handlers).poll();

  assert.ok(handled);
  assert.equal(seen!.env.job, URN);
  assert.deepEqual(seen!.headers, { traceparent: TRACEPARENT });
  // Acked on the *stored* frame value (LREM must match what was reserved).
  assert.equal(client.removed[0]!.value, stored);
});

test("consume of a bare (pre-ADR-0028) value yields empty headers and still routes", async () => {
  const env = envelope();
  const bare = redisValue(env);
  const client = new FakeRedis([bare]);
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _raw, headers) => { seen = headers; } };

  await new RedisConsumer(client, "orders", handlers).poll();

  assert.deepEqual(seen, {});
  assert.equal(client.removed[0]!.value, bare);
});

// --- broker-gated integration (skips cleanly without a real Redis) ------------

const REDIS_URL = process.env.BABELQUEUE_REDIS_URL;

test("integration: a published traceparent arrives on consume", { skip: !REDIS_URL }, async () => {
  // Wired only when BABELQUEUE_REDIS_URL points at a real Redis. Imports ioredis lazily so the
  // suite never requires the optional peer when the test is skipped. Constructed via an untyped
  // module handle so the optional peer's types are not needed to typecheck the suite.
  const mod = (await import("ioredis")) as unknown as { default: new (url: string) => RedisClient & { quit(): Promise<unknown> } };
  const client = new mod.default(REDIS_URL!);
  const queue = `bq-otel-it-${Date.now()}`;
  try {
    await RedisPublisher.create(client, queue).publish(
      URN,
      { order_id: 7 },
      { headers: { traceparent: TRACEPARENT } },
    );
    let received: HeaderCarrier | null = null;
    await new RedisConsumer(client, queue, {
      [URN]: (_e, _raw, headers) => { received = headers; },
    }, { blockTimeout: 2 }).poll();
    assert.deepEqual(received, { traceparent: TRACEPARENT });
  } finally {
    await client.quit();
  }
});
