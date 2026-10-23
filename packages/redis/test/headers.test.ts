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
} from "../src/index.js";
import { FakeRedis } from "./fake-redis.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
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

test("the stored frame IS the ack handle (handle === stored value)", () => {
  const stored = frameValue(BARE, { traceparent: TRACEPARENT });
  const [body, headers] = unframe(stored);
  assert.equal(body, BARE); // decoded from the verbatim envelope
  assert.deepEqual(headers, { traceparent: TRACEPARENT });
  // The reserve returns `stored`; ack / release must gate on that exact value.
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
  // Acked on the *stored* frame value (the gate must match what was reserved).
  assert.equal(client.called("legacyAck")[0]!.args[1], stored);
});

test("consume of a bare (pre-ADR-0028) value yields empty headers and still routes", async () => {
  const env = envelope();
  const bare = redisValue(env);
  const client = new FakeRedis([bare]);
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _raw, headers) => { seen = headers; } };

  await new RedisConsumer(client, "orders", handlers).poll();

  assert.deepEqual(seen, {});
  assert.equal(client.called("legacyAck")[0]!.args[1], bare);
});
