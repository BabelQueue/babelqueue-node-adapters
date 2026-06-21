import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import { decodeMetadata, encodeMetadata } from "../src/headers.js";
import { headersOf, processor, publish, type BabelHandlers } from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

// A duck-typed BullMQ Queue: records add() calls; no Redis.
function fakeQueue(name = "orders") {
  const added: Array<{ name: string; data: unknown; opts: unknown }> = [];
  return {
    name,
    added,
    async add(jobName: string, data: unknown, opts?: unknown) {
      added.push({ name: jobName, data, opts });
      return { id: "1" };
    },
  };
}

// A duck-typed BullMQ Job carrying envelope data + opts.
function fakeJob(data: unknown, opts: unknown = {}, id = "1") {
  return { id, data, opts } as unknown as import("bullmq").Job;
}

// --- metadata encode/decode round-trip (pure) ---------------------------------

test("encodeMetadata is undefined for no usable headers, JSON for a carrier", () => {
  assert.equal(encodeMetadata(undefined), undefined);
  assert.equal(encodeMetadata({}), undefined);
  assert.equal(encodeMetadata({ "": "", k: "" }), undefined);
  assert.deepEqual(decodeMetadata(encodeMetadata({ traceparent: TRACEPARENT })), { traceparent: TRACEPARENT });
});

test("decodeMetadata is empty for missing / malformed / non-object values", () => {
  assert.deepEqual(decodeMetadata(undefined), {});
  assert.deepEqual(decodeMetadata(""), {});
  assert.deepEqual(decodeMetadata("not json"), {});
  assert.deepEqual(decodeMetadata("[1,2,3]"), {});
  assert.deepEqual(decodeMetadata("42"), {});
});

// --- publish carries the traceparent in telemetry.metadata, not in the envelope

test("publish without headers leaves job opts (and the envelope) untouched", async () => {
  const queue = fakeQueue();
  await publish(queue as never, URN, { order_id: 7 });
  const job = queue.added[0]!;
  // The original (undefined) options are passed through unchanged — no telemetry slot is created.
  assert.equal(job.opts, undefined);
});

test("publish with a traceparent stores it in telemetry.metadata, envelope unchanged (GR-1)", async () => {
  const queue = fakeQueue();
  await publish(queue as never, URN, { order_id: 7 }, { headers: { traceparent: TRACEPARENT } });
  const job = queue.added[0]!;
  const metadata = (job.opts as { telemetry?: { metadata?: string } }).telemetry?.metadata;
  assert.deepEqual(decodeMetadata(metadata), { traceparent: TRACEPARENT });
  // The job data is still the pure canonical envelope.
  const env = job.data as Envelope;
  assert.equal(env.job, URN);
  assert.equal(EnvelopeCodec.urn(env), URN);
});

test("an explicit jobsOptions.telemetry.metadata wins (merge-not-clobber)", async () => {
  const queue = fakeQueue();
  await publish(queue as never, URN, { order_id: 7 }, {
    headers: { traceparent: TRACEPARENT },
    jobsOptions: { telemetry: { metadata: "explicit" } },
  });
  const metadata = (queue.added[0]!.opts as { telemetry?: { metadata?: string } }).telemetry?.metadata;
  assert.equal(metadata, "explicit");
});

// --- headersOf + processor surfaces headers to the handler --------------------

test("headersOf reads the carrier back from a job's telemetry.metadata", () => {
  const job = fakeJob({}, { telemetry: { metadata: encodeMetadata({ traceparent: TRACEPARENT }) } });
  assert.deepEqual(headersOf(job), { traceparent: TRACEPARENT });
  assert.deepEqual(headersOf(fakeJob({}, {})), {});
});

test("the processor surfaces the carried traceparent to the handler", async () => {
  const env = EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders" });
  const job = fakeJob(env, { telemetry: { metadata: encodeMetadata({ traceparent: TRACEPARENT }) } });
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _j, headers) => { seen = headers; } };

  await processor(handlers)(job);

  assert.deepEqual(seen, { traceparent: TRACEPARENT });
});

test("the processor surfaces empty headers for a job published without them", async () => {
  const env = EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders" });
  const job = fakeJob(env, {});
  let seen: HeaderCarrier | null = null;
  await processor({ [URN]: (_e, _j, headers) => { seen = headers; } })(job);
  assert.deepEqual(seen, {});
});
