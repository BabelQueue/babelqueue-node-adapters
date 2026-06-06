import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import { BabelQueueError, UnknownUrnError } from "@babelqueue/core";
import { processor, publish } from "../src/index.js";

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

// A duck-typed BullMQ Job carrying envelope data.
function fakeJob(data: unknown, id = "1") {
  return { id, data } as unknown as import("bullmq").Job;
}

test("publish adds a canonical-envelope job named by URN", async () => {
  const queue = fakeQueue("orders");
  const id = await publish(queue as never, "urn:babel:orders:created", { order_id: 1042 });

  assert.equal(queue.added.length, 1);
  const job = queue.added[0];
  assert.equal(job.name, "urn:babel:orders:created");

  const env = job.data as ReturnType<typeof EnvelopeCodec.make>;
  assert.equal(env.job, "urn:babel:orders:created");
  assert.equal(env.meta.queue, "orders");
  assert.equal(env.meta.lang, "node");
  assert.deepEqual(env.data, { order_id: 1042 });
  assert.equal(id, env.meta.id);
});

test("processor routes a valid envelope to its URN handler", async () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 7 }, { queue: "orders" });
  let seen: unknown = null;

  const run = processor({
    "urn:babel:orders:created": (e) => {
      seen = e.data;
    },
  });

  await run(fakeJob(env));
  assert.deepEqual(seen, { order_id: 7 });
});

test("processor rejects a non-conformant envelope", async () => {
  const run = processor({});
  await assert.rejects(() => run(fakeJob({})), BabelQueueError);
});

test("processor throws on an unmapped URN, or calls onUnknownUrn", async () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 }, { queue: "orders" });

  await assert.rejects(() => processor({})(fakeJob(env)), UnknownUrnError);

  let captured = "";
  const run = processor(
    {},
    { onUnknownUrn: (e) => { captured = EnvelopeCodec.urn(e); } },
  );
  await run(fakeJob(env));
  assert.equal(captured, "urn:babel:orders:created");
});
