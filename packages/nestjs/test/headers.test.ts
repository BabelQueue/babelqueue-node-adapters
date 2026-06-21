import "reflect-metadata";

import assert from "node:assert/strict";
import { test } from "node:test";

import { decodeMetadata } from "@babelqueue/bullmq";

import { BabelQueuePublisher } from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

// Duck-typed BullMQ queue that captures opts — no Redis.
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

test("publish threads a traceparent through to the BullMQ job's telemetry.metadata", async () => {
  const queue = fakeQueue("orders");
  const publisher = new BabelQueuePublisher(queue as never);

  await publisher.publish(URN, { order_id: 5 }, { headers: { traceparent: TRACEPARENT } });

  const opts = queue.added[0]!.opts as { telemetry?: { metadata?: string } };
  assert.deepEqual(decodeMetadata(opts.telemetry?.metadata), { traceparent: TRACEPARENT });
  // The envelope (job data) is unchanged.
  const env = queue.added[0]!.data as { job: string };
  assert.equal(env.job, URN);
});

test("publish without headers leaves the job options untouched", async () => {
  const queue = fakeQueue("orders");
  await new BabelQueuePublisher(queue as never).publish(URN, { order_id: 5 });
  assert.equal(queue.added[0]!.opts, undefined);
});
