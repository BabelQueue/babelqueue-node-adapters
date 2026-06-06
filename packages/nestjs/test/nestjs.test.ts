import "reflect-metadata";

import assert from "node:assert/strict";
import { test } from "node:test";

import {
  BABELQUEUE_QUEUE,
  BabelQueueModule,
  BabelQueuePublisher,
} from "../src/index.js";

// Duck-typed BullMQ queue — no Redis.
function fakeQueue(name = "orders") {
  const added: Array<{ name: string; data: unknown }> = [];
  return {
    name,
    added,
    async add(jobName: string, data: unknown) {
      added.push({ name: jobName, data });
      return { id: "1" };
    },
  };
}

test("BabelQueuePublisher publishes a canonical envelope via the queue", async () => {
  const queue = fakeQueue("orders");
  const publisher = new BabelQueuePublisher(queue as never);

  const id = await publisher.publish("urn:babel:orders:created", { order_id: 5 });

  assert.equal(queue.added.length, 1);
  assert.equal(queue.added[0].name, "urn:babel:orders:created");
  const env = queue.added[0].data as { job: string; meta: { id: string; queue: string } };
  assert.equal(env.job, "urn:babel:orders:created");
  assert.equal(env.meta.queue, "orders");
  assert.equal(id, env.meta.id);
});

test("forRoot returns a well-formed DynamicModule", () => {
  const dynamicModule = BabelQueueModule.forRoot({ queue: "orders", connection: { host: "localhost", port: 6379 } });

  assert.equal(dynamicModule.module, BabelQueueModule);
  const providers = dynamicModule.providers ?? [];
  assert.ok(providers.some((p) => (p as { provide?: unknown }).provide === BabelQueuePublisher));
  assert.ok(providers.some((p) => (p as { provide?: unknown }).provide === BABELQUEUE_QUEUE));
  assert.ok((dynamicModule.exports ?? []).includes(BabelQueuePublisher));
});
