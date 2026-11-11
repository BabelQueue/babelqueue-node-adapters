import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { DeadLetterEvent, DeadLetterObserver, Envelope } from "@babelqueue/core";
import { InvalidEnvelopeKeyError, UnknownUrnError } from "@babelqueue/core";
import { Queue, UnrecoverableError } from "bullmq";
import { decodeMetadata, processor, publish, publishEnvelope } from "../src/index.js";
import { publish as otelPublish } from "@babelqueue/core/otel";

import { producerMessageId, recordingTracer } from "../../../test-support/otel.js";

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
function fakeJob(data: unknown, id = "1", extra: Record<string, unknown> = {}) {
  return { id, data, name: "urn:babel:orders:created", queueName: "orders", attemptsMade: 0, ...extra } as unknown as import("bullmq").Job;
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

test("a poison job fails with UnrecoverableError (no retry), never reaching a handler", async () => {
  let called = 0;
  const run = processor({ "urn:babel:orders:created": () => { called += 1; } });
  for (const data of [{}, null, 5, "text", [], { job: 5 }]) {
    await assert.rejects(() => run(fakeJob(data)), UnrecoverableError, JSON.stringify(data));
  }
  assert.equal(called, 0);
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

/** A duck-typed DLQ `Queue` that records add() calls (or rejects them). */
function fakeDlq(fails = false) {
  const added: Array<{ name: string; data: Record<string, unknown> }> = [];
  return {
    added,
    async add(name: string, data: Record<string, unknown>) {
      if (fails) throw new Error("redis down");
      added.push({ name, data });
      return { id: "d1" };
    },
  };
}

test("deadLetterQueue (a Queue instance): the ADR-0035 poison record is added before the job fails unrecoverably", async () => {
  const dlq = fakeDlq();
  const run = processor({}, { deadLetterQueue: dlq as unknown as Queue });
  await assert.rejects(() => run(fakeJob({ job: 5, big: 1.5 }, "7", { attemptsMade: 2 })), UnrecoverableError);

  assert.equal(dlq.added.length, 1);
  assert.equal(dlq.added[0]!.name, "urn:babel:orders:created");
  const record = dlq.added[0]!.data;
  assert.equal(record["job"], 5); // an object body is spliced: its members stay at the top level
  assert.equal(record["big"], 1.5);
  const deadLetter = record["dead_letter"] as Record<string, unknown>;
  assert.equal(deadLetter["reason"], "poison");
  assert.equal(deadLetter["original_queue"], "orders");
  assert.equal(deadLetter["attempts"], 2);
});

test("deadLetterQueue: a non-object job data is wrapped in raw; an object already carrying dead_letter is wrapped too", async () => {
  const dlq = fakeDlq();
  const run = processor({}, { deadLetterQueue: dlq as unknown as Queue, queue: "billing" });
  await assert.rejects(() => run(fakeJob("just text")), UnrecoverableError);
  await assert.rejects(() => run(fakeJob({ dead_letter: { reason: "x" } })), UnrecoverableError);
  await assert.rejects(() => run(fakeJob(null)), UnrecoverableError);

  assert.equal(dlq.added[0]!.data["raw"], '"just text"');
  assert.equal((dlq.added[0]!.data["dead_letter"] as Record<string, unknown>)["original_queue"], "billing");
  assert.equal(dlq.added[1]!.data["raw"], '{"dead_letter":{"reason":"x"}}');
  assert.equal(dlq.added[2]!.data["raw"], "null");
});

test("deadLetterQueue: a failed add is reported to onError and the job still fails as unrecoverable", async () => {
  const errors: unknown[] = [];
  const run = processor({}, { deadLetterQueue: fakeDlq(true) as unknown as Queue, onError: (e) => void errors.push(e) });
  await assert.rejects(() => run(fakeJob({})), UnrecoverableError);
  assert.equal(errors.length, 1);
  assert.match((errors[0] as Error).message, /redis down/);

  // A throwing report must not replace the unrecoverable failure either.
  const throwing = processor({}, { deadLetterQueue: fakeDlq(true) as unknown as Queue, onError: () => { throw new Error("report broke"); } });
  await assert.rejects(() => throwing(fakeJob({})), UnrecoverableError);
});

test("no deadLetterQueue: a poison job is only failed (nothing is added anywhere)", async () => {
  await assert.rejects(() => processor({})(fakeJob({})), UnrecoverableError);
});

// --- dead-letter observer (ADR-0039 section 5.1) --------------------------------

function recorder(): { events: DeadLetterEvent[]; observer: DeadLetterObserver } {
  const events: DeadLetterEvent[] = [];
  return { events, observer: (event) => { events.push(event); } };
}

const settleTicks = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));
const namedDlq = (fails = false) => ({ ...fakeDlq(fails), name: "orders.dlq" });

test("observer: a poison record added to the DLQ is reported once with queue, dlq and reason poison", async () => {
  const seen = recorder();
  const dlq = namedDlq();
  const run = processor({}, { deadLetterQueue: dlq as unknown as Queue, deadLetterObserver: seen.observer });
  await assert.rejects(() => run(fakeJob({ job: 5 })), UnrecoverableError);
  assert.equal(dlq.added.length, 1);
  assert.deepEqual(seen.events, [{ queue: "orders", deadLetterQueue: "orders.dlq", reason: "poison" }]);

  const named = recorder();
  const configured = processor({}, { deadLetterQueue: namedDlq() as unknown as Queue, queue: "billing", deadLetterObserver: named.observer });
  await assert.rejects(() => configured(fakeJob("text")), UnrecoverableError);
  assert.deepEqual(named.events, [{ queue: "billing", deadLetterQueue: "orders.dlq", reason: "poison" }]);
});

test("observer: not called for a failed add, with no deadLetterQueue, or for a valid job or unknown URN", async () => {
  const seen = recorder();
  const failing = processor({}, { deadLetterQueue: namedDlq(true) as unknown as Queue, deadLetterObserver: seen.observer, onError: () => undefined });
  await assert.rejects(() => failing(fakeJob({})), UnrecoverableError);
  await assert.rejects(() => processor({}, { deadLetterObserver: seen.observer })(fakeJob({})), UnrecoverableError);

  const valid = EnvelopeCodec.make("urn:babel:orders:created", { id: 1 }, { queue: "orders" });
  const dlq = namedDlq();
  const ok = processor({ "urn:babel:orders:created": () => undefined }, { deadLetterQueue: dlq as unknown as Queue, deadLetterObserver: seen.observer });
  await ok(fakeJob(valid));
  await assert.rejects(() => processor({}, { deadLetterQueue: dlq as unknown as Queue, deadLetterObserver: seen.observer })(fakeJob(valid)), UnknownUrnError);
  assert.equal(dlq.added.length, 0);
  assert.deepEqual(seen.events, []);
});

test("observer: a throwing observer does not change the outcome: the job still fails as unrecoverable", async (t) => {
  const warn = t.mock.method(console, "warn", () => {});
  const dlq = namedDlq();
  const run = processor({}, { deadLetterQueue: dlq as unknown as Queue, deadLetterObserver: () => { throw new Error("observer boom"); } });
  await assert.rejects(() => run(fakeJob({})), UnrecoverableError);
  assert.equal(dlq.added.length, 1);
  assert.equal(warn.mock.callCount(), 1);
});

test("observer: a rejecting observer does not change the outcome", async (t) => {
  const warn = t.mock.method(console, "warn", () => {});
  const errors: unknown[] = [];
  const dlq = namedDlq();
  const run = processor({}, {
    deadLetterQueue: dlq as unknown as Queue,
    onError: (e) => void errors.push(e),
    deadLetterObserver: () => Promise.reject(new Error("async boom")),
  });
  await assert.rejects(() => run(fakeJob({})), UnrecoverableError);
  await settleTicks();
  assert.equal(dlq.added.length, 1);
  assert.equal(errors.length, 0, "an observer failure is not an add failure");
  assert.equal(warn.mock.callCount(), 1);
});

// Needs a live Redis (BullMQ creates the Queue it adds the record to); skipped without BABELQUEUE_REDIS_URL, like the
// live tests of @babelqueue/redis. Not run by the default `npm test` on a machine without Redis.
const REDIS_URL = process.env["BABELQUEUE_REDIS_URL"];

test(
  "deadLetterQueue: true / a name create a Queue on the worker's connection and add the record (default <queue>.dlq)",
  { skip: REDIS_URL === undefined ? "set BABELQUEUE_REDIS_URL to run against a live Redis" : false },
  async () => {
    const url = new URL(REDIS_URL as string);
    const connection = { host: url.hostname, port: Number(url.port || 6379), maxRetriesPerRequest: null };
    const unique = `bq-it-${Math.random().toString(36).slice(2, 10)}`;
    const job = fakeJob({ job: 5 }, "1", { queueName: unique, queue: { opts: { connection } } });

    for (const [target, expected] of [[true, `${unique}.dlq`], [`${unique}-named`, `${unique}-named`]] as const) {
      const run = processor({}, { deadLetterQueue: target });
      const probe = new Queue(expected, { connection });
      try {
        await assert.rejects(() => run(job), UnrecoverableError);
        const jobs = await probe.getJobs(["waiting"]);
        assert.equal(jobs.length, 1, expected);
        const data = jobs[0]!.data as Record<string, unknown>;
        assert.equal(data["job"], 5);
        assert.equal((data["dead_letter"] as Record<string, unknown>)["reason"], "poison");
        assert.equal((data["dead_letter"] as Record<string, unknown>)["original_queue"], unique);
      } finally {
        await probe.obliterate({ force: true });
        await probe.close();
        await run.close();
      }
    }
  },
);

test(
  "deadLetterQueue: true on a processor shared by two Workers sends each queue's poison records to its own <queue>.dlq",
  { skip: REDIS_URL === undefined ? "set BABELQUEUE_REDIS_URL to run against a live Redis" : false },
  async () => {
    const url = new URL(REDIS_URL as string);
    const connection = { host: url.hostname, port: Number(url.port || 6379), maxRetriesPerRequest: null };
    const unique = `bq-it-${Math.random().toString(36).slice(2, 10)}`;
    const shared = processor({}, { deadLetterQueue: true });
    const probes = [`${unique}-a.dlq`, `${unique}-b.dlq`].map((name) => new Queue(name, { connection }));
    try {
      for (const queueName of [`${unique}-a`, `${unique}-b`]) {
        await assert.rejects(() => shared(fakeJob({}, "1", { queueName, queue: { opts: { connection } } })), UnrecoverableError);
      }
      for (const probe of probes) assert.equal((await probe.getJobs(["waiting"])).length, 1, probe.name);
    } finally {
      for (const probe of probes) {
        await probe.obliterate({ force: true });
        await probe.close();
      }
      await shared.close();
    }
  },
);

// --- publishEnvelope / otel seam (ADR-0038 messaging.message.id == wire meta.id) ----------

test("publishEnvelope sends the given envelope unchanged: id, trace_id and extensions kept (GR-1/GR-4)", async () => {
  const built = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 7 }, { queue: "orders", traceId: "trace-ext", meta: { tenant: "acme" } });
  const queue = fakeQueue("orders");
  const id = await publishEnvelope(queue as never, built);
  const wire = EnvelopeCodec.decode(JSON.stringify(queue.added[0]!.data)) as Envelope;
  assert.deepEqual(wire, built);
  assert.equal(wire.meta.id, built.meta.id);
  assert.equal(wire.trace_id, "trace-ext");
  assert.equal(wire.meta.schema_version, 1);
  assert.equal(id, built.meta.id);
});

test("otel publish: the producer span's messaging.message.id equals the wire meta.id and traceparent rides out of band", async () => {
  const { tracer, exporter } = recordingTracer();
  const queue = fakeQueue("orders");
  const headers: Record<string, string> = {};
  const id = await otelPublish(
    tracer,
    "urn:babel:orders:created",
    { order_id: 7 },
    (env) => publishEnvelope(queue as never, env, { headers }),
    { queue: "orders", headers },
  );
  const wire = EnvelopeCodec.decode(JSON.stringify(queue.added[0]!.data)) as Envelope;
  assert.equal(producerMessageId(exporter), wire.meta.id);
  assert.equal(id, wire.meta.id);
  assert.equal(wire.trace_id.length > 0, true);
  assert.match(headers["traceparent"] ?? "", /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);
  const metadata = (queue.added[0]!.opts as { telemetry?: { metadata?: string } }).telemetry?.metadata;
  assert.equal(decodeMetadata(metadata)["traceparent"], headers["traceparent"]);
});

test("publishEnvelope rejects an envelope with a forbidden (§10) key before it reaches the queue, like the other adapters", async () => {
  const queue = fakeQueue("orders");
  const built = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 7 }, { queue: "orders" });
  await assert.rejects(publishEnvelope(queue as never, { ...built, timestamp: 1 } as unknown as Envelope), InvalidEnvelopeKeyError);
  assert.equal(queue.added.length, 0);
});
