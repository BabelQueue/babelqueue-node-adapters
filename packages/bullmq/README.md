# @babelqueue/bullmq

[![npm](https://img.shields.io/npm/v/@babelqueue/bullmq.svg)](https://www.npmjs.com/package/@babelqueue/bullmq)

> **Polyglot Queues, Simplified.** A [BullMQ](https://docs.bullmq.io) adapter for
> BabelQueue: your BullMQ jobs carry the canonical BabelQueue envelope and are
> routed by URN — the same contract the PHP/Laravel, Python, Go, Java and .NET SDKs
> speak.

```bash
npm install @babelqueue/bullmq bullmq
```

## Produce

```ts
import { Queue } from "bullmq";
import { publish } from "@babelqueue/bullmq";

const queue = new Queue("orders", { connection: { host: "localhost", port: 6379 } });
await publish(queue, "urn:babel:orders:created", { order_id: 1042 });
// job name = the URN; job data = the canonical { job, trace_id, data, meta, attempts } envelope
```

## Consume

```ts
import { Worker } from "bullmq";
import { processor } from "@babelqueue/bullmq";

new Worker(
  "orders",
  processor({
    "urn:babel:orders:created": async (env) => {
      console.log(env.data.order_id, env.trace_id);
    },
  }),
  { connection: { host: "localhost", port: 6379 } },
);
```

`processor` validates each envelope (`EnvelopeCodec.accepts`) and routes by URN. A
non-conformant envelope is **poison** (ADR-0037 decision 3): it fails with BullMQ's
`UnrecoverableError` — **no retry**, straight to BullMQ's failed set, which keeps the job data. An
unmapped URN, an exhausted handler failure and a throwing handler keep BullMQ's own retry and
failed-set behaviour; an unmapped URN throws unless you pass `onUnknownUrn`.

To also keep poison jobs in a BabelQueue dead-letter queue, pass `deadLetterQueue` (opt-in):

```ts
const handler = processor(handlers, {
  deadLetterQueue: true, // a Queue instance, a queue name, or `true` for "<queue>.dlq"
  onError: (error, job) => console.error("DLQ add failed", job.id, error),
});
new Worker("orders", handler, { connection });
// on shutdown: await handler.close(); // closes a Queue the adapter created (string / true)
```

The [ADR-0035](https://babelqueue.com) record (`dead_letter.reason: "poison"`: an object body is spliced
with a `dead_letter` block, anything else wrapped in `raw`) is added to that queue as a job, named like the
poison job, **before** the job fails. A `Queue` instance is used as given; a name or `true` creates a `Queue`
on the worker's connection and prefix (one per queue name, so a processor shared by several Workers sends each queue's records to its own `<queue>.dlq`). If adding the record fails, the failure goes to `onError` and the job
still fails as unrecoverable. The body fed to the record rule is the job data serialised as JSON, and
BullMQ has already parsed what it stored, so a value `JSON.parse` cannot carry (`1e400`, an integer beyond
2^53) is lost before the adapter sees it — a property of BullMQ.

> BullMQ stores jobs in its own Redis structures, so this gives the canonical
> envelope **shape + URN routing + trace propagation** to BullMQ-based Node
> services. For raw cross-language queues, other SDKs read/write plain broker
> queues directly.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
BullMQ's native `telemetry.metadata` job-options slot (so the envelope — `job.data` — is never touched). On consume, the `processor` surfaces a delivered job's headers to the handler's third argument (and `headersOf(job)` reads them back),
so the core's `otel` `wrapHandler` starts the consumer span as a true **child** of the producer span.

```ts
import { trace } from "@opentelemetry/api";
import { publish as tracedPublish } from "@babelqueue/core/otel";
import type { HeaderCarrier } from "@babelqueue/core";

const headers: HeaderCarrier = {};
await tracedPublish(trace.getTracer("orders"), urn, data,
  () => adapterPublish(urn, data, { headers }), { headers });
```

## License

[MIT](../../LICENSE) © Muhammet Şafak · [babelqueue.com](https://babelqueue.com)
