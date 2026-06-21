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

`processor` validates each envelope (`EnvelopeCodec.accepts`) and routes by URN; a
non-conformant envelope throws (BullMQ retries/fails per its options), and an
unmapped URN throws unless you pass `onUnknownUrn`.

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
