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

## License

[MIT](../../LICENSE) © Muhammet Şafak · [babelqueue.com](https://babelqueue.com)
