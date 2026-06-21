# @babelqueue/redis

Redis adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope **publisher** and a
URN-routed **consumer** over the §1 reliable-queue pattern, so a Redis-based Node service speaks
the same wire contract as the PHP, Python and Go SDKs. Implements
[§1 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#redis).

Redis lists carry **no native metadata** — the list element **is** the canonical envelope JSON,
byte-for-byte, with **no wrapping** (unlike `@babelqueue/bullmq`, which uses BullMQ's own job
layout). Produce is `RPUSH`; consume reserves the head into a `<queue>:processing` list
(`BRPOPLPUSH`, so an in-flight message survives a crash), routes by URN, and `LREM`s it on success.

> This is a Node-owned reliable queue. Full parity with Laravel's reserved-sorted-set reservation
> on a *shared* Redis queue is a separate task — for a mixed PHP+Node fleet, prefer a queue this
> consumer owns end-to-end.

## Install

```bash
npm install @babelqueue/redis ioredis
```

`ioredis` is an optional peer — you provide the client; an `ioredis` instance satisfies the
adapter structurally.

## Produce

```ts
import Redis from "ioredis";
import { RedisPublisher } from "@babelqueue/redis";

const client = new Redis("redis://localhost:6379/0");
const id = await RedisPublisher.create(client, "orders").publish("urn:babel:orders:created", { order_id: 1042 });
```

`publish(urn, data, { traceId? })` returns the message `meta.id`.

## Consume

```ts
import { RedisConsumer, type BabelHandlers } from "@babelqueue/redis";

const handlers: BabelHandlers = {
  "urn:babel:orders:created": (envelope, raw) => {
    // envelope.data, envelope.trace_id, envelope.attempts ...
  },
};

const consumer = new RedisConsumer(client, "orders", handlers, {
  maxTries: 3,            // requeue with attempts+1, then <queue>.dlq
  onError: (err) => console.error(err),
});

await consumer.run(() => true); // reserve → process → LREM, until you stop it
```

A successful handler `LREM`s the element from `<queue>:processing`. A throwing handler requeues
the envelope with `attempts + 1` (at-least-once) up to `maxTries`, then dead-letters to
`<queue>.dlq` with a `dead_letter` block. Unknown-URN strategy is one of
`fail` / `delete` / `release` / `dead_letter`. `poll()` and `handle(raw)` are exposed for testing.

## Contract mapping (§1)

| Envelope | Redis |
| :--- | :--- |
| body | the list element — the canonical envelope JSON, **verbatim** (no wrapping) |
| produce | `RPUSH <queue> <envelope>` |
| reserve | `BRPOPLPUSH <queue> <queue>:processing` (crash-safe) |
| ack | `LREM <queue>:processing 1 <element>` |
| retry | requeue with `attempts + 1` (the body owns the count) |
| dead-letter | `RPUSH <queue>.dlq` + `dead_letter` block |

The envelope is unchanged (`schema_version` stays `1`). The `ioredis` client is replaced with a
fake in the unit suite — no Redis, no network.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
a transport-owned `__bq_frame` JSON frame (Redis lists have no native metadata channel; the `LREM` ack handle *is* the stored value). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
so the core's `otel` `wrapHandler` starts the consumer span as a true **child** of the producer span.

A bare (pre-0028 / cross-version) value consumes with empty headers, and a header-less publish stays byte-identical.

```ts
import { trace } from "@opentelemetry/api";
import { publish as tracedPublish } from "@babelqueue/core/otel";
import type { HeaderCarrier } from "@babelqueue/core";

const headers: HeaderCarrier = {};
await tracedPublish(trace.getTracer("orders"), urn, data,
  () => adapterPublish(urn, data, { headers }), { headers });
```

## License

MIT
