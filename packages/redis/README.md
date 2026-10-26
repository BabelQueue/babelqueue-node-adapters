# @babelqueue/redis

Redis adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope **publisher** and a
URN-routed **consumer** over the §1 reliable-queue pattern, so a Redis-based Node service speaks
the same wire contract as the PHP, Python and Go SDKs. Implements
[§1 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#redis).

Redis lists carry **no native metadata** — the list element **is** the canonical envelope JSON,
byte-for-byte, with **no wrapping** (unlike `@babelqueue/bullmq`, which uses BullMQ's own job
layout). Produce is `RPUSH`; consume reserves the head into a `<queue>:processing` list
(`LMOVE`, so an in-flight message survives a crash), routes by URN, and acknowledges it on success.

Two layouts are available ([ADR-0033](https://babelqueue.com/docs/spec/1.x/broker-bindings#redis)):

- **`legacy`** (default) — the Node-owned `:processing` reliable queue, now with deadline recovery.
- **`zset`** — the Laravel-compatible `queues:<q>` / `:delayed` / `:reserved` / `:notify` layout, so a
  Node worker can share a queue with a Laravel producer or worker, with native delays and
  `retry_after` recovery.

## Install

```bash
npm install @babelqueue/redis ioredis
```

`ioredis` is an optional peer — you provide the client; an `ioredis` instance satisfies the
adapter structurally. Consuming also needs `evalBuffer` (Lua eval returning raw `Buffer`s, so a
non-UTF-8 body can be detected as poison); without it `reserve()` throws. A non-`ioredis` client
must provide it.

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

A successful handler acknowledges the reservation. A throwing handler gives the envelope back with
`attempts + 1` (at-least-once) up to `maxTries`, then dead-letters to `<queue>.dlq` with a
`dead_letter` block. Unknown-URN strategy is one of `fail` / `delete` / `release` / `dead_letter`; the default `fail` reports the error and retries with `attempts + 1` up to `maxTries`, then dead-letters (`reason: "unknown_urn"`).
`poll()` and `handle(reservation)` are exposed for testing.

`blockTimeout` (seconds, default 5) is how long one `poll()` waits: a sleep-poll (100 ms, doubling,
capped at 1 s) in the legacy layout, `BLPOP` on `:notify` (at most 1 s per slice, migrating between
slices) in the zset layout. `0` or less means a single non-blocking attempt.

## Zset layout, delays and recovery

```ts
const publisher = RedisPublisher.create(client, "orders", { layout: "zset" });
await publisher.publish("urn:babel:orders:created", { order_id: 1 }, { delay: 30 }); // ZADD :delayed

const consumer = new RedisConsumer(client, "orders", handlers, {
  layout: "zset",
  retryAfter: 90,           // seconds before an unacked reservation is recovered; null disables it
  migrationBatchSize: -1,   // max items moved per migrate (-1 = unlimited)
});
```

- An immediate publish is `RPUSH queues:<q>` plus one `RPUSH queues:<q>:notify 1`; a delayed one is
  `ZADD queues:<q>:delayed floor(now)+ceil(delay)` (no notify token).
- Every pop first migrates due `:delayed` items, then expired `:reserved` items (skipped when
  `retryAfter` is `null`), to the end of the ready list. A recovery counts as an attempt.
- Reserve is one Lua script: it increments `attempts` in the stored reservation, while the handler
  still sees the body you published. A body that is not JSON, or whose `attempts` is not a number
  (poison), is dead-lettered to `<queue>.dlq` and removed from `:reserved`.
- A release is one Lua script (`ZREM :reserved` + `ZADD :delayed`) that raises `attempts` by one and
  preserves every other key. Integers above 2^53 lose precision in that rewrite (`JSON.parse`).
- **`deadLetterQueue: null`** disables the dead-letter of an **exhausted** message and of an **unknown URN**
  only (they are dropped; `unknownUrn: "dead_letter"` degrades to `delete`). A **poison** body (not JSON, not a
  valid envelope, not valid UTF-8, a non-numeric `attempts`) is still dead-lettered as the ADR-0035 record in
  every configuration (ADR-0037 decision 5): to the configured `deadLetterQueue` name, and with `null` to the
  default `<queue>.dlq`.
- Cluster hash tags are the operator's responsibility (pass them in the queue name).
- The handler timeout SHOULD be shorter than `retryAfter`, otherwise a slow handler is redelivered
  while still running. A negative `retryAfter` is rejected with a `BabelQueueError`.
- **No headers in the zset layout.** The Laravel wire has no place for the `__bq_frame` wrapper, so
  `headers` (including `traceparent`) are dropped on publish and the handler's third argument is
  always `{}` (A3). Use the legacy layout when trace-context propagation matters.

## Legacy recovery

Each reservation also writes a member `<32 hex token>:<raw>` into the `<queue>:processing:deadlines`
sorted set (score `floor(now)+retryAfter`; `+inf` when `retryAfter` is `null`). A sweep before every
reserve moves expired entries back to the ready list byte-for-byte (no `attempts` change), so a
crashed consumer's message is not lost. An ack that arrives after the sweep touches nothing and logs
a "reservation expired" warning. Tokens come from `crypto.randomBytes`.

## Contract mapping (§1)

| Envelope | Redis |
| :--- | :--- |
| body | the list element — the canonical envelope JSON, **verbatim** (no wrapping) |
| produce | `RPUSH <queue> <envelope>` |
| reserve | legacy: `LMOVE <queue> <queue>:processing LEFT RIGHT` + deadline; zset: the Laravel reserve script |
| ack | legacy: `ZREM` deadline, then `LREM <queue>:processing 1 <element>`; zset: `ZREM :reserved` |
| retry | give back with `attempts + 1` (the body owns the count) |
| dead-letter | `RPUSH <queue>.dlq` + `dead_letter` block |

The envelope is unchanged (`schema_version` stays `1`). The unit suite uses a fake client; the Lua
scripts and the conformance `redis_layout` cases are exercised against a real Redis
(`BABELQUEUE_REDIS_URL`, default `redis://127.0.0.1:6379`). With `BQ_REQUIRE_REDIS` set an
unreachable Redis fails the run; without it those tests are skipped.

When `BQ_CONFORMANCE_RESULTS` is set, the conformance run writes `results.redis.json` beside the
given path (the package slug is inserted so several packages sharing one value do not overwrite each
other).

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
a transport-owned `__bq_frame` JSON frame in the **legacy** layout (Redis lists have no native metadata channel; the `LREM` ack handle *is* the stored value). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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
