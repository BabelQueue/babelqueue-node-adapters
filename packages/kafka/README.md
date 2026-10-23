# @babelqueue/kafka

Apache Kafka adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope
**publisher** and a URN-routed, **process-then-commit** consumer over
[KafkaJS](https://kafka.js.org), on `@babelqueue/core`. Implements
[§6 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-kafka),
so a Kafka-based Node service speaks the same wire contract as the .NET, Java, Python and Go
SDKs.

Kafka has **no native** per-message ack, delayed delivery, dead-letter queue, or delivery
counter — this adapter absorbs all four in the binding layer (the envelope stays
`schema_version: 1`): the record **value** is the envelope JSON, the contract fields are
mirrored onto `bq-` headers (route on `bq-job` without decoding the body), the record
timestamp mirrors `meta.created_at`, **`bq-attempts` is the authoritative retry counter**,
consume is process-then-commit (manual commit), retry/delay use SDK-owned tiered retry topics
`<topic>.retry.<n>`, and terminal failures go to an opt-in `<topic>.dlq`.

## Install

```bash
npm i @babelqueue/kafka kafkajs
```

`kafkajs` is an optional peer — **you provide the producer/consumer** (a KafkaJS `Producer` /
`Consumer` satisfies the adapter structurally).

## Use

```ts
import { Kafka } from "kafkajs";
import { KafkaPublisher, KafkaConsumer, RetryTopics } from "@babelqueue/kafka";

const kafka = new Kafka({ brokers: ["localhost:9092"] });

// produce
const producer = kafka.producer();
await producer.connect();
const id = await KafkaPublisher.create(producer, "orders")
  .publish("urn:babel:orders:created", { order_id: 1042 });

// consume (manual commit, process-then-commit)
const consumer = kafka.consumer({ groupId: "orders-workers" });
await consumer.connect();
await consumer.subscribe({ topic: "orders" });

const retry = new RetryTopics("orders", [5_000, 60_000]); // orders.retry.1/.2 + orders.dlq
const babel = new KafkaConsumer(
  consumer,
  {
    "urn:babel:orders:created": async (env, message) => {
      console.log(env.data.order_id, env.trace_id, env.attempts);
    },
  },
  { producer, retryTopics: retry, maxTries: 3, onError: (err) => console.error(err) },
);
await babel.run(); // poll → process → commit (autoCommit: false)
```

Delayed delivery: `publish(urn, data, { delayMs: 300_000 })` routes to the matching retry
tier (requires `KafkaPublisher.withRetryTopics`); on a plain publisher a delay throws. A
throwing handler republishes to the next `<topic>.retry.<n>` tier with `bq-attempts + 1`, then
commits; once `maxTries` is reached it goes to `<topic>.dlq` with a `dead_letter` block.

**Re-inject consumer (§6.4).** A failed record is parked on `<topic>.retry.<n>`; something must
move it back to the work topic once its tier has elapsed. `RetryReinjector` does that: subscribe a
consumer (its own group, `autoCommit: false`) to `reinjector.topics` and `run()` it. The due time is
`record.timestamp + tier_ms` (the tier comes from the topic, never from `bq-delay`), clamped to
`[now, now + tier_ms]`; a record that is not yet due is `seek`ed back and its partition `pause`d
until it is, so the poll loop keeps heartbeating. A due record is written to `bq-original-topic`
byte for byte (value and key; no decode, no `attempts` change) minus `bq-delay` /
`bq-original-topic`, `traceparent` kept, and its offset is committed only after the broker
acknowledged the write. `DEFAULT_RETRY_TIERS_MS` is `[5s, 30s, 5m, 30m]`. Create the retry topics
with `message.timestamp.type=LogAppendTime` (SHOULD) and a retention longer than the largest tier.

```ts
import { RetryReinjector, RetryTopics } from "@babelqueue/kafka";

const reinjector = new RetryReinjector(consumer, { producer, retryTopics: retry });
await consumer.subscribe({ topics: reinjector.topics });
await reinjector.run();
```

**Version gate (§6.7).** Before the record value is decoded the consumer reads the
`bq-schema-version` header. A missing or blank value (empty or only ASCII whitespace) follows the
normal decode path; any other value is compared as-is (never trimmed) to `"1"`. A record that does
not match is rejected undecoded as poison: reported to `onError`, forwarded raw to `<topic>.dlq`
(when configured) and its offset committed.

**Poison records (§6.5).** A body that is not a decodable BabelQueue envelope - the bodies the Go
SDK's typed `Decode` rejects, except that member names are matched case-sensitively as the contract spells them (invalid JSON, a non-object root other than `null`, a wrongly typed member such as
`meta.created_at: "2026"`, `meta.id: 5`, a fractional or string `attempts`, or `meta.schema_version: "1"`) - is reported to `onError`, written to
`<topic>.dlq` wrapped like the Go SDK does - a body that is one JSON object keeps its keys, anything else
becomes `{"raw": "<body>"}`, plus a `dead_letter` block with `reason: "poison"` - with the record key
preserved byte for byte, and its offset committed. A well-typed envelope that is merely non-conformant (for
example an empty `trace_id`, or a non-string `urn` alias) is not poison: like Go, it is routed by its URN.

## Contract mapping (§6)

| Envelope | Apache Kafka |
| :--- | :--- |
| body | record `value` (byte-identical across SDKs) |
| `job` (URN) | header `bq-job` (consumer routes on this) |
| `trace_id` | header `bq-trace-id` |
| `meta.id` | header `bq-message-id` |
| `meta.schema_version` | header `bq-schema-version` (`"1"`) |
| `meta.lang` | header `bq-source-lang` |
| `meta.created_at` | record `timestamp` (Unix ms) |
| `attempts` | header `bq-attempts` (**authoritative**; body is the fallback) |
| reserve / ack | poll → process → **commit offset** (manual) |
| retry / delay | republish to `<topic>.retry.<n>` (`bq-attempts + 1`) |
| dead-letter | `<topic>.dlq` + `dead_letter` block |

All header values are UTF-8 strings (integers as decimal strings, e.g. `"1"`). The
producer/consumer are injected, so the unit tests use fakes — no Kafka, no broker. Dual
ESM + CJS.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
native Kafka record headers, beside the contract `bq-*` headers (the contract wins a key collision). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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

MIT
