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

## License

MIT
