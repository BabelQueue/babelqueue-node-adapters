# @babelqueue/rabbitmq

RabbitMQ adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope **publisher** and
a URN-routed **consumer** over RabbitMQ (AMQP 0-9-1, amqplib), so a RabbitMQ-based Node service
speaks the same wire contract as the PHP, Python, Go, Java and .NET SDKs. Implements
[§2 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#rabbitmq-amqp-0-9-1).

The envelope JSON is the message body; the contract fields are projected onto native AMQP 0-9-1
properties so a consumer routes without decoding the body: `type` = URN, `correlation_id` =
`trace_id`, `message_id` = `meta.id`, `app_id` = `babelqueue`, plus the native-typed
`x-schema-version` / `x-source-lang` / `x-attempts` headers (AMQP field-tables carry typed values —
integers stay integers). Consume is `basic.get` + manual ack (at-least-once).

## Install

```bash
npm install @babelqueue/rabbitmq amqplib
```

`amqplib` is an optional peer — you provide the channel; an amqplib `Channel` satisfies the
adapter structurally.

## Produce

```ts
import amqp from "amqplib";
import { RabbitMQPublisher } from "@babelqueue/rabbitmq";

const conn = await amqp.connect("amqp://guest:guest@localhost:5672/");
const channel = await conn.createChannel();
await channel.assertQueue("orders", { durable: true });

const id = await RabbitMQPublisher.create(channel, "orders").publish("urn:babel:orders:created", { order_id: 1042 });
```

`publish(urn, data, { traceId? })` returns the message `meta.id`. Messages are persistent
(`delivery_mode = 2`).

## Consume

```ts
import { RabbitMQConsumer, type BabelHandlers } from "@babelqueue/rabbitmq";

const handlers: BabelHandlers = {
  "urn:babel:orders:created": (envelope, message) => {
    // envelope.data, envelope.trace_id, envelope.attempts ...
  },
};

const consumer = new RabbitMQConsumer(channel, "orders", handlers, {
  maxTries: 3,
  onError: (err) => console.error(err),
});

await consumer.run(() => true); // basic.get → process → ack, until you stop it
```

A successful handler `ack`s the message. A throwing handler republishes the envelope with
`attempts + 1` (at-least-once) up to `maxTries`, then dead-letters to `<queue>.dlq` with a
`dead_letter` block. The consumer routes on `properties.type` (falling back to the body URN).
Unknown-URN strategy is one of `fail` / `delete` / `release` / `dead_letter`. `poll()` and
`handle(message)` are exposed for testing.

## Contract mapping (§2)

| Envelope | RabbitMQ (AMQP 0-9-1) |
| :--- | :--- |
| body | message body (the canonical envelope JSON) |
| `job` (URN) | `properties.type` (consumer routes on this) |
| `trace_id` | `properties.correlation_id` |
| `meta.id` | `properties.message_id` |
| — | `properties.app_id` = `babelqueue`, `content_type` = `application/json`, persistent |
| `meta.schema_version` | header `x-schema-version` (number) |
| `meta.lang` | header `x-source-lang` |
| `attempts` | header `x-attempts` (number; the body owns the count) |
| reserve / ack | `basic.get` → process → **`basic.ack`** |
| retry | republish with `attempts + 1` |
| dead-letter | `<queue>.dlq` + `dead_letter` block |

The envelope is unchanged (`schema_version` stays `1`); the amqplib channel is replaced with a
fake in the unit suite — no RabbitMQ, no network.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
the native AMQP message header table (`properties.headers`), beside the contract `x-*` headers (the contract wins a key collision). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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
