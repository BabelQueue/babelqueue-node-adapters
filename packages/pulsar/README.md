# @babelqueue/pulsar

Apache Pulsar adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope
**publisher** and a URN-routed **consumer** over `pulsar-client`, on `@babelqueue/core`.
Implements
[§5 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-pulsar),
so a Pulsar-based Node service speaks the same wire contract as the .NET, Java, Python and Go
SDKs.

## Install

```bash
npm i @babelqueue/pulsar pulsar-client
```

`pulsar-client` is an optional peer — **you provide the producer/consumer** (a `Producer` /
`Consumer` from `pulsar-client` satisfies the adapter structurally).

## Use

```ts
import Pulsar from "pulsar-client";
import { PulsarPublisher, PulsarConsumer } from "@babelqueue/pulsar";

const client = new Pulsar.Client({ serviceUrl: "pulsar://localhost:6650" });

// produce
const producer = await client.createProducer({ topic: "orders" });
const id = await new PulsarPublisher(producer)
  .publish("urn:babel:orders:created", { order_id: 1042 });

// consume (Shared subscription)
const sub = await client.subscribe({
  topic: "orders",
  subscription: "babelqueue",
  subscriptionType: "Shared",
});
const consumer = new PulsarConsumer(
  sub,
  {
    "urn:babel:orders:created": async (env, message) => {
      console.log(env.data.order_id, env.trace_id, env.attempts);
    },
  },
  { onError: (err) => console.error(err) },
);
await consumer.run();
```

Delayed delivery: `publish(urn, data, { delayMs: 300000 })` → native `deliverAfter`. The
consumer routes on the `bq-job` property (§5.2); when it is absent or blank it falls back to the
body's URN. The body is always decoded and the handler always receives the body's envelope.

## Contract mapping (§5)

| Envelope | Apache Pulsar |
| :--- | :--- |
| body | message payload (byte-identical across SDKs) |
| `job` (URN) | property `bq-job` (consumer routes on this) |
| `trace_id` | property `bq-trace-id` |
| `meta.id` | property `bq-message-id` |
| `meta.schema_version` | property `bq-schema-version` |
| `meta.lang` | property `bq-source-lang` |
| `meta.created_at` | publish time (mirror; body authoritative) |
| `attempts` | property `bq-attempts` (authoritative; body is the fallback), floored by `getRedeliveryCount()` |
| reserve / ack / retry | `acknowledge` / `negativeAcknowledge` |

Pulsar properties are string→string, so `bq-attempts` carries the contract `attempts` and is
**authoritative**. It is valid only when it is plain decimal digits (`^[0-9]+$`, at most
2147483647) — a sign, whitespace, an empty string or an out-of-range value is invalid and the
consumer falls back to the body's `attempts`. The result is reconciled to
`max(.., getRedeliveryCount())`: `getRedeliveryCount()` is 0-based (0 on first delivery) so it
maps directly with **no −1** (the body is only rewritten when it holds an integer `attempts` that really changes; a missing or non-integer `attempts` is still rejected) — so a republish-driven retry and a native redelivery both
converge on the same number.

If the `bq-job` property and the body's URN differ, routing follows the property and the
optional `onRoutingMismatch(propertyUrn, bodyUrn, envelope)` hook is called (default: no-op).
It is separate from `onError`, its failures (including a rejected promise) are swallowed, and
it never changes the ack flow. The hook runs only for an accepted envelope — after `accepts()` and
the `attempts` reconcile — so it sees the same envelope the handler gets. A body without a `job` is still rejected by the codec even when
`bq-job` is set. A throwing handler `negativeAcknowledge`s, so
the broker redelivers (at-least-once). The poll loop never stops on a bad message — observe
via `onError` / `onUnknownUrn`. The envelope is unchanged (`schema_version` stays `1`);
Pulsar is purely additive.

The producer/consumer are injected, so the unit tests use fakes — no Pulsar, no broker. Dual
ESM + CJS.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
native Pulsar message properties, beside the contract `bq-*` properties (the contract wins a key collision). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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
