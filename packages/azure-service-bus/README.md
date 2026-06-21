# @babelqueue/azure-service-bus

Azure Service Bus adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope
**publisher** and a URN-routed **consumer** over `@azure/service-bus`, on
`@babelqueue/core`. Implements
[§4 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#azure-service-bus),
so an ASB-based Node service speaks the same wire contract as the .NET, Java, Python and Go
SDKs.

## Install

```bash
npm i @babelqueue/azure-service-bus @azure/service-bus
```

`@azure/service-bus` is an optional peer — **you provide the sender/receiver** (a
`ServiceBusSender` / `ServiceBusReceiver` satisfies the adapter structurally).

## Use

```ts
import { ServiceBusClient } from "@azure/service-bus";
import { AsbPublisher, AsbConsumer } from "@babelqueue/azure-service-bus";

const client = new ServiceBusClient(connectionString); // or (namespace, credential)

// produce
const id = await new AsbPublisher(client.createSender("orders"))
  .publish("urn:babel:orders:created", { order_id: 1042 });

// consume (PeekLock)
const consumer = new AsbConsumer(
  client.createReceiver("orders"),
  {
    "urn:babel:orders:created": async (env, message) => {
      console.log(env.data.order_id, env.trace_id, env.attempts);
    },
  },
  { onError: (err) => console.error(err) },
);
await consumer.run();
```

Delayed delivery: `publish(urn, data, { delayMs: 300000 })` → native
`scheduledEnqueueTimeUtc`.

## Contract mapping (§4)

| Envelope | Azure Service Bus |
| :--- | :--- |
| body | `body` (byte-identical across SDKs) |
| `job` (URN) | `subject` |
| `trace_id` | `correlationId` |
| `meta.id` | `messageId` |
| `meta.schema_version` | `applicationProperties["bq-schema-version"]` |
| `meta.lang` | `applicationProperties["bq-source-lang"]` |
| `meta.created_at` | `applicationProperties["bq-created-at"]` (ms) |
| `attempts` | `deliveryCount − 1` (broker-authoritative) |
| reserve / ack / retry | PeekLock → `completeMessage` / `abandonMessage` |

A throwing handler `abandonMessage`s, so the broker redelivers and increments
`deliveryCount` (at-least-once). The poll loop never stops on a bad message — observe via
`onError` / `onUnknownUrn`. The envelope is unchanged (`schema_version` stays `1`); Azure
Service Bus is purely additive.

The sender/receiver are injected, so the unit tests use fakes — no Azure, no broker. Dual
ESM + CJS.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
native `applicationProperties`, beside the contract `bq-*` properties (the contract wins a key collision). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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
