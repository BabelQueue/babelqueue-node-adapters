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

## License

MIT
