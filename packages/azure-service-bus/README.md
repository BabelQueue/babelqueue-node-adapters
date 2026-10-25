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

**Version gate (§4.7).** Before the body is decoded the consumer reads the `bq-schema-version`
application property. A missing or blank value (empty or only ASCII whitespace) changes nothing;
the string `"1"` and the integral number `1` decode; any other value or type (`"2"`, `"x"`, `"01"`,
`2`, `1.5` — strings are never trimmed) rejects the message **undecoded**: it reaches `onError`
(with an empty envelope) and is settled on its **first delivery** (ADR-0037 decision 1) — never decoded,
never given to a handler or `onUnknownUrn`. With a `deadLetterSender` (a sender on the cross-language
`<queue>.dlq` queue) the body is sent there exactly as received (no `dead_letter` block) and the message is
**completed only after the send succeeded**; a failed send is reported and abandons it, so the write is tried
again. Without a `deadLetterSender` it is left to the **native** mechanism: **dead-lettered explicitly**
(`deadLetterMessage`, reason `unsupported schema version`, into `$DeadLetterQueue`) — never abandoned into a
loop. The receiver must provide `deadLetterMessage` (a `ServiceBusReceiver` does); one without it degrades to
`abandonMessage` (`MaxDeliveryCount` then dead-letters it). This consumer has no unknown-URN strategy option
and an `onUnknownUrn` hook never applies to a version-gated delivery: it behaves as `fail`.

**Poison (ADR-0037 decision 2, ADR-0035).** A body that does not decode into a valid envelope is never given
to a handler. With a `deadLetterSender` the **ADR-0035 record** (`reason: "poison"`, built with the core's
`buildPoisonRecord`: an object body spliced byte for byte, anything else wrapped in `raw`, invalid UTF-8 as
base64) is sent to `<queue>.dlq` and the message completed only after the send; a failed send abandons it.
Without a `deadLetterSender` it is reported and abandoned, so `MaxDeliveryCount` moves it to the native
dead-letter sub-queue. `dead_letter.original_queue` is the `queue` option, else the receiver's `entityPath`,
else the sender's entity path without `.dlq`.

**Bodies and `skipParsingBodyAsJson`.** `@azure/service-bus` parses a data-section body that holds valid JSON into an
object unless the receiver is created with `skipParsingBodyAsJson: true`. The consumer still routes such a message
(it re-serialises the parsed value to decode the envelope), but the original bytes are gone, so it **never writes a DLQ
record from a parsed body**: a version-gated or poison delivery whose body was parsed is reported and abandoned (the
native mechanism keeps it), never completed. Create the receiver with `skipParsingBodyAsJson: true` to get exact
bytes and the full `deadLetterSender` behaviour. DLQ records are always sent as bytes (the SDK would otherwise
JSON-encode a string into a quoted literal).

**Upgrading from a release before the publisher fix.** Upgrade every consumer first, then the publishers: an old
consumer (`<= 1.2.0`) cannot read the new wire body and would abandon every message to the native dead-letter queue.
A new consumer also reads the quoted JSON string literal that old publishers left in the queue (a body that is a
quoted JSON string whose content is a valid envelope is opened once, with either receiver setting); any other
quoted string is poison. Note that with the default receiver the SDK's `JSON.parse` already rounds integers above
2^53 and `1e400`-style numbers before the consumer sees them; `skipParsingBodyAsJson: true` avoids that.

**Publisher wire format.** `AsbPublisher` / `toServiceBusMessage` send the envelope as bytes (a `Buffer`), so the data
section holds the envelope JSON itself, byte-identical to the other SDKs (GR-5).

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
