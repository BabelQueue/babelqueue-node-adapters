# @babelqueue/artemis

Apache ActiveMQ Artemis adapter for [BabelQueue](https://babelqueue.com) — a canonical-envelope
**publisher** and a URN-routed **AMQP 1.0** consumer over [rhea](https://github.com/amqp/rhea),
so an Artemis-based Node service speaks the same wire contract (envelope shape, URN identity,
trace propagation) as the Java, .NET, Python and Go SDKs. Implements
[§7 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#apache-activemq-artemis).

Artemis speaks AMQP 1.0 (not RabbitMQ's 0-9-1) and gives the binding native primitives —
per-message settlement, scheduled delivery, a delivery counter and a dead-letter address — so
this adapter maps onto them (the envelope stays `schema_version: 1`):

- the envelope JSON is the message **body**; the contract fields are mirrored onto the AMQP a JMS
  peer reads — `correlation-id` = `trace_id`, `creation-time` = `meta.created_at`, the
  `x-opt-jms-type` annotation = URN (so a Java/JMS or AMQP consumer routes without decoding the
  body) — plus the `bq-` application properties;
- consume settles per message: **`accept` after success**; a throwing handler **`release`s** the
  message so the broker redelivers it (incrementing the AMQP `delivery-count`);
- **`attempts = max(body, delivery-count)`** — the AMQP delivery-count is 0-based (0 on first
  delivery), so it maps directly with no −1 (the Java JMS binding reads the 1-based
  `JMSXDeliveryCount` and subtracts 1, arriving at the same 0-based `attempts`);
- delay uses Artemis's **native** AMQP scheduled delivery (`x-opt-delivery-time`); terminal
  failures go to an opt-in `<queue>.dlq` with a `dead_letter` block.

## Install

```bash
npm install @babelqueue/artemis rhea
```

`rhea` is an optional peer — you provide the sender/receiver; a rhea sender/receiver satisfies
the adapter structurally.

## Produce

```ts
import { Container } from "rhea";
import { ArtemisPublisher } from "@babelqueue/artemis";

const container = new Container();
const connection = container.connect({ host: "localhost", port: 5672 });
const sender = connection.open_sender("orders");

const publisher = ArtemisPublisher.create(sender, "orders");
const id = await publisher.publish("urn:babel:orders:created", { order_id: 1042 });
```

`publish(urn, data, options?)` returns the message `meta.id`; `options` adds a `traceId` and a
`delayMs` (native scheduled delivery).

## Consume

```ts
import { Container } from "rhea";
import { ArtemisConsumer, type BabelHandlers } from "@babelqueue/artemis";

const receiver = connection.open_receiver({ source: "orders", autoaccept: false, credit_window: 10 });
const dlqSender = connection.open_sender("orders.dlq");

const handlers: BabelHandlers = {
  "urn:babel:orders:created": (envelope, message) => {
    // envelope.data, envelope.trace_id, envelope.attempts ...
  },
};

const consumer = new ArtemisConsumer(handlers, {
  deadLetterSender: dlqSender, // enables the cross-language <queue>.dlq
  maxTries: 3,
  onError: (err, envelope, context) => console.error(err),
});

consumer.listen(receiver); // wires receiver.on("message") → accept / release / dead-letter
```

A successful handler `accept`s the message. A throwing handler `release`s it (the broker
redelivers and bumps `delivery-count`); once `maxTries` is reached the envelope goes to
`<queue>.dlq` with a `dead_letter` block. The consumer routes on the `x-opt-jms-type` annotation
(falling back to the body URN), so it never decodes a message it cannot handle. Unknown-URN
strategy is one of `fail` / `delete` / `release` / `dead_letter`.

**Version gate (§7.7).** Before the body is decoded the consumer reads the `bq_schema_version`
application property (underscores, ADR-0017). A missing or blank value (empty or only ASCII
whitespace) follows the normal decode path; any other value is compared as-is (never trimmed) to
`"1"`. A message that does not match is rejected undecoded and settled on its **first delivery**
(ADR-0037 decision 1): never decoded, never given to a handler, never retried. Under
`unknownUrn: "delete"`, or without a `deadLetterSender`, it is `accept`ed and dropped (reported to
`onError`). Otherwise its raw body, byte for byte and without a `dead_letter` block, is forwarded to the
`deadLetterSender`, then `accept`ed — but only **after the broker
has accepted the DLQ delivery** (rhea `accepted` event on the DLQ sender). Only the explicitly
configured `unknownUrn` strategy is read (`release` and `fail` behave alike here: a version-gated
delivery is never released). If the DLQ delivery is
rejected/released/modified, the link closes, `send` throws, or `deadLetterTimeoutMs` (default 30000)
elapses, the failure is reported to `onError` and the source message gets a plain `release()` instead —
after `deadLetterRetryDelayMs` (default 1000) so a persistently failing DLQ is not a hot redelivery
loop. It is never accepted and never reaches a handler. The release deliberately omits
`delivery_failed`: that would bump Artemis's delivery counter and, once `max-delivery-attempts` is hit,
route the message to the broker's DLA (or delete it when no DLA is configured). While waiting, the
delivery stays unsettled and holds one receiver credit. The DLQ confirmation listens on the DLQ
sender only for the duration of each gate send and re-dispatches events that are not its own to the
sender's session.

**Poison (ADR-0037 decision 2, ADR-0035).** A body that does not decode into a valid envelope (not JSON,
not valid UTF-8, a JSON value that is not an envelope, an unsupported `meta.schema_version`, an object with
no valid identity) is never given to a handler and never retried. With a `deadLetterSender` the **ADR-0035
record** is written to `<queue>.dlq` — built with the core's `buildPoisonRecord`: an object body is spliced
byte for byte with a `dead_letter` block (`reason: "poison"`), anything else is wrapped as
`{"raw": …, "dead_letter": …}` (invalid UTF-8 as base64 with `"raw_encoding": "base64"`) — and the message is
`accept`ed only after the broker accepted that delivery (a failed write releases it, like the gate path).
`dead_letter.original_queue` is the `queue` option, else the `address` of the receiver the message arrived on (`context.receiver`), else of the receiver passed to `listen()` (set `queue` when one consumer serves several receivers). A version-gated body is forwarded to the DLQ as received (a Buffer or data-section body is not decoded to text).
Under `unknownUrn: "delete"`, or without a `deadLetterSender`, the message is accepted and dropped
(reported to `onError`). The raw forward of earlier releases is gone.

> `autoaccept: false` is required — the consumer owns the disposition. `handle(context)` is also
> exposed directly for testing or a custom event wiring.

## Contract mapping (§7)

| Envelope | Apache ActiveMQ Artemis (AMQP 1.0) |
| :--- | :--- |
| body | message body (byte-identical across SDKs) |
| `job` (URN) | `x-opt-jms-type` annotation → JMSType (consumer routes on this) |
| `trace_id` | `correlation-id` → JMSCorrelationID |
| `meta.created_at` | `creation-time` → JMSTimestamp (Unix ms) |
| `meta.schema_version` | application property `bq-schema-version` (`"1"`) |
| `meta.lang` | application property `bq-source-lang` |
| `attempts` | `max(body, delivery-count)` (AMQP counter is 0-based) |
| reserve / ack | message event → process → **`accept`** |
| retry / delay | `release` redelivery · native `x-opt-delivery-time` |
| dead-letter | `<queue>.dlq` + `dead_letter` block (alongside the native DLA) |

The `bq-` application-property values are strings (integers as decimal, e.g. `"1"`); `bq-app-id`
is `"babelqueue"`. The envelope is unchanged (`schema_version` stays `1`); Artemis is purely
additive.

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
AMQP `application_properties`, beside the contract `bq_` properties (the contract wins a key collision; `traceparent`/`tracestate` are hyphen-free, so JMS-legal). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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
