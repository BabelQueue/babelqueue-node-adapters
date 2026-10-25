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

**Version gate (§5.7 step 3).** Before the body is decoded the consumer reads
`bq-schema-version`. Missing or blank: nothing changes ("blank" is the same set in every SDK: the empty string or only ASCII whitespace — space, `\t`, `\n`, `\v`, `\f`, `\r`; NBSP, U+0085, U+FEFF, U+3000 and the like are not blank). Any value other than the core's supported
`SCHEMA_VERSION` (`"2"`, `"x"`, `"01"`, ...) rejects the message **undecoded** through the same
path as a non-conformant envelope — `onError` (an error naming the version, with an empty
envelope) and a `negativeAcknowledge`; the handler, `onUnknownUrn` and `onRoutingMismatch` are
not called.

If the `bq-job` property and the body's URN differ, routing follows the property and the
optional `onRoutingMismatch(propertyUrn, bodyUrn, envelope)` hook is called (default: no-op).
It is separate from `onError`, its failures (including a rejected promise) are swallowed, and
it never changes the ack flow. The hook runs only for an accepted envelope — after `accepts()` and
the `attempts` reconcile — so it sees the same envelope the handler gets. A body without a `job` is still rejected by the codec even when
`bq-job` is set. A throwing handler `negativeAcknowledge`s, so
the broker redelivers (at-least-once) — up to `maxAttempts` deliveries (see below). The poll loop
never stops on a bad message — observe via `onError` / `onUnknownUrn`. The envelope is unchanged
(`schema_version` stays `1`); Pulsar is purely additive.

### Max attempts, dead-letter and poison (ADR-0036)

Redelivery is bounded, so every message terminates. `maxAttempts` (default **3**, an integer of at
least 1) is the total number of deliveries a message gets, the first included; `1` means no retry.
The SDK counts `counted = reconciled attempts + 1` (the reconciled value above plus the current
delivery) and decides for a retryable outcome — a handler that throws, an unmapped URN without an
`onUnknownUrn` hook, or a poison body with dead-lettering off:

- `counted < maxAttempts` → **retry**: `negativeAcknowledge` (the native redelivery count advances).
- otherwise → **dead-letter** when a `deadLetterProducer` is given, else **drop** (acknowledge, and
  a warning through `onWarn`, default `console.warn`).

```ts
const dlq = await client.createProducer({ topic: "orders.dlq" });
const consumer = new PulsarConsumer(sub, handlers, {
  maxAttempts: 3,          // default
  deadLetterProducer: dlq, // dead-lettering is on exactly when this is set
  queue: "orders",         // dead_letter.original_queue of a poison record (default: the DLQ topic without ".dlq")
});
```

The dead-letter record is the canonical envelope with the `dead_letter` block (`reason` `failed` or
`unknown_urn`; `dead_letter.attempts` and the record's `attempts` are the counted value, `3` with the
default). The original is acknowledged **only after** the record was sent; a failed send leaves it
negatively acknowledged, so it is redelivered and the write is tried again. The SDK does not use
Pulsar's native `DeadLetterPolicy`; adding one yourself creates a second, uncoordinated limit.

**Poison.** A body that does not decode into an envelope — not JSON, not an object, or an object with
neither a valid `job` nor a valid `urn`, or any other body the codec does not accept as a conformant envelope (for example a missing `trace_id` or a non-integer `attempts`) — never reaches a handler or `onUnknownUrn`. With
`deadLetterProducer` it is written to the dead-letter topic at once, without retry, in the ADR-0035
shape (a strict JSON object keeps every original byte and gains a `dead_letter` block with
`reason: "poison"`; anything else is wrapped as `{"raw", "raw_encoding"?, "dead_letter"}`). Without
one it takes the bounded retry above and is then dropped.

**Behaviour change (MINOR).** Redelivery used to be unbounded; with the defaults a message that keeps
failing is now dropped (or dead-lettered) at its third delivery. Set `maxAttempts` higher to keep
more retries. A message with an unsupported `bq-schema-version` still takes the version gate above
(`onError` and a `negativeAcknowledge`).

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
