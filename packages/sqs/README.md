# @babelqueue/sqs

Amazon SQS adapter for [BabelQueue](https://babelqueue.com) — "Polyglot Queues,
Simplified."

A canonical-envelope **publisher** and a URN-routed **consumer** over Amazon SQS, so
an SQS-based Node service speaks the same wire contract as the PHP/Laravel, Python,
Go, Java and .NET SDKs. Implements [§3 of the broker-bindings contract](https://babelqueue.com):
the canonical envelope is the message body, projected onto native SQS
`MessageAttributes`.

## Install

```bash
npm i @babelqueue/sqs @aws-sdk/client-sqs
```

`@aws-sdk/client-sqs` is an (optional) peer — you provide the SQS client.

## Use

```ts
import { SQS } from "@aws-sdk/client-sqs";
import { SqsPublisher, SqsConsumer } from "@babelqueue/sqs";

const sqs = new SQS({ region: "eu-central-1" });
const url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

// produce
await new SqsPublisher(sqs, url).publish("urn:babel:orders:created", { order_id: 1042 });

// consume
const consumer = new SqsConsumer(sqs, url, {
  "urn:babel:orders:created": async (env) => {
    // env.data, env.trace_id, env.attempts ...
  },
});
await consumer.run(); // long-polls until the AbortSignal you pass aborts
```

FIFO queues: `new SqsPublisher(sqs, url, { fifo: true })` (the URL must end in `.fifo`).
For LocalStack/ElasticMQ, point the `SQS` client's `endpoint` there.

## Contract mapping (§3)

| Envelope | SQS |
| :--- | :--- |
| body | `MessageBody` (byte-identical across SDKs) |
| `job` (URN) | `MessageAttributes.bq-job` |
| `trace_id` | `MessageAttributes.bq-trace-id` |
| `meta.id` | `MessageAttributes.bq-message-id` |
| `meta.schema_version` | `MessageAttributes.bq-schema-version` (Number) |
| `meta.lang` | `MessageAttributes.bq-source-lang` |
| `meta.created_at` | `MessageAttributes.bq-created-at` (Number, ms) |
| `attempts` | reconciled to `ApproximateReceiveCount − 1` on receive |
| reserve / ack | visibility timeout → `DeleteMessage` |

Retry is **SQS-native**: a throwing handler leaves the message undeleted, so SQS
redelivers it after the visibility timeout (at-least-once). The loop never stops on a
bad message — use `onError` / `onUnknownUrn` to observe. The envelope is unchanged
(`schema_version` stays `1`); SQS is purely additive.

## Test

The unit tests inject a fake SQS client (`SqsApi`), so they run without
`@aws-sdk/client-sqs` and without a broker:

```bash
npm test
```

## OpenTelemetry tracing (ADR-0028)

Cross-hop **span** linkage rides on the out-of-band `HeaderCarrier` from
[`@babelqueue/core@^1.4.0`](https://www.npmjs.com/package/@babelqueue/core). Pass the carrier produced by
`@babelqueue/core/otel`'s `publish` to this adapter's `publish({ headers })`; it is carried on
String `MessageAttributes`, beside the contract `bq-*` attributes (the contract wins a key collision; bounded by SQS's 10-attribute cap). On consume, the consumer surfaces a delivered message's headers to the handler's third argument (and a `headersOf(...)` extractor reads them back),
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
