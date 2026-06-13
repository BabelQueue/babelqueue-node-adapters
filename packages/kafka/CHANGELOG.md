# Changelog

All notable changes to `@babelqueue/kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache Kafka adapter on `@babelqueue/core` + KafkaJS, implementing §6 of
  the broker-bindings contract. Kafka has no native ack/delay/DLQ/delivery-counter, so the
  binding absorbs all four: `KafkaPublisher` (value = canonical envelope, record timestamp =
  `meta.created_at`, the `bq-` header projection — `bq-job` routes; a delay routes to a
  `<topic>.retry.<n>` tier or throws without one) and `KafkaConsumer` (**process-then-commit**
  manual-commit consume via `eachMessage` + `commitOffsets`; a throwing handler republishes to
  the next retry tier with **`bq-attempts + 1`** then commits; terminal failures go to
  `<topic>.dlq` with the additive `dead_letter` block; the `bq-attempts` header is the
  authoritative counter with the body as fallback; `fail`/`delete`/`release`/`dead_letter`
  unknown-URN strategies; poison records forwarded raw to the DLQ). `RetryTopics` configures the
  tiered retry/delay topology. Built on `@babelqueue/core`; `kafkajs` is an optional peer (the
  producer/consumer are injected, so the unit tests use fakes — no Kafka, no broker). Dual
  ESM+CJS. The envelope is unchanged (`schema_version: 1`); Apache Kafka is purely additive.
