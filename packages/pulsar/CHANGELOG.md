# Changelog

All notable changes to `@babelqueue/pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. `PulsarPublisher` (canonical-envelope send with the §5 property projection
  — `bq-job` = URN, `bq-trace-id` = `trace_id`, `bq-message-id` = `meta.id`, plus
  `bq-schema-version`/`bq-source-lang`/`bq-attempts`; native `deliverAfter` for delays) and
  `PulsarConsumer` (receive → URN-routed handlers → `acknowledge`; a throwing handler
  `negativeAcknowledge`s for at-least-once redelivery; `attempts` reconciled to
  `max(bq-attempts, getRedeliveryCount())` — Pulsar's redelivery count is 0-based so it maps
  directly with no −1, and the `max` keeps a republish-driven retry and a native redelivery
  in agreement). Built on `@babelqueue/core`; `pulsar-client` is an optional peer (the
  producer/consumer are injected, so the unit tests use fakes — no Pulsar, no broker). Dual
  ESM+CJS. The envelope is unchanged (`schema_version: 1`); Apache Pulsar is purely additive.
