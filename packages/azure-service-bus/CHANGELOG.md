# Changelog

All notable changes to `@babelqueue/azure-service-bus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. `AsbPublisher` (canonical-envelope send with the §4 native projection —
  `subject` = URN, `correlationId` = `trace_id`, `messageId` = `meta.id`, plus
  `bq-schema-version`/`bq-source-lang`/`bq-created-at` application properties; native
  `scheduledEnqueueTimeUtc` for delays) and `AsbConsumer` (PeekLock receive → URN-routed
  handlers → `completeMessage`; a throwing handler `abandonMessage`s for at-least-once
  redelivery; `attempts` reconciled to the broker-authoritative `deliveryCount − 1`). Built
  on `@babelqueue/core`; `@azure/service-bus` is an optional peer (the sender/receiver are
  injected, so the unit tests use fakes — no Azure, no broker). Dual ESM+CJS. The envelope
  is unchanged (`schema_version: 1`); Azure Service Bus is purely additive.
