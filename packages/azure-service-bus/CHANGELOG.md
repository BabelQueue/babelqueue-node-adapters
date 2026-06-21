# Changelog

All notable changes to `@babelqueue/azure-service-bus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` as additional native **`applicationProperties`**
  beside the contract `bq-*` properties — the contract properties win a key collision
  (merge-not-clobber). `publish({ headers })` injects them; the consumer reads the inbound
  `applicationProperties` back and surfaces them to the handler's third argument, so the core's
  `otel` extract links the consumer span as a true child of the producer span. New `headersOf`
  export; `toServiceBusMessage` takes an optional headers carrier. A header-less publish stays
  byte-identical. Bumped `@babelqueue/core` to `^1.4.0`.

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
