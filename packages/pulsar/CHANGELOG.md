# Changelog

All notable changes to `@babelqueue/pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` as additional native Pulsar **message properties**
  beside the contract `bq-*` properties — the contract properties win a key collision
  (merge-not-clobber). `publish({ headers })` injects them; the consumer reads `getProperties()`
  back and surfaces them to the handler's third argument, so the core's `otel` extract links the
  consumer span as a true child of the producer span. New `headersOf` export; `toPulsarMessage`
  takes an optional headers carrier. A header-less publish stays byte-identical. Bumped
  `@babelqueue/core` to `^1.4.0`.
- `PulsarConsumerOptions.onRoutingMismatch(propertyUrn, bodyUrn, envelope)` — optional, separate
  hook (default no-op) called when the `bq-job` property and the body URN differ. Its errors and
  rejections are swallowed (`Promise.resolve(result).catch`); it never affects acknowledgement and is not routed
  through `onError`. It is called only for an accepted envelope, after `accepts()` and the `attempts` reconcile.

### Fixed
- **Consumer version-gates on `bq-schema-version` (broker-bindings §5.7 step 3).** Before the body
  is decoded the consumer reads the `bq-schema-version` property. A missing or blank value changes
  nothing; "Blank" is the same set in every SDK: the empty string or only ASCII whitespace (space, `\t`, `\n`, `\v`, `\f`, `\r`); anything else (NBSP, U+001C-U+001F, U+0085, U+FEFF, U+3000, ...) is not blank and is rejected unless it is exactly the supported version. (`String.prototype.trim()` is not used: it also trims NBSP, U+FEFF, U+2028, U+3000, ...) a value other than the core's supported `SCHEMA_VERSION` (`"2"`, `"x"`, `"01"`, ...)
  rejects the message undecoded through the existing non-conformant-envelope path: `onError` (an
  error naming the version, with an empty envelope) and a `negativeAcknowledge`. No wire change.
- **Consumer now follows broker-bindings §5.2/§5.7.** It routes on the `bq-job` property (falling
  back to the body URN when the property is absent or blank) and takes `attempts` from the
  authoritative `bq-attempts` property (falling back to the body when it is absent or not
  `^[0-9]+$` within int32), then applies `max(.., redeliveryCount)`. Previously it routed on the
  body URN and ignored `bq-attempts`. The handler still receives the body envelope, and a body
  without `job` is still rejected. `attempts` is written to the envelope only when the body holds an
  integer value and it really changes, so a body with a missing or non-integer `attempts` is still
  rejected by `accepts()`. No version bump; envelope unchanged (`schema_version: 1`).

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
