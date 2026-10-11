# Changelog

All notable changes to `@babelqueue/kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Fixed

- **A throwing `onError` callback can no longer bypass settlement.** The version-gate, undecodable
  (decode error) and non-conformant-envelope branches reported to `onError` *before* the DLQ write
  and the offset commit, so a callback that threw skipped both and the record redelivered forever.
  Every branch now settles first (DLQ / retry + commit), then reports through a guarded `report()`
  that swallows callback exceptions; the original error is still reported when settlement itself
  fails. Behaviour change: a throwing `onError` is now swallowed instead of propagating out of
  `handle()`. An `async` `onError` whose promise rejects is swallowed too (no unhandled
  rejection). No wire, header or `dead_letter` change.

## [1.1.3] - 2026-10-11

### Fixed

- **The original record key now survives retry republish, unknown-URN release, and the envelope
  DLQ write.** Only the raw (poison) DLQ forward kept the key before; the retry-topic and envelope
  DLQ writes dropped it, so a keyed record lost its key after the first retry. The key is carried
  byte-for-byte (an empty key stays empty); a null/absent key stays null and is never synthesised.
  Partition equality is not guaranteed. The envelope, the `dead_letter` block and the header
  projection are unchanged. No public API change.

## [1.1.2] - 2026-10-11

### Fixed

- A `bq-schema-version` header present with a null value is now treated as absent (§6.7), like
  every other SDK: the record decodes normally. Previously the null was stringified to `"null"`
  and the record was rejected by the version gate. A null header value also no longer crashes
  the raw DLQ forward (`Buffer.from(null)` threw a `TypeError`); it is dropped from the forwarded
  headers.
- The exported `IncomingHeaders` type now admits `null` values (`Buffer | string | (Buffer | string)[]
  | null | undefined`), matching what kafkajs can deliver at runtime. This widens an input type only;
  producer-side types are unchanged.

## [1.1.1] - 2026-10-10

### Fixed
- **The consumer now gates on `bq-schema-version` before decoding the record value (contract §6.7).**
  A record whose `bq-schema-version` header is present, non-blank and not exactly the core's
  supported `"1"` (`"2"`, `"x"`, `"01"`, `" 1"`, ...) is rejected undecoded down the poison path:
  reported to `onError` (with a `null` envelope), the raw value forwarded to `<topic>.dlq` when one is
  configured, and the offset committed. A missing or blank value (empty or only ASCII whitespace)
  keeps the existing decode path. The envelope and the producer are unchanged.
- **Raw DLQ forwarding now keeps the original record metadata.** Every path that forwards a record
  undecoded to `<topic>.dlq` (version-gate rejection, decode error, non-conformant envelope) used to
  send only the value and a fresh timestamp. It now also forwards the original record `key` and all
  headers, aligned with the Java reference. Header values are copied as `Buffer`s, array-valued
  headers stay arrays, and the timestamp is still re-stamped. The commit-after-forward behaviour
  is unchanged.

## [1.1.0] - 2026-06-21

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` as additional Kafka **record headers** beside the
  contract `bq-*` headers — the contract headers win a key collision (merge-not-clobber).
  `publish({ headers })` injects them; the consumer reads the inbound record headers back and
  surfaces them to the handler's third argument, so the core's `otel` extract links the consumer
  span as a true child of the producer span. New `headersOf` export. A header-less publish stays
  byte-identical. Bumped `@babelqueue/core` to `^1.4.0`.

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
