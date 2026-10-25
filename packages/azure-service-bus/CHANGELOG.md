# Changelog

All notable changes to `@babelqueue/azure-service-bus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **`deadLetterSender` option (ADR-0037 decision 2): a BabelQueue `<queue>.dlq` for this consumer** (an ordinary
  Service Bus queue, §4.5; opt-in, default none), and the `queue` option for `dead_letter.original_queue`.

### Fixed
- **Wire behaviour: the publisher no longer double-encodes the envelope (GR-5).** `AsbPublisher` / `toServiceBusMessage`
  put the envelope JSON in `body` as a *string*, and `@azure/service-bus` JSON-encodes every body that is not a
  `Buffer` / `Uint8Array`, so the data section held a quoted JSON string literal (`"{\"job\":...}"`) instead of the envelope
  bytes the .NET / Java / Go / Python SDKs write and read. The body is now a `Buffer` of the envelope's UTF-8 bytes,
  so the data section is the envelope JSON itself. A conformance test checks that the publisher hands the codec's
  output to the SDK as bytes.
  **Upgrade order (ships as a MINOR): upgrade every Node consumer first, then the publishers.** Two migration cases:
  1. *New publisher, old consumer.* A consumer on `<= 1.2.0` read the body with `String(body)`; the new wire body
     reaches a default receiver as a parsed object (`"[object Object]"`), so the old consumer treats every message as
     poison and abandons it until `MaxDeliveryCount` moves it to the native `$DeadLetterQueue`: work stops. Never
     run a new publisher against an old consumer.
  2. *Old messages still in the queue, new consumer.* Messages published before this fix hold a quoted JSON string
     literal. The new consumer **opens such a body once** (migration step, on both the default receiver, which parses
     the literal into its inner string, and `skipParsingBodyAsJson: true`, which delivers the literal's bytes) when
     it is a quoted JSON string whose content is a valid envelope; any other quoted string stays poison, as before.
     The step is meant for draining and can be dropped in a later major.
- A data-section body that `@azure/service-bus` already parsed into an object (the default receiver) used to be
  decoded via `String(object)` (`"[object Object]"`) and treated as poison. It is now decoded from the exact bytes (the raw AMQP
  body; re-serialised only when no raw view exists), so an envelope written by another SDK routes to its handler.
- DLQ writes (`deadLetterSender`) send bytes, never a string the SDK would quote.
- **A default receiver (JSON-parsed body) together with a `deadLetterSender` now writes to `<queue>.dlq` (ADR-0037
  decision 2).** The consumer reads the exact bytes from the raw AMQP message the SDK keeps on the received message
  (`_rawAmqpMessage.body`, rhea's decoded section: `{ typecode: 0x75, content: Buffer }`, an array of Buffers for several
  data sections (concatenated), or, for an amqp-value, the plain string / Buffer rhea gives unwrapped), so a poison body (the ADR-0035
  record) and a version-gated delivery (the raw body) are written byte for byte and the message is completed, instead of
  being abandoned to the native sub-queue until `MaxDeliveryCount`. Envelopes are decoded from those bytes too. A
  shape that is not recognised (amqp-sequence, a non-text amqp-value, no raw message) is still reported and abandoned,
  never completed.

### Changed
- **Behaviour change (only with a `deadLetterSender`): poison bodies and version-gated deliveries go to
  `<queue>.dlq` on their first delivery.** A poison body is sent as the ADR-0035 record (`reason: "poison"`)
  instead of being abandoned until `MaxDeliveryCount`; a version-gated delivery is sent as its raw body (byte
  for byte, no `dead_letter` block) instead of being dead-lettered into the native sub-queue. The source
  message is completed only after the send succeeded; a failed send is reported and abandons it. **Without a
  `deadLetterSender` nothing changes**: poison is abandoned, a version-gated delivery is dead-lettered
  natively. The version gate never reads an `onUnknownUrn` hook.
- `@babelqueue/core` dependency range raised from `^1.4.0` to `^1.13.0` (`buildPoisonRecord`).
- Conformance v1.8.0 vendored; the `poison_dlq` and `version_gate_asb` sections run (the 9 rows that set an
  `on_unknown_urn` option other than `fail` belong to SDKs with that option and are reported as skipped).

## [1.2.0] - 2026-10-10

### Fixed
- **The consumer now gates on `bq-schema-version` before decoding the body (contract §4.7).** A
  message whose `bq-schema-version` application property is present, non-blank and neither the
  string `"1"` nor the integral number `1` (`"2"`, `"x"`, `"01"`, `2`, `1.5`, ...) is rejected
  undecoded: it is reported to `onError` (with an empty envelope) and **dead-lettered explicitly**
  with reason `unsupported schema version` — never abandoned. A missing or blank value (empty or only
  ASCII whitespace) keeps the existing decode path. A receiver without `deadLetterMessage` degrades
  to `abandonMessage` (the `onError` message says so). The envelope and the producer are unchanged.
  The rejected value is echoed into the dead-letter description and the `onError` message truncated
  to its first 64 characters (plus `...`), so an oversized value stays under Service Bus's
  4096-character description limit.

### Added
- **`AsbReceiver.deadLetterMessage?` (new optional public-type member).** A `ServiceBusReceiver`
  provides it; the version gate uses it to dead-letter explicitly. Backward compatible (optional), but
  it extends the exported interface, so release it as a MINOR.

## [1.1.0] - 2026-06-21

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
