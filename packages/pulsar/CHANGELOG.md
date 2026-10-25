# Changelog

All notable changes to `@babelqueue/pulsar` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Changed
- **Behaviour change: redelivery is bounded (ADR-0036 decisions 1 and 2).** `PulsarConsumer` used to
  `negativeAcknowledge` a failing handler, an unmapped URN (no `onUnknownUrn`) and a non-conformant body
  without limit, so such a message looped forever. It now takes `maxAttempts` (default **3**, the total
  number of deliveries, the first included; `1` = no retry; below 1 or non-integer throws
  `BabelQueueError`). Counted attempt = `bq-attempts` (body `attempts` as the fallback) floored by
  `getRedeliveryCount()`, plus one for the current delivery; while it is below `maxAttempts` the message is
  negatively acknowledged, otherwise it is dead-lettered or dropped. With the defaults a message that keeps
  failing is now dropped at its **third delivery** (set `maxAttempts` higher to keep retrying, or pass a
  `deadLetterProducer` to keep the message).
- A body that does not decode into an envelope (not JSON, not an object, an object with neither a valid
  `job` nor a valid `urn`, or any other body the codec does not accept as a conformant envelope) is **poison**: it never reaches a handler or `onUnknownUrn`. With a
  `deadLetterProducer` it is written to the DLQ at once, without retry; without one it takes the bounded
  retry, then a drop. It used to be negatively acknowledged forever.
- `@babelqueue/core` dependency range raised from `^1.4.0` to `^1.13.0` (`buildPoisonRecord`).
- Conformance v1.7.0 vendored; the `pulsar_exhaustion` section runs (the 10 rows that set an
  `on_unknown_urn` option belong to SDKs with that option and are reported as skipped).

### Fixed
- **Bounded retry on `Exclusive` / `Failover` subscriptions.** A negative acknowledgement does not advance
  `getRedeliveryCount()` on those subscription types, so the nack-driven retry never reached `maxAttempts`.
  A retry can now re-publish the message to the consumer's own topic with `bq-attempts` = counted (body
  `attempts` in sync, out-of-band headers carried, a poison body byte for byte) and acknowledge the original,
  the Go model, so the bound holds on every subscription type. New options: `client` (the consumer builds the
  retry producer itself, lazily on the first retry, on `topic`, the topic you subscribed to),
  `retryProducer` (your own producer on that topic; wins over `client`), `topic` and `subscriptionType`.
  A failed re-publish or producer creation leaves the original negatively acknowledged and warns through
  `onWarn`. The retry and the DLQ message keep the route the delivery arrived on: a non-blank incoming `bq-job`
  property is carried over even when the body names another URN, so every delivery reaches the same handler
  (the Go runtime's `publishKeepingRoute`).
- **Behaviour change: the constructor fails closed on a subscription it cannot bound.** On `Exclusive` and
  `Failover` a negative acknowledgement does not advance the redelivery count, so a retry without a
  re-publish would loop forever. `subscriptionType` (new option) is therefore checked at construction: with
  `maxAttempts > 1` (the default is 3) and neither a `retryProducer` nor a `client` together with its `topic` option,
  `new PulsarConsumer(...)` throws `BabelQueueError` for `Exclusive` and `Failover` — and **also when
  `subscriptionType` is not declared**, which counts as `Exclusive` (pulsar-client's own default).
  **Who is affected:** everyone who builds `new PulsarConsumer(sub, handlers, options)` with the default
  `maxAttempts` and passes none of `client`, `retryProducer` or `subscriptionType`. **Fix, either one:** pass
  `client` **together with `topic`**, the topic you subscribed to (the pulsar-client `Client`; the consumer builds its
  retry producer itself; a `client` without `topic` throws too), or a `retryProducer`, or declare
  `subscriptionType: "Shared"` (or `"Key_Shared"`) when the subscription really is one — the nack retry is
  bounded there. `maxAttempts: 1` (no retry) needs neither.
- New exported types `PulsarClientLike` and `PulsarSubscriptionType`; `PulsarConsumerClient.getTopic?()` (optional: pulsar-client's `Consumer` has no `getTopic()`, so with `client` the `topic` option is required; a `client` without a topic on an `Exclusive` / `Failover` / undeclared subscription throws a `BabelQueueError` that asks for `topic`).

### Added
- `PulsarConsumerOptions.maxAttempts`, `deadLetterProducer`, `queue` and `onWarn`; the exported
  `DEFAULT_MAX_ATTEMPTS`. Dead-lettering is on exactly when `deadLetterProducer` (a producer on
  `<queue>.dlq`) is set. The dead-letter record is the canonical envelope with a `dead_letter` block
  (`reason` `failed` / `unknown_urn`, `dead_letter.attempts` and `attempts` = the counted value), or for a
  poison body the ADR-0035 record (`reason: "poison"`, original bytes kept; invalid UTF-8 as base64). The
  original is acknowledged only after the record was sent; a failed send leaves it negatively acknowledged.
  A dropped message is acknowledged and reported through `onWarn` (default `console.warn`).
- The native Pulsar `DeadLetterPolicy` is not used by the SDK (it amends ADR-0014).

### Known gap
- A message with an unsupported `bq-schema-version` still takes the version gate (`onError` and a
  `negativeAcknowledge`), which this release does not bound.

## [1.2.0] - 2026-10-10

### Added
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
  rejected by `accepts()`. Envelope unchanged (`schema_version: 1`).

## [1.1.0] - 2026-06-21

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` as additional native Pulsar **message properties**
  beside the contract `bq-*` properties — the contract properties win a key collision
  (merge-not-clobber). `publish({ headers })` injects them; the consumer reads `getProperties()`
  back and surfaces them to the handler's third argument, so the core's `otel` extract links the
  consumer span as a true child of the producer span. New `headersOf` export; `toPulsarMessage`
  takes an optional headers carrier. A header-less publish stays byte-identical. Bumped
  `@babelqueue/core` to `^1.4.0`.

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
