# Changelog

All notable changes to `@babelqueue/sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.2.0] - 2026-10-03

### Changed
- **A failed handler is now always released via `ChangeMessageVisibility` (contract §3.5).** The
  consumer calls `ChangeMessageVisibility(ReceiptHandle, VisibilityTimeout = releaseDelay)` instead of
  leaving the message to wait out the whole visibility timeout; the message is never deleted. The new
  `releaseDelay` option sets the backoff (seconds, or a function of the reconciled `attempts` for an
  exponential backoff; clamped to `0…43200`). **Default `0`: the message is visible again
  immediately** — the same default as the Java and .NET SDKs.
- **Poison-message risk — configure a `RedrivePolicy`.** With the `0 s` default a handler that
  always fails is retried back-to-back until the queue's native `RedrivePolicy` (`maxReceiveCount`)
  moves the message to its DLQ. Without a `RedrivePolicy` such a message loops forever. Attach a
  `RedrivePolicy` to every consumed queue (e.g. `maxReceiveCount` 3–5, target `<queue>.dlq`), and/or
  pass a `releaseDelay` backoff such as `(attempts) => Math.min(2 ** attempts * 5, 900)`.
- A client without `changeMessageVisibility` keeps the 1.1.0 behaviour for the default release (the
  message is left for visibility-timeout redelivery).

### Fixed
- A failing release or dead-letter send (e.g. `MessageNotInflight` after a handler outlived the
  reservation, a missing `sqs:ChangeMessageVisibility` IAM grant, a throwing `releaseDelay`) is reported
  to `onError` and the message is left for visibility-timeout redelivery; it never rejects `poll()` or
  stops `run()`.
- A `DeleteMessage` failure after a successful handler is no longer treated as a handler failure: it
  is reported to `onError` as the new `SqsDeleteError` (broker error in `cause`) and the message is
  **not** released, so an already-processed message is redelivered only after its visibility timeout
  instead of immediately. The same applies to the delete of the `delete` / `dead_letter` unknown-URN
  strategies (matching the Java `SqsDeleteException` and the .NET SDK).
- Under an `unknownUrn` strategy a throwing `onUnknownUrn` hook or a failing `delete` is reported to
  `onError` and never rejects `poll()` or stops `run()`; the strategy still applies after a throwing hook.

### Added
- `unknownUrn` strategy (`fail` | `delete` | `release` | `dead_letter`) mapped to the §3.5 SQS ops:
  `release` → `ChangeMessageVisibility` with `unknownUrnReleaseDelay` (default 0 s); `dead_letter` →
  annotated envelope `SendMessage` to the opt-in `deadLetterQueueUrl`, then `DeleteMessage`
  (degrades to `delete` without a DLQ). A `.fifo` DLQ (`<queue>.dlq.fifo`) is sent with
  `MessageGroupId` (source queue name) and `MessageDeduplicationId` (`meta.id`). `onUnknownUrn` stays a
  notification hook when a strategy is set. Without `unknownUrn` the unknown-URN path is unchanged.
- `releaseDelay`, `unknownUrnReleaseDelay`, `deadLetterQueueUrl` options and the
  `MAX_VISIBILITY_TIMEOUT` export.
- Optional `SqsApi.changeMessageVisibility` (the AWS `SQS` client provides it); required — checked at
  construction — when `releaseDelay` or the `release` strategy is set explicitly.

## [1.1.0] - 2026-06-21

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` as String **`MessageAttributes`** beside the
  contract `bq-*` attributes (where `bq-trace-id` already rides) — the contract attributes win a key
  collision and the merged set is bounded by SQS's 10-attribute limit (contract attributes seeded
  first). `publish({ headers })` injects them; the consumer reads the inbound `MessageAttributes`
  back and surfaces them to the handler's third argument, so the core's `otel` extract links the
  consumer span as a true child of the producer span. New `mergeAttributes` / `headersOf` exports. A
  header-less publish stays byte-identical. Bumped `@babelqueue/core` to `^1.4.0`.

## [1.0.0] - 2026-06-12

### Added
- Initial release. `SqsPublisher` (canonical-envelope `SendMessage` with the §3
  `MessageAttributes` projection — `bq-job`/`bq-trace-id`/`bq-message-id`/
  `bq-schema-version`/`bq-source-lang`/`bq-created-at`; FIFO group/dedup) and
  `SqsConsumer` (long-poll receive → URN-routed handlers → `DeleteMessage`;
  SQS-native visibility-timeout retry; `attempts` reconciled to
  `ApproximateReceiveCount − 1`, never lowering a runtime-incremented count). Built on
  `@babelqueue/core`; `@aws-sdk/client-sqs` is an optional peer (the client is injected,
  so the unit tests use a fake — no AWS, no broker). Dual ESM+CJS. The envelope is
  unchanged (`schema_version: 1`); SQS is purely additive.
