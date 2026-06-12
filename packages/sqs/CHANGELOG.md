# Changelog

All notable changes to `@babelqueue/sqs` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

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
