# Changelog

All notable changes to `@babelqueue/nestjs` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable** (breaking changes require a MAJOR).
Now requires `@babelqueue/core ^1.0.0`; the wire envelope is unchanged
(`schema_version: 1`).

## [0.1.0] - 2026-06-06

### Added
- `BabelQueueModule.forRoot({ queue, connection })` — a dynamic module providing an
  injectable `BabelQueuePublisher` (over a BullMQ queue) and a `BABELQUEUE_QUEUE`
  token.
- `BabelQueuePublisher.publish(urn, data, opts?)` — emits the canonical envelope.
- Re-exports `processor` from `@babelqueue/bullmq` for building URN-routed workers.
- Dual ESM + CommonJS with bundled types. `@nestjs/common` and `bullmq` are peer
  dependencies.
