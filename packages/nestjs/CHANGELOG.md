# Changelog

All notable changes to `@babelqueue/nestjs` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** `BabelQueuePublisher.publish`
  accepts an optional out-of-band `headers` carrier (e.g. a W3C `traceparent`) and threads it
  through to the underlying `@babelqueue/bullmq` job's native `telemetry.metadata` slot — the
  canonical envelope is never touched (GR-1). Consume with the re-exported `processor`, whose handler
  receives the carried headers as its third argument, so the core's `otel` extract links the
  consumer span as a true child of the producer span. Bumped `@babelqueue/core` to `^1.4.0`.

## [1.0.1] - 2026-06-07

### Fixed
- Depend on `@babelqueue/bullmq ^1.0.0` (was `^0.1.0`). The `1.0.0` release bumped
  the `@babelqueue/core` constraint but left the sibling `@babelqueue/bullmq`
  constraint at `^0.1.0`, so a fresh `npm install @babelqueue/nestjs` resolved the
  old `0.1.0` BullMQ adapter (and core `0.1.0`) instead of the `1.x` line. No API
  or behavior change; the wire envelope is unchanged (`schema_version: 1`).

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
