# Changelog

All notable changes to `@babelqueue/bullmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` in BullMQ's native context-propagation slot,
  `JobsOptions.telemetry.metadata` (serialized to JSON; stored compactly as `tm` in Redis), so the
  canonical envelope (`job.data`) stays byte-identical (GR-1). `publish(queue, urn, data, { headers })`
  writes it (without clobbering an explicit `jobsOptions.telemetry.metadata`); the `processor`
  surfaces the carried headers to the handler's third argument and `headersOf(job)` reads them back,
  so the core's `otel` extract links the consumer span as a true child of the producer span. New
  `headersOf` / `encodeMetadata` / `decodeMetadata` exports. A header-less publish leaves the job
  options unchanged. Bumped `@babelqueue/core` to `^1.4.0`.

## [1.0.1] - 2026-06-07

### Changed
- Version-aligned republish in lockstep with `@babelqueue/nestjs 1.0.1` (which
  corrects its dependency on this package). No API or behavior change; the wire
  envelope is unchanged (`schema_version: 1`).

## [1.0.0] - 2026-06-07

**1.0.0 — the public API is now SemVer-stable** (breaking changes require a MAJOR).
Now requires `@babelqueue/core ^1.0.0`; the wire envelope is unchanged
(`schema_version: 1`).

## [0.1.0] - 2026-06-06

### Added
- `publish(queue, urn, data, opts?)` — adds a BullMQ job whose name is the URN and
  whose data is the canonical BabelQueue envelope; returns `meta.id`.
- `processor(handlers, opts?)` — a BullMQ processor that validates each envelope
  (`EnvelopeCodec.accepts`) and routes it by URN; `onUnknownUrn` hook.
- Dual ESM + CommonJS with bundled types. Built on `@babelqueue/core`; `bullmq` is
  a peer dependency.
