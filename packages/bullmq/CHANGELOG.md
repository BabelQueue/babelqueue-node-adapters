# Changelog

All notable changes to `@babelqueue/bullmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.0] - 2026-10-26

### Changed
- **Behaviour change: a poison job is no longer retried (ADR-0037 decision 3).** A job whose data does not decode
  into a valid envelope used to throw a `BabelQueueError`, so BullMQ retried it as an ordinary failure up to the
  job's `attempts`. It now fails with BullMQ's `UnrecoverableError`: no retry, straight to the failed set (which
  keeps the job data). Code that caught `BabelQueueError` from the processor for this case must now handle
  `UnrecoverableError`. Unmapped URNs, exhausted handler failures and handler errors are unchanged.
- `@babelqueue/core` dependency range raised from `^1.4.0` to `^1.13.0` (`buildPoisonRecord`). `bullmq` is now
  imported at runtime (`Queue`, `UnrecoverableError`); it was already a required peer dependency.

### Added
- `processor` option **`deadLetterQueue?: Queue | string | true`** (opt-in): the ADR-0035 poison record
  (`reason: "poison"`) is also added to it as a job before the job fails. A `Queue` is used as given, a string
  creates a `Queue` of that name on the worker's connection, `true` one named `<queue>.dlq`. Also the `queue`
  and `onError` options, and `close()` on the returned processor (closes a `Queue` the adapter created).
- Conformance v1.8.0 vendored; the `poison_dlq` section runs (15 of its 43 bodies are delivered as BullMQ job
  data through the processor; the others, which cannot reach the adapter as job data, run through the same core
  record builder).

## [1.1.0] - 2026-06-21

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
