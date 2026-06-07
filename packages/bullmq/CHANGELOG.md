# Changelog

All notable changes to `@babelqueue/bullmq` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
