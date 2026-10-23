# Changelog

All notable changes to `@babelqueue/redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **Laravel-compatible `zset` layout (ADR-0033, opt-in via `layout: "zset"`).** Keys
  `queues:<q>`, `:delayed`, `:reserved`, `:notify`; Laravel's reserve Lua (poison-safe, A1),
  atomic release (A2), `retry_after` recovery, and native delayed publish (`publish({ delay })`).
  Migrate runs before every pop (`migrationBatchSize`), blocking pop is `BLPOP :notify`.
- **`retryAfter` for the legacy layout.** Reservations carry a deadline in
  `<queue>:processing:deadlines` (`<32 hex token>:<raw>`, `crypto.randomBytes`); a sweep before every
  reserve returns expired entries to the ready list byte-for-byte. `null` disables recovery; a
  negative value throws `BabelQueueError`.
- Conformance v1.6.0 vendored; `redis`, `redis_layout` and `redis_delay` run against a real Redis.
  Exports `RedisQueue`, `zsetKeys`, `ZSET_KEY_PREFIX`, `LUA`, `newReservationToken`.

### Changed
- **Behaviour change:** the legacy reserve pops from the head (`LMOVE <q> <q>:processing LEFT RIGHT`)
  instead of the tail (`BRPOPLPUSH`), so the order is FIFO and matches the other SDKs.
- **Breaking (TypeScript):** the `RedisClient` interface drops `brpoplpush` and gains `zadd`,
  `blpop` and `eval`; `RedisConsumer.handle` takes a `Reservation` instead of a raw string. An
  `ioredis` instance still satisfies the interface.
- `blockTimeout` of 0 or less is a single non-blocking attempt.
- Zset layout drops headers: no `__bq_frame`, `traceparent` is not carried (A3). The legacy layout
  is unchanged and remains the default.

### Fixed
- **Zset reserve no longer loses a message whose body cannot be re-encoded.** `lua-cjson` decodes
  `1e999` / `NaN` / `Infinity` but refuses to encode them; the unguarded `cjson.encode` raised after
  the `LPOP` and dropped the already-popped message. The encode is now `pcall`-guarded and such a body
  takes the A1 poison path (forwarded byte-identical to the dead-letter list, no `:reserved` residue).
- The reserve script returns an explicit poison flag as a third element; the client no longer infers
  poison from `reserved === job` (a two-element reply from a test double still falls back to that).
- **Zset release keeps every other byte of the body.** The released body is built by splicing only
  the top-level `attempts` value instead of a `JSON.parse` / `JSON.stringify` round trip, so integers
  above 2^53 in unknown keys are no longer rounded (ADR-0031).
- **Zset terminal-failure dead-letter is gated on the `ZREM`**, like the legacy scripts: a late
  dead-letter whose reservation was already recovered writes nothing (and warns) instead of leaving
  the body both ready and in the DLQ. `RedisQueue.deadLetter` returns `false` in that case. This
  deliberately differs from Laravel, whose unconditional `rpush` + `zrem` can duplicate the message.
  Known limit (broker-bindings §1, zset layout: byte-equal copies reserved together share one `:reserved`
  member): of two such copies failing terminally, only the first `ZREM` wins, so they land in the DLQ
  as one record.
- **Zset poison dead-letter is unconditional** (section 1.1: the SDK MUST write the message to the
  DLQ and `ZREM` it). A reservation the reserve script flagged as poison carries `poison: true` and is
  dead-lettered by a script with no `ZREM` gate, so two byte-equal poison bodies reserved together
  both reach the DLQ.

## [1.1.0] - 2026-06-21

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` so a consumer span links as a true child of the
  producer span. Redis lists have no native per-message metadata channel (the `LREM` ack handle
  *is* the stored value), so a non-empty `publish({ headers })` RPUSHes a transport-owned JSON
  **frame** (`{"__bq_frame":1,"headers":…,"body":<raw envelope>}`) that wraps the envelope without
  touching it (GR-1). The consumer `unframe`s it and surfaces the carried headers to the handler's
  third argument; a bare (pre-0028 / cross-version) value consumes with empty headers and
  byte-for-byte the same ack handle. New exports: `frameValue`, `unframe`, `headersOf`, `FRAME_KEY`.
  A header-less publish stays byte-identical. Bumped `@babelqueue/core` to `^1.4.0`.

## [1.0.0] - 2026-06-14

### Added
- Initial release. A Redis adapter on `@babelqueue/core` over the §1 reliable-queue pattern
  (`ioredis`). `RedisPublisher` (`RPUSH` the byte-identical envelope — no wrapping, unlike
  BullMQ's job layout) and `RedisConsumer` (reserve the head into `<queue>:processing` via
  `BRPOPLPUSH` so an in-flight message survives a crash, route by URN, `LREM` on success; a
  throwing handler requeues with `attempts + 1` then dead-letters to `<queue>.dlq`;
  `fail`/`delete`/`release`/`dead_letter` unknown-URN strategies; poison bodies forwarded raw to
  the DLQ). A Node-owned reliable queue — full Laravel reserved-set parity on a shared queue is a
  separate task. Built on `@babelqueue/core`; `ioredis` is an optional peer (the client is
  injected, so the unit tests use a fake — no Redis, no broker). Dual ESM+CJS. The envelope is
  unchanged (`schema_version: 1`).
