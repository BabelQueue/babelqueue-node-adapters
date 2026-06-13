# Changelog

All notable changes to `@babelqueue/redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

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
