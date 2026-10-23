# Changelog

All notable changes to `@babelqueue/kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added

- `RetryReinjector` (contract §6.4, ADR-0034 §5-7): the consumer that moves a record from `<topic>.retry.<n>` back to
  its work topic once `record.timestamp + tier_ms` has elapsed (clamp to `[now, now + tier_ms]`, `seek` + `pause` while waiting). The
  re-inject is byte-identical (value and key), drops `bq-delay` / `bq-original-topic`, keeps `traceparent`, and
  commits the offset only after the broker acknowledged the write.
- `DEFAULT_RETRY_TIERS_MS` (`5s, 30s, 5m, 30m`).
- Tests: the cross-SDK conformance `backoff` (topic names, tier selection, republish, default tiers) and
  `kafka_poison` (92 cases) sections run against the consumer, and results are written through
  `BQ_CONFORMANCE_RESULTS`.

### Fixed

- `RetryReinjector` pins the due time per `(topic, partition, offset)` on first sight and reuses it on every
  redelivery until the offset is committed, so a future-dated record is held for one tier in total instead of one
  tier per delivery (the clamp was recomputed against a fresh `now` after each `resume`). One pin is kept per
  partition, so a pin stranded by a rebalance is replaced by the next record on that partition instead of
  accumulating.
- A tombstone (`null` value) is forwarded as `null` by the re-inject and the raw dead-letter path instead of an
  empty byte string.

### Changed

- The poison decision is read from the vendored conformance `kafka_poison` section; the hand-kept
  `test/poison-trigger-set.json` is removed.
- Retry delay stays the tier's (`retryBackoff*` does not apply on Kafka, ADR-0034 §9).
- Docs only: the note in the previous release that the Go reference's case-insensitive envelope-key read "is tightened separately" is now closed — Go core 1.14.0 reads envelope keys exactly (case-sensitive, last duplicate wins), so every SDK now decides poison on the same exact-case keys.

## [1.3.0] - 2026-10-12

### Changed

- **Poison records are wrapped, not forwarded raw** (Go/PHP parity, contract §6.5). A body the consumer
  cannot decode (invalid JSON, non-object root, wrong member types, decode error, non-conformant envelope)
  is now written to `<topic>.dlq` as a JSON body: a single JSON object keeps its keys, anything else becomes
  `{"raw": "<body>"}`, and a `dead_letter` block (`reason: "poison"`, `original_queue`, `attempts` from
  `bq-attempts`, `lang: "node"`) replaces any existing one. The record key is preserved; the DLQ record
  carries `bq-attempts` and `bq-original-topic` plus the `bq-*` headers projected from the body when present
  (inbound custom headers are no longer copied). The schema-version gate rejection (§6.7) still forwards the
  record raw.
- **Poison trigger set aligned to Go** (behaviour change). A body is now poison exactly when the Go reference's typed `Decode` (`Envelope` / `Meta` / `DeadLetter`) rejects it, checked by a dedicated scanner (`JSON.parse` would lose duplicate keys): invalid JSON, a BOM, trailing data, an empty body, a non-object root other than `null`; `job`, `trace_id`, `meta.id|queue|lang` and the `dead_letter` string members that are not string-or-null; `meta.schema_version`, `meta.created_at`, `attempts` and `dead_letter.failed_at|attempts` that are not an int64 integer literal (so `"2026"`, `1.5`, `1e2` and `true` are poison); `data` / `meta` / `dead_letter` that are not objects; a non-finite number inside `data`. Member names are matched exactly as spelled (case-sensitive, as the contract schema and the core decoder read them; `JOB` or `Meta` is an unknown member) and duplicate keys are each checked. Previously a wrongly typed `meta.created_at` / `meta.schema_version` or a fractional `attempts` was dispatched and a non-string `urn` alias was dead-lettered; now it is the reverse. A bare `null` body is an empty envelope (routed to the unknown-URN path), not poison.
- **The `EnvelopeCodec.accepts` consume gate is gone** (behaviour change, Go parity). A well-typed envelope that is merely non-conformant (empty `trace_id`, missing `data`, `meta.schema_version: 2`) is no longer dead-lettered as poison: Go's consume path never calls `Accepts`, so it is routed by URN like any other record.
- Poison DLQ records: a body holding a number a JS double cannot carry (an integer beyond 2^53, `1e400`, `9007199254740993.0`, `9007199254740993e0`, `1.2345678901234567890123`) is carried verbatim as `{"raw": ...}` instead of being silently altered; the check compares the normalised decimal value of the token with that of the double. Remaining intentional differences from the Go reference for a poison record: `dead_letter.attempts` is the `bq-attempts` header (Go writes 0) and `original_queue` is the `bq-original-topic` header when present (Go writes the consumed topic); the DLQ record also carries `bq-original-topic` plus the `bq-*` headers projected from the body (Go adds no headers); and a float literal such as `1.0` / `1e2` is re-formatted where the body is re-encoded. The wire envelope and the `dead_letter` shape are unchanged.
- **Any decode failure is poison; the partition is never left to wedge.** Member names are matched case-sensitively, exactly as the core decoder reads them (no `encoding/json`-style folding; the Go reference's case-insensitive read contradicts the contract schema and is tightened separately), and the scanner's nesting limit is counted over the whole document like Go's 10000 (so `[`-depth 9998 under `data.x` / `meta.zz`, 9999 at top level, is the deepest accepted). A poison body too deeply nested for `JSON.stringify` (a `RangeError`) is carried verbatim under `raw` instead of escaping the DLQ write. The wire envelope and the `dead_letter` shape are unchanged.

### Fixed

- **`KafkaRedriveIO` restore writes the consumed DLQ record back verbatim** (Go/Java/.NET parity). A restore
  write (`publishFrom` where the queue is the record's own `topic` and the body is unchanged: dry run,
  unselected, undecodable, failed redrive write) used to re-derive the `bq-*` headers from the body and stamp
  the record with `meta.created_at`. It now re-writes the original value bytes, headers and key and stamps a
  fresh timestamp, so the restored copy keeps its provenance (`bq-original-topic`, ...) and does not age out
  of the DLQ early. New optional `KafkaRedriveMessage` `value` / `headers` fields carry the consumed record;
  `pop` keeps them on its tracked copy. A message without them still restores (body as value; `bq-*`
  projection when no headers), only with the fresh timestamp. The redrive to the work topic is unchanged.
  Restore detection needs `topic` to be set on the reserved message (the `pop` option now documents this).
  A header the consumer decoded as `null` is not re-written on restore (known limit).

### Added

- **`KafkaRedriveIO` rewinds on a run that stops on a failed message.** New optional `consumer`
  (`KafkaSeekClient`, i.e. `consumer.seek`) option and `KafkaRedriveMessage` `topic` / `partition` /
  `offset` fields: with a consumer the IO offers core's optional `RedriveIO.rewind`, which seeks the
  consumer back to the record - and, because redrive pops the whole batch before processing it, every
  affected partition back to its first unacknowledged record - so a later cumulative offset commit
  cannot skip any of them. A record whose restore failed is never acked; if the rewind fails too both
  errors are thrown together. A record that was acknowledged (restored after a failed redrive write) is
  skipped by the rewind, so only the rest of the batch is redelivered. `pop` no longer mutates the caller's
  message: it returns a tracked copy built from the message's `body`, `key`, `topic`, `partition` and
  `offset` fields (read one by one, so a message class exposing them as getters keeps them). Without
  `consumer` nothing changes.
  Needs the `@babelqueue/core` release that adds `RedriveIO.rewind` (older cores simply never call it).

## [1.2.0] - 2026-10-11

### Added

- **`KafkaRedriveIO` — DLQ redrive carries the record key.** A `@babelqueue/core` `RedriveIO` over a
  `KafkaProducerClient` + a caller-supplied `pop`. Its `publishFrom` re-publishes with the reserved DLQ
  record's key byte-for-byte (null stays null, empty stays empty) for both the redrive write and the
  restore write back to the DLQ, per contract §6.5 "Record key on republish". **Requires
  `@babelqueue/core` ^1.9.0**, which adds the optional `RedriveIO.publishFrom` capability (with core
  <= 1.8.0 `redrive()` ignores `publishFrom`, falls back to the keyless `publish` and the redriven
  record loses its key); the dependency range is raised accordingly. No wire, header or
  `dead_letter` change.

### Fixed

- **A throwing `onError` callback can no longer bypass settlement.** The version-gate, undecodable
  (decode error) and non-conformant-envelope branches reported to `onError` *before* the DLQ write
  and the offset commit, so a callback that threw skipped both and the record redelivered forever.
  Every branch now settles first (DLQ / retry + commit), then reports through a guarded `report()`
  that swallows callback exceptions; the original error is still reported when settlement itself
  fails. Behaviour change: a throwing `onError` is now swallowed instead of propagating out of
  `handle()`. An `async` `onError` whose promise rejects is swallowed too (no unhandled
  rejection). No wire, header or `dead_letter` change.

## [1.1.3] - 2026-10-11

### Fixed

- **The original record key now survives retry republish, unknown-URN release, and the envelope
  DLQ write.** Only the raw (poison) DLQ forward kept the key before; the retry-topic and envelope
  DLQ writes dropped it, so a keyed record lost its key after the first retry. The key is carried
  byte-for-byte (an empty key stays empty); a null/absent key stays null and is never synthesised.
  Partition equality is not guaranteed. The envelope, the `dead_letter` block and the header
  projection are unchanged. No public API change.

## [1.1.2] - 2026-10-11

### Fixed

- A `bq-schema-version` header present with a null value is now treated as absent (§6.7), like
  every other SDK: the record decodes normally. Previously the null was stringified to `"null"`
  and the record was rejected by the version gate. A null header value also no longer crashes
  the raw DLQ forward (`Buffer.from(null)` threw a `TypeError`); it is dropped from the forwarded
  headers.
- The exported `IncomingHeaders` type now admits `null` values (`Buffer | string | (Buffer | string)[]
  | null | undefined`), matching what kafkajs can deliver at runtime. This widens an input type only;
  producer-side types are unchanged.

## [1.1.1] - 2026-10-10

### Fixed
- **The consumer now gates on `bq-schema-version` before decoding the record value (contract §6.7).**
  A record whose `bq-schema-version` header is present, non-blank and not exactly the core's
  supported `"1"` (`"2"`, `"x"`, `"01"`, `" 1"`, ...) is rejected undecoded down the poison path:
  reported to `onError` (with a `null` envelope), the raw value forwarded to `<topic>.dlq` when one is
  configured, and the offset committed. A missing or blank value (empty or only ASCII whitespace)
  keeps the existing decode path. The envelope and the producer are unchanged.
- **Raw DLQ forwarding now keeps the original record metadata.** Every path that forwards a record
  undecoded to `<topic>.dlq` (version-gate rejection, decode error, non-conformant envelope) used to
  send only the value and a fresh timestamp. It now also forwards the original record `key` and all
  headers, aligned with the Java reference. Header values are copied as `Buffer`s, array-valued
  headers stay arrays, and the timestamp is still re-stamped. The commit-after-forward behaviour
  is unchanged.

## [1.1.0] - 2026-06-21

### Added
- **OpenTelemetry v0.2 — `traceparent` transport wiring (ADR-0028).** Carries the out-of-band
  `HeaderCarrier` from `@babelqueue/core@^1.4.0` as additional Kafka **record headers** beside the
  contract `bq-*` headers — the contract headers win a key collision (merge-not-clobber).
  `publish({ headers })` injects them; the consumer reads the inbound record headers back and
  surfaces them to the handler's third argument, so the core's `otel` extract links the consumer
  span as a true child of the producer span. New `headersOf` export. A header-less publish stays
  byte-identical. Bumped `@babelqueue/core` to `^1.4.0`.

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache Kafka adapter on `@babelqueue/core` + KafkaJS, implementing §6 of
  the broker-bindings contract. Kafka has no native ack/delay/DLQ/delivery-counter, so the
  binding absorbs all four: `KafkaPublisher` (value = canonical envelope, record timestamp =
  `meta.created_at`, the `bq-` header projection — `bq-job` routes; a delay routes to a
  `<topic>.retry.<n>` tier or throws without one) and `KafkaConsumer` (**process-then-commit**
  manual-commit consume via `eachMessage` + `commitOffsets`; a throwing handler republishes to
  the next retry tier with **`bq-attempts + 1`** then commits; terminal failures go to
  `<topic>.dlq` with the additive `dead_letter` block; the `bq-attempts` header is the
  authoritative counter with the body as fallback; `fail`/`delete`/`release`/`dead_letter`
  unknown-URN strategies; poison records forwarded raw to the DLQ). `RetryTopics` configures the
  tiered retry/delay topology. Built on `@babelqueue/core`; `kafkajs` is an optional peer (the
  producer/consumer are injected, so the unit tests use fakes — no Kafka, no broker). Dual
  ESM+CJS. The envelope is unchanged (`schema_version: 1`); Apache Kafka is purely additive.
