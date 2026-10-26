/**
 * Redis adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **consumer** over two Redis layouts
 * (broker-bindings §1, ADR-0033):
 *
 * - **`legacy`** (default) — the reliable-queue pattern: `RPUSH` to produce, `LMOVE` the head of
 *   `<queue>` to `<queue>:processing` to reserve (a Lua script that also writes a recovery deadline
 *   to `<queue>:processing:deadlines`), and a deadline-gated `LREM` to acknowledge, so an in-flight
 *   message survives a crash. A **recovery sweep** runs before every reserve and moves a reservation
 *   whose deadline (`retryAfter`) passed back onto `<queue>`, body untouched. The pop end changed
 *   from the tail (`BRPOPLPUSH`) to the head (`LMOVE LEFT RIGHT`) so the queue is FIFO, like the Go,
 *   Python, Java and .NET SDKs.
 * - **`zset`** — the Laravel-compatible layout (`queues:<q>`, `:delayed`, `:reserved`, `:notify`):
 *   delayed publish, `retryAfter` recovery through the reserved sorted set, a Lua reserve that
 *   increments `attempts`, and blocking pop on the `:notify` list. Opt-in via `layout: "zset"`.
 *
 * Redis lists carry **no native metadata** — the list element **is** the canonical envelope JSON,
 * byte-for-byte, with **no wrapping** (unlike `@babelqueue/bullmq`, which uses BullMQ's own job
 * layout). Routing and tracing read the body's `job` / `trace_id`.
 *
 * `ioredis` is an optional peer — you provide the client (an `ioredis` instance satisfies the
 * adapter structurally).
 *
 * **Out-of-band headers (ADR-0028) — legacy layout only.** A Redis list element *is* the canonical
 * envelope JSON, so there is no native per-message metadata channel. To carry a
 * {@link HeaderCarrier} (e.g. a W3C `traceparent`) the legacy layout owns a tiny JSON *frame*
 * distinct from the wire envelope:
 *
 *     {"__bq_frame":1,"headers":{"traceparent":"00-…"},"body":"<raw wire envelope>"}
 *
 * Framing is **opt-in and backward compatible**: only `publish` with a non-empty `headers` carrier
 * writes a frame; a plain publish stores the **bare** envelope byte-for-byte. The consumer detects
 * frame-vs-bare by the reserved `__bq_frame` sentinel (a frozen envelope can never carry it).
 *
 * **The zset layout never writes a frame (ADR-0033 A3).** A Laravel worker on the same queue would
 * read the frame as a job, so a header-carrying publish drops the headers and stores the bare
 * envelope; a `traceparent` is therefore not propagated in this layout (`trace_id` stays in the
 * envelope, GR-4). GR-1: the wire envelope is never touched.
 */

import { randomBytes } from "node:crypto";

import { annotate, BabelQueueError, EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";
import { buildPoisonRecord } from "./poison-record.js";

// --- Minimal Redis shape (a structural subset of ioredis) ----------------------

/** The subset of an ioredis client this adapter calls. */
export interface RedisClient {
  /** Append a value to the tail of a list; returns the new length. */
  rpush(key: string, value: string): Promise<number>;
  /** Add `member` to a sorted set with `score`. */
  zadd(key: string, score: number | string, member: string): Promise<number | string>;
  /** Blocking pop from the head of a list for up to `timeout` seconds (fractions need Redis 6+);
   *  resolves to `[key, value]`, or `null` on timeout. */
  blpop(key: string, timeout: number): Promise<[string, string] | null>;
  /** Run a Lua script with `numKeys` keys followed by the arguments. */
  eval(script: string, numKeys: number, ...args: Array<string | number>): Promise<unknown>;
  /**
   * The binary-safe twin of {@link RedisClient.eval} (ioredis `evalBuffer`): string replies come back as
   * `Buffer`s and `Buffer` arguments are sent byte for byte. Optional for publishing, but required to consume
   * ({@link RedisQueue.reserve} / {@link RedisConsumer} throw without it): the reserve scripts and the settle
   * scripts of a body that is not valid UTF-8 use it so the exact stored bytes survive. Invalid UTF-8 is poison
   * and a poison record never carries a U+FFFD substitution (broker-bindings.md section 1.1, ADR-0035 section 1),
   * which a string-only `eval` cannot honour.
   */
  evalBuffer?(script: string, numKeys: number, ...args: Array<string | number | Buffer>): Promise<unknown>;
}

// --- Payload (contract §1) -----------------------------------------------------

/** The list element for an envelope: the canonical JSON, verbatim — no wrapping (§1). */
export function redisValue(envelope: Envelope): string {
  return EnvelopeCodec.encode(envelope);
}


// --- Header frame (ADR-0028) ---------------------------------------------------

/** The reserved discriminator key + current version of the transport-owned header frame. */
export const FRAME_KEY = "__bq_frame";
const FRAME_VERSION = 1;

/** The transport-owned frame the list value carries when out-of-band headers accompany a message. */
interface HeaderFrame {
  __bq_frame: number;
  headers?: HeaderCarrier;
  body: string;
}

/**
 * The pure produce-side decision: the exact string to `RPUSH` for `body` + `headers`. With no
 * usable headers it returns `body` verbatim (the **bare** form, so a plain publish and a
 * header-less publish store byte-identical values); otherwise it returns the transport-owned frame
 * JSON. Kept pure so the framing decision is unit-testable without a broker.
 */
export function frameValue(body: string, headers: HeaderCarrier | null | undefined): string {
  const clean = sanitizeHeaders(headers);
  if (Object.keys(clean).length === 0) return body;
  const frame: HeaderFrame = { [FRAME_KEY]: FRAME_VERSION, headers: clean, body };
  return JSON.stringify(frame);
}

/**
 * Interpret a stored Redis list value: `[wire-envelope-body, headers]`. A value is a header frame
 * iff it is a JSON object carrying the reserved `__bq_frame` sentinel (a frozen wire envelope never
 * has it); then it yields the unframed body plus the carried headers. Any other value — a bare
 * envelope, non-JSON, or JSON without the sentinel — is returned verbatim as the body with empty
 * headers, so older / cross-version queue values consume exactly as before.
 */
export function unframe(value: string): [string, HeaderCarrier] {
  // Cheap reject: a frame is always a JSON object, and the sentinel substring must appear. This
  // avoids a full parse for the overwhelmingly common bare-envelope case (only short-circuits
  // negatives).
  if (value === "" || value[0] !== "{" || !value.includes(`"${FRAME_KEY}"`)) {
    return [value, {}];
  }
  let decoded: unknown;
  try {
    decoded = JSON.parse(value);
  } catch {
    return [value, {}];
  }
  if (
    decoded == null ||
    typeof decoded !== "object" ||
    !(FRAME_KEY in decoded) ||
    !(decoded as HeaderFrame)[FRAME_KEY] ||
    typeof (decoded as HeaderFrame).body !== "string"
  ) {
    return [value, {}];
  }
  const frame = decoded as HeaderFrame;
  return [frame.body, sanitizeHeaders(frame.headers)];
}

/** The out-of-band headers carried by a stored list value (empty for a bare value). */
export function headersOf(value: string): HeaderCarrier {
  return unframe(value)[1];
}


// --- Layouts, keys and Lua (ADR-0033) -------------------------------------------

/** The Redis layout a queue uses. `legacy` is the default `:processing` reliable queue. */
export type RedisLayout = "legacy" | "zset";

/** The four keys of the Laravel-compatible zset layout. */
export interface ZsetKeys {
  ready: string;
  delayed: string;
  reserved: string;
  notify: string;
}

/** The default key prefix of the zset layout (Laravel's `queues:`). */
export const ZSET_KEY_PREFIX = "queues:";

/**
 * The zset-layout keys for `queue`: `queues:<q>`, `:delayed`, `:reserved`, `:notify`. In a Redis
 * Cluster, hash tags (`{orders}`) are the caller's responsibility (put them in `queue`).
 */
export function zsetKeys(queue: string, prefix: string = ZSET_KEY_PREFIX): ZsetKeys {
  const ready = `${prefix}${queue}`;
  return { ready, delayed: `${ready}:delayed`, reserved: `${ready}:reserved`, notify: `${ready}:notify` };
}

/**
 * The Lua scripts, exported so a test double can dispatch `eval` on script identity. Every script
 * is atomic; none of them rewrites a body except the zset reserve (Laravel's `cjson` increment).
 */
export const LUA = {
  /** KEYS=[q, q:processing, q:processing:deadlines] ARGV=[deadline, token]. Returns the raw body or nil. */
  legacyReserve: `
local raw = redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
if not raw then return false end
redis.call('ZADD', KEYS[3], ARGV[1], ARGV[2] .. ':' .. raw)
return raw
`,
  /** KEYS=[q, q:processing, q:processing:deadlines] ARGV=[floor(now), batch]. Returns the count moved. */
  legacySweep: `
local batch = tonumber(ARGV[2])
local members
if batch > 0 then
  members = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, batch)
else
  members = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[1])
end
local moved = 0
for _, m in ipairs(members) do
  if #m >= 34 and string.sub(m, 33, 33) == ':' then
    local raw = string.sub(m, 34)
    if redis.call('LREM', KEYS[2], 1, raw) == 1 then
      redis.call('RPUSH', KEYS[1], raw)
      moved = moved + 1
    end
  end
  redis.call('ZREM', KEYS[3], m)
end
return moved
`,
  /** KEYS=[q:processing, q:processing:deadlines] ARGV=[token, raw]. 1 if the reservation was live. */
  legacyAck: `
if redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('LREM', KEYS[1], 1, ARGV[2])
  return 1
end
return 0
`,
  /** KEYS=[q, q:processing, q:processing:deadlines] ARGV=[token, raw, body]. */
  legacyRelease: `
if redis.call('ZREM', KEYS[3], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('LREM', KEYS[2], 1, ARGV[2])
  redis.call('RPUSH', KEYS[1], ARGV[3])
  return 1
end
return 0
`,
  /** KEYS=[q:processing, q:processing:deadlines, dlq] ARGV=[token, raw, body]. */
  legacyDeadLetter: `
if redis.call('ZREM', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('LREM', KEYS[1], 1, ARGV[2])
  redis.call('RPUSH', KEYS[3], ARGV[3])
  return 1
end
return 0
`,
  /**
   * KEYS=[q, q:reserved, q:notify] ARGV=[expiry]. Returns `{job, reserved, flag}` or nil; flag `'1'`
   * is poison (`reserved == job`), `'0'` a delivered body. Nothing after the LPOP may raise, so the
   * re-encode is guarded too: lua-cjson decodes `1e999` / `NaN` / `Infinity` but refuses to encode
   * them, and a bare `cjson.encode` here would lose the already-popped message (broker-bindings
   * section 1.1, ADR-0033 A1). An unencodable body takes the poison path.
   */
  zsetReserve: `
local job = redis.call('LPOP', KEYS[1])
if not job then return false end
local ok, decoded = pcall(cjson.decode, job)
if ok and type(decoded) == 'table' and type(decoded['attempts']) == 'number' then
  decoded['attempts'] = decoded['attempts'] + 1
  local eok, reserved = pcall(cjson.encode, decoded)
  if eok and type(reserved) == 'string' then
    redis.call('ZADD', KEYS[2], ARGV[1], reserved)
    redis.call('LPOP', KEYS[3])
    return {job, reserved, '0'}
  end
end
redis.call('ZADD', KEYS[2], ARGV[1], job)
return {job, job, '1'}
`,
  /** KEYS=[source zset, q, q:notify] ARGV=[floor(now), batch]. Returns the count moved. */
  zsetMigrate: `
local batch = tonumber(ARGV[2])
local val
if batch > 0 then
  val = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, batch)
else
  val = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
end
if #val > 0 then
  redis.call('ZREMRANGEBYRANK', KEYS[1], 0, #val - 1)
  for i = 1, #val, 100 do
    redis.call('RPUSH', KEYS[2], unpack(val, i, math.min(i + 99, #val)))
  end
  for _ = 1, #val do
    redis.call('RPUSH', KEYS[3], '1')
  end
end
return #val
`,
  /** KEYS=[q, q:notify] ARGV=[body]. */
  zsetPush: `
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('RPUSH', KEYS[2], '1')
return 1
`,
  /** KEYS=[q:reserved] ARGV=[member]. */
  zsetAck: `
return redis.call('ZREM', KEYS[1], ARGV[1])
`,
  /** KEYS=[q:delayed, q:reserved] ARGV=[member, score, body]. The ZADD is unconditional. */
  zsetRelease: `
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
return 1
`,
  /**
   * KEYS=[q:reserved, dlq] ARGV=[member, body]. Terminal-failure dead-letter, gated on the ZREM like
   * the legacy scripts: a late dead-letter whose reservation was already recovered (back on `<q>`)
   * writes nothing, so the body is never both ready and dead-lettered. A poison reservation uses
   * `zsetDeadLetterPoison` instead (unconditional, as the contract requires).
   */
  zsetDeadLetter: `
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  redis.call('RPUSH', KEYS[2], ARGV[2])
  return 1
end
return 0
`,
  /**
   * KEYS=[q:reserved, dlq] ARGV=[member, body]. The poison dead-letter (broker-bindings section 1.1):
   * the SDK MUST write the message to the DLQ and `ZREM` it, unconditionally. Two byte-equal poison
   * bodies reserved together share one `:reserved` member, so a `ZREM` gate would drop the second
   * copy without ever writing it.
   */
  zsetDeadLetterPoison: `
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
`,
} as const;

// --- Transport -----------------------------------------------------------------

/** Options for {@link RedisQueue} (and, through it, {@link RedisPublisher} / {@link RedisConsumer}). */
export interface RedisQueueOptions {
  /** `"legacy"` (default, the `:processing` reliable queue) or `"zset"` (Laravel-compatible). */
  layout?: RedisLayout;
  /**
   * Seconds before an unacknowledged reservation is recovered (default 90). `null` disables
   * recovery (the legacy deadline member is written with score `+inf`; the zset `:reserved`
   * migrate is skipped). A negative or non-finite value is a configuration error. In the zset
   * layout a recovery counts as an attempt. The handler timeout SHOULD be shorter than this.
   */
  retryAfter?: number | null;
  /** Max items one migrate/sweep moves; `-1` (default) or `0` means no limit. */
  migrationBatchSize?: number;
  /** Epoch-seconds clock (fractions allowed); injectable for tests. Default `Date.now() / 1000`. */
  clock?: () => number;
  /** Zset key prefix (default `"queues:"`). Ignored by the legacy layout. */
  keyPrefix?: string;
  /** Suffix for the legacy reservation list (default `":processing"`). */
  processingSuffix?: string;
  /**
   * The dead-letter list (default `<queue>.dlq`). `null` disables the dead-letter of an **exhausted** message and of
   * an **unknown URN** only (they are dropped; `on_unknown_urn: dead_letter` degrades to `delete`). A **poison** body
   * is still dead-lettered as the ADR-0035 record in every configuration (broker-bindings §1.1, ADR-0037 decision 5):
   * to the configured name, and with `null` to the default `<queue>.dlq`.
   */
  deadLetterQueue?: string | null;
  /** Receives the "reservation expired" warning (default `console.warn`). */
  onWarn?: (message: string) => void;
}

/** A legacy reservation: the raw body plus the 32-hex token of its deadline member. */
export interface LegacyReservation {
  layout: "legacy";
  raw: string;
  token: string;
  /**
   * The exact stored bytes, set only when they are not valid UTF-8 (so `raw` carries U+FFFD substitutions).
   * The settle scripts and the poison record use these, never the lossy `raw`.
   */
  rawBytes?: Buffer;
}

/** A zset reservation: the raw body (what the handler sees) and the exact `:reserved` member. */
export interface ZsetReservation {
  layout: "zset";
  raw: string;
  reserved: string;
  /** The exact stored bytes of the job / `:reserved` member, set only when they are not valid UTF-8. */
  rawBytes?: Buffer;
  reservedBytes?: Buffer;
  /**
   * Set when the reserve script flagged the body as poison (undecodable, or `attempts` not a
   * number): its dead-letter is then unconditional, not gated on the `ZREM`.
   */
  poison?: boolean;
}

/** Whatever a reserve handed out; pass it back to ack / release / deadLetter. */
export type Reservation = LegacyReservation | ZsetReservation;

/** The outcome of one non-blocking {@link RedisQueue.reserve}. */
export interface ReserveResult {
  outcome: "delivered" | "empty" | "poison";
  /** The raw body the handler sees (absent on `empty`). */
  raw?: string;
  /** The reservation handle (absent on `empty`). */
  reservation?: Reservation;
  /** Zset only: the delivery count the handler is told (`job.attempts + 1`); absent on poison. */
  attempts?: number;
}

/**
 * A reply value as `[text, bytes]`. `bytes` is set only when the value is a `Buffer` that is not valid UTF-8,
 * i.e. when `text` would carry U+FFFD substitutions; valid UTF-8 needs no side copy.
 */
function readValue(value: unknown): [string, Buffer | undefined] {
  if (!Buffer.isBuffer(value)) return [String(value), undefined];
  const text = value.toString("utf8");
  return [text, Buffer.from(text, "utf8").equals(value) ? undefined : value];
}

/** The exact stored bytes of a reservation's value, as the settle scripts must compare them. */
function storedValue(reservation: Reservation): string | Buffer {
  return reservation.rawBytes ?? reservation.raw;
}

/** The exact `:reserved` member of a zset reservation. */
function zsetMember(reservation: ZsetReservation): string | Buffer {
  return reservation.reservedBytes ?? reservation.reserved;
}

/** A fresh 128-bit reservation token: 32 lowercase hex characters from the OS CSPRNG. */
export function newReservationToken(): string {
  return randomBytes(16).toString("hex");
}

function validateRetryAfter(value: number | null | undefined): number | null {
  if (value === undefined) return 90;
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new BabelQueueError(`Invalid retryAfter ${String(value)}: must be a non-negative number of seconds, or null.`);
  }
  return value;
}

/**
 * The Redis operations behind the publisher and the consumer, one set per layout. The methods are
 * non-blocking and take their time from the injectable clock, so they can be driven step by step
 * (the conformance suite does exactly that).
 */
export class RedisQueue {
  readonly layout: RedisLayout;
  readonly retryAfter: number | null;
  readonly migrationBatchSize: number;
  readonly deadLetterKey: string | null;
  /** Where a poison record goes: the configured dead-letter list, or `<queue>.dlq` when it is `null` (ADR-0037 decision 5). */
  readonly poisonDeadLetterKey: string;
  /** The zset keys (zset layout) — also usable to inspect the legacy ready list via `ready`. */
  readonly keys: ZsetKeys;
  readonly processingKey: string;
  readonly deadlinesKey: string;

  private readonly clock: () => number;
  private readonly warn: (message: string) => void;

  constructor(
    private readonly client: RedisClient,
    readonly queue: string,
    options: RedisQueueOptions = {},
  ) {
    this.layout = options.layout ?? "legacy";
    if (this.layout !== "legacy" && this.layout !== "zset") {
      throw new BabelQueueError(`Unknown Redis layout ${String(this.layout)}: expected "legacy" or "zset".`);
    }
    this.retryAfter = validateRetryAfter(options.retryAfter);
    const batch = options.migrationBatchSize ?? -1;
    this.migrationBatchSize = Number.isFinite(batch) && batch > 0 ? Math.floor(batch) : -1;
    this.clock = options.clock ?? (() => Date.now() / 1000);
    this.warn = options.onWarn ?? ((message) => console.warn(message));
    this.deadLetterKey = options.deadLetterQueue === undefined ? `${queue}.dlq` : options.deadLetterQueue;
    this.poisonDeadLetterKey = this.deadLetterKey ?? `${queue}.dlq`;
    this.keys = this.layout === "zset" ? zsetKeys(queue, options.keyPrefix) : { ready: queue, delayed: "", reserved: "", notify: "" };
    this.processingKey = queue + (options.processingSuffix ?? ":processing");
    this.deadlinesKey = `${this.processingKey}:deadlines`;
  }

  /**
   * Run a script. `binary` (or any `Buffer` argument) selects the client's `evalBuffer`, when it has one,
   * so replies and arguments are exact bytes.
   */
  private run(script: string, keys: string[], args: Array<string | number | Buffer>, binary = false): Promise<unknown> {
    const useBuffer = (binary || args.some((a) => Buffer.isBuffer(a))) && this.client.evalBuffer !== undefined;
    if (useBuffer) return this.client.evalBuffer!(script, keys.length, ...keys, ...args);
    return this.client.eval(script, keys.length, ...keys, ...(args as Array<string | number>));
  }

  private nowSeconds(): number {
    return Math.floor(this.clock());
  }

  private deadlineScore(): string {
    return this.retryAfter === null ? "+inf" : String(this.nowSeconds() + this.retryAfter);
  }

  /** Append a body to the ready list (zset: also one `:notify` token). */
  async push(body: string): Promise<void> {
    if (this.layout === "zset") {
      await this.run(LUA.zsetPush, [this.keys.ready, this.keys.notify], [body]);
    } else {
      await this.client.rpush(this.queue, body);
    }
  }

  /**
   * Schedule a body for `delay` seconds from now: `ZADD <q>:delayed floor(now)+ceil(delay)` with
   * the body byte-for-byte and no `:notify` token. Zset layout only.
   */
  async pushDelayed(body: string, delay: number): Promise<void> {
    if (this.layout !== "zset") {
      throw new BabelQueueError("A delayed publish requires layout \"zset\"; the legacy layout has no delayed set.");
    }
    if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) {
      throw new BabelQueueError(`Invalid delay ${String(delay)}: must be a non-negative number of seconds.`);
    }
    await this.client.zadd(this.keys.delayed, this.nowSeconds() + Math.ceil(delay), body);
  }

  /** Zset: move due `:delayed` items, then due `:reserved` ones (skipped if `retryAfter` is null). */
  async migrate(): Promise<number> {
    if (this.layout !== "zset") return 0;
    let moved = await this.migrateFrom(this.keys.delayed);
    if (this.retryAfter !== null) moved += await this.migrateFrom(this.keys.reserved);
    return moved;
  }

  private async migrateFrom(source: string): Promise<number> {
    const moved = await this.run(
      LUA.zsetMigrate,
      [source, this.keys.ready, this.keys.notify],
      [this.nowSeconds(), this.migrationBatchSize],
    );
    return Number(moved);
  }

  /** Legacy: move expired reservations back onto the ready list (body untouched). */
  async sweep(): Promise<number> {
    if (this.layout !== "legacy") return 0;
    const moved = await this.run(
      LUA.legacySweep,
      [this.queue, this.processingKey, this.deadlinesKey],
      [this.nowSeconds(), this.migrationBatchSize],
    );
    return Number(moved);
  }

  /** Recovery and migration for the layout; runs before every reserve. */
  async maintain(): Promise<number> {
    return this.layout === "zset" ? this.migrate() : this.sweep();
  }

  /** One non-blocking reserve, preceded by the layout's migrate / sweep. */
  async reserve(): Promise<ReserveResult> {
    // Checked before anything is popped, so nothing is lost: a string-only client would turn invalid UTF-8
    // into U+FFFD and hand a lossy body to a handler, which broker-bindings.md section 1.1 forbids.
    if (this.client.evalBuffer === undefined) {
      throw new BabelQueueError(
        "The Redis client must provide evalBuffer (ioredis does) to consume: invalid UTF-8 is poison and its exact bytes must be read (broker-bindings.md section 1.1).",
      );
    }
    await this.maintain();
    return this.layout === "zset" ? this.reserveZset() : this.reserveLegacy();
  }

  private async reserveLegacy(): Promise<ReserveResult> {
    const token = newReservationToken();
    const reply = await this.run(
      LUA.legacyReserve,
      [this.queue, this.processingKey, this.deadlinesKey],
      [this.deadlineScore(), token],
      true,
    );
    if (typeof reply !== "string" && !Buffer.isBuffer(reply)) return { outcome: "empty" };
    const [raw, rawBytes] = readValue(reply);
    const reservation: LegacyReservation = { layout: "legacy", raw, token };
    if (rawBytes) reservation.rawBytes = rawBytes;
    return { outcome: "delivered", raw, reservation };
  }

  private async reserveZset(): Promise<ReserveResult> {
    const reply = await this.run(
      LUA.zsetReserve,
      [this.keys.ready, this.keys.reserved, this.keys.notify],
      [this.deadlineScore()],
      true,
    );
    if (!Array.isArray(reply) || reply.length < 2) return { outcome: "empty" };
    const [raw, rawBytes] = readValue(reply[0]);
    const [reserved, reservedBytes] = readValue(reply[1]);
    const reservation: ZsetReservation = { layout: "zset", raw, reserved };
    if (rawBytes) reservation.rawBytes = rawBytes;
    if (reservedBytes) reservation.reservedBytes = reservedBytes;
    // The script's third element is the authoritative poison flag; a two-element reply (a test
    // double) falls back to the `reserved == job` signal.
    const poison = reply.length >= 3 ? readValue(reply[2])[0] === "1" : raw === reserved;
    if (poison) return { outcome: "poison", raw, reservation: { ...reservation, poison: true } };
    return { outcome: "delivered", raw, reservation, attempts: attemptsOf(raw) + 1 };
  }

  /** Acknowledge. Legacy: false (plus a warning) if the reservation had already expired. */
  async ack(reservation: Reservation): Promise<boolean> {
    if (reservation.layout === "zset") {
      return Number(await this.run(LUA.zsetAck, [this.keys.reserved], [zsetMember(reservation)])) === 1;
    }
    const live = await this.run(LUA.legacyAck, [this.processingKey, this.deadlinesKey], [reservation.token, storedValue(reservation)]);
    return this.liveOrWarn(live, reservation);
  }

  /**
   * Give a reservation back. Legacy: the body (default: `raw`) goes to the ready list, gated on the
   * deadline member; `delay` is ignored. Zset: one script `ZREM`s the `:reserved` member and `ZADD`s
   * `<q>:delayed` at `floor(now)+ceil(delay)` with the body (default: `raw` re-encoded with
   * `attempts + 1`, every other key preserved).
   */
  async release(reservation: Reservation, delay = 0, body?: string): Promise<boolean> {
    if (reservation.layout === "zset") {
      if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) {
        throw new BabelQueueError(`Invalid delay ${String(delay)}: must be a non-negative number of seconds.`);
      }
      const bumped = body ?? bumpAttempts(reservation.raw);
      await this.run(
        LUA.zsetRelease,
        [this.keys.delayed, this.keys.reserved],
        [zsetMember(reservation), this.nowSeconds() + Math.ceil(delay), bumped],
      );
      return true;
    }
    const live = await this.run(
      LUA.legacyRelease,
      [this.queue, this.processingKey, this.deadlinesKey],
      [reservation.token, storedValue(reservation), body ?? storedValue(reservation)],
    );
    return this.liveOrWarn(live, reservation);
  }

  /**
   * Write `body` (default: `raw`) to the dead-letter list and settle the reservation. With the dead-letter list
   * disabled (`deadLetterQueue: null`) the message is acknowledged and dropped — except a `poison` record, which is
   * always written, to `<queue>.dlq` (broker-bindings §1.1, ADR-0037 decision 5).
   */
  async deadLetter(reservation: Reservation, body?: string, poison = false): Promise<boolean> {
    const payload = body ?? reservation.raw;
    const deadLetterKey = poison ? this.poisonDeadLetterKey : this.deadLetterKey;
    if (deadLetterKey === null) return this.ack(reservation);
    if (reservation.layout === "zset" && reservation.poison === true) {
      // Poison (section 1.1): always write to the DLQ and ZREM, even when a byte-equal twin already
      // removed the shared `:reserved` member.
      await this.run(
        LUA.zsetDeadLetterPoison,
        [this.keys.reserved, deadLetterKey],
        [zsetMember(reservation), payload],
      );
      return true;
    }
    if (reservation.layout === "zset") {
      const settled = await this.run(
        LUA.zsetDeadLetter,
        [this.keys.reserved, deadLetterKey],
        [zsetMember(reservation), payload],
      );
      if (Number(settled) === 1) return true;
      this.warn(
        `reservation expired: queue "${this.queue}" reservation was already recovered or settled; nothing dead-lettered`,
      );
      return false;
    }
    const live = await this.run(
      LUA.legacyDeadLetter,
      [this.processingKey, this.deadlinesKey, deadLetterKey],
      [reservation.token, storedValue(reservation), payload],
    );
    return this.liveOrWarn(live, reservation);
  }

  private liveOrWarn(reply: unknown, reservation: LegacyReservation): boolean {
    if (Number(reply) === 1) return true;
    this.warn(
      `reservation expired: queue "${this.queue}" token ${reservation.token} was already swept or settled; left the queue untouched`,
    );
    return false;
  }
}

/** `job.attempts` of a raw body, 0 when absent or unreadable. */
function attemptsOf(raw: string): number {
  try {
    const decoded: unknown = JSON.parse(raw);
    const attempts = (decoded as { attempts?: unknown } | null)?.attempts;
    return typeof attempts === "number" ? attempts : 0;
  } catch {
    return 0;
  }
}

/**
 * The raw body with `attempts` raised by one and every other byte untouched (ADR-0031): the zset
 * release / recovery body. Only the `attempts` value is spliced, so unknown keys keep their exact
 * text, including integers above 2^53 that a `JSON.parse` / `JSON.stringify` round trip would round.
 * Falls back to the raw value when it is not a JSON object.
 */
function bumpAttempts(raw: string): string {
  let decoded: unknown;
  try {
    decoded = JSON.parse(raw);
  } catch {
    return raw;
  }
  if (decoded === null || typeof decoded !== "object" || Array.isArray(decoded)) return raw;

  const scan = scanTopLevel(raw);
  const spans = scan.members.filter((member) => member.key === "attempts");
  const last = spans[spans.length - 1];
  const current = last === undefined ? null : raw.slice(last.start, last.end);
  const next = current !== null && NUMBER_LITERAL.test(current) ? incrementNumberLiteral(current) : "1";
  if (last === undefined) {
    const member = `"attempts":${next}`;
    return `${raw.slice(0, scan.close)}${scan.members.length > 0 ? "," : ""}${member}${raw.slice(scan.close)}`;
  }
  let result = raw;
  for (const span of [...spans].reverse()) result = `${result.slice(0, span.start)}${next}${result.slice(span.end)}`;
  return result;
}

const NUMBER_LITERAL = /^-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$/;

/** `literal + 1`, exact for an integer literal of any size and a plain `Number` sum otherwise. */
function incrementNumberLiteral(literal: string): string {
  if (/^-?\d+$/.test(literal)) return (BigInt(literal) + 1n).toString();
  return String(Number(literal) + 1);
}

interface TopLevelScan {
  /** The key and the `[start, end)` span of the value text of each top-level member, in order. */
  members: Array<{ key: string; start: number; end: number }>;
  /** Index of the closing `}` of the top-level object. */
  close: number;
}

/** Locate the top-level members of an already-validated JSON object text (no re-serialisation). */
function scanTopLevel(text: string): TopLevelScan {
  const members: TopLevelScan["members"] = [];
  const skipSpace = (from: number): number => {
    let i = from;
    while (i < text.length && " \t\n\r".includes(text.charAt(i))) i++;
    return i;
  };
  const stringEnd = (from: number): number => {
    let i = from + 1;
    while (text.charAt(i) !== '"') i += text.charAt(i) === "\\" ? 2 : 1;
    return i + 1;
  };
  let i = skipSpace(text.indexOf("{") + 1);
  while (text.charAt(i) !== "}") {
    if (text.charAt(i) === ",") i = skipSpace(i + 1);
    const keyEnd = stringEnd(i);
    const key = JSON.parse(text.slice(i, keyEnd)) as string;
    i = skipSpace(skipSpace(keyEnd) + 1);
    const start = i;
    let depth = 0;
    while (i < text.length) {
      const c = text.charAt(i);
      if (c === '"') {
        i = stringEnd(i);
        continue;
      }
      if (c === "{" || c === "[") depth++;
      else if (c === "}" || c === "]") {
        if (depth === 0) break;
        depth--;
      } else if (c === "," && depth === 0) break;
      i++;
    }
    let end = i;
    while (end > start && " \t\n\r".includes(text.charAt(end - 1))) end--;
    members.push({ key, start, end });
    i = skipSpace(i);
  }
  return { members, close: i };
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link RedisPublisher.publish}. */
export interface PublishOptions {
  traceId?: string;
  /**
   * Out-of-band transport headers to carry beside the frozen envelope (ADR-0028) — e.g. a W3C
   * `traceparent`. Legacy layout: a non-empty carrier RPUSHes a transport-owned frame; an
   * empty/omitted carrier stays a byte-identical bare publish. **Zset layout: dropped** (ADR-0033
   * A3) — the bare envelope is written and the `traceparent` is not propagated.
   */
  headers?: HeaderCarrier;
  /**
   * Deliver after `delay` seconds (`ZADD <q>:delayed floor(now)+ceil(delay)`, no `:notify` token).
   * Requires `layout: "zset"`; the legacy layout throws {@link BabelQueueError}.
   */
  delay?: number;
}

/** Sends canonical-envelope messages to one Redis queue. */
export class RedisPublisher {
  private readonly transport: RedisQueue;

  private constructor(
    client: RedisClient,
    private readonly queue: string,
    options: RedisQueueOptions,
  ) {
    this.transport = new RedisQueue(client, queue, options);
  }

  /** A publisher over `client`, producing to the queue named `queue`. */
  static create(client: RedisClient, queue: string, options: RedisQueueOptions = {}): RedisPublisher {
    return new RedisPublisher(client, queue, options);
  }

  /**
   * Build + push the canonical envelope; returns the message id (`meta.id`). Legacy layout: a
   * non-empty `options.headers` pushes a transport-owned frame (ADR-0028), otherwise the bare
   * envelope byte-for-byte. Zset layout: always the bare envelope (A3); with `options.delay > 0`
   * it goes to `<q>:delayed` instead of the ready list.
   */
  async publish(urn: string, data: Record<string, unknown>, options: PublishOptions = {}): Promise<string> {
    const delay = options.delay ?? 0;
    if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) {
      throw new BabelQueueError(`Invalid delay ${String(delay)}: must be a non-negative number of seconds.`);
    }
    if (delay > 0 && this.transport.layout !== "zset") {
      throw new BabelQueueError("A delayed publish requires layout \"zset\"; the legacy layout has no delayed set.");
    }
    const envelope = EnvelopeCodec.make(urn, data, { queue: this.queue, traceId: options.traceId });
    const body = redisValue(envelope);
    if (this.transport.layout === "zset") {
      if (delay > 0) await this.transport.pushDelayed(body, delay);
      else await this.transport.push(body);
    } else {
      await this.transport.push(frameValue(body, options.headers));
    }
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw stored list element, and the out-of-band
 * {@link HeaderCarrier} carried beside it (empty for a bare value and always empty in the zset
 * layout). Pass `headers` to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a
 * child of the producer span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, raw: string, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link RedisConsumer}. */
export interface RedisConsumerOptions extends RedisQueueOptions {
  /**
   * How long one {@link RedisConsumer.poll} waits for a message, in seconds (default 5). Legacy:
   * a sleep-poll (100 ms, doubling, capped at 1 s). Zset: `BLPOP` on `:notify` in slices of at most
   * 1 s, migrating between slices. `0` or less means one non-blocking attempt.
   */
  blockTimeout?: number;
  /** Attempts before terminal dead-lettering (default 3). */
  maxTries?: number;
  /** Strategy for a URN with no handler (default `fail`). */
  unknownUrn?: string;
  /** Called for a non-conformant message, an unmapped URN, or a throwing handler. */
  onError?: (error: unknown, envelope: IncomingEnvelope | null, raw: string) => void;
}

const SLEEP_START_MS = 100;
const SLEEP_CAP_MS = 1000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Consumes a Redis queue: reserve one message (legacy: `LMOVE` into `<queue>:processing` with a
 * deadline; zset: the Laravel pop), decode + validate, route to the handler for its URN, then
 * acknowledge. A throwing handler gives the message back with `attempts + 1` (at-least-once) up to
 * `maxTries`, then dead-letters to `<queue>.dlq`. Redis has no native delivery count, so `attempts`
 * lives in the body and the runtime owns retry.
 */
export class RedisConsumer {
  private readonly transport: RedisQueue;
  private readonly blockTimeout: number;
  private readonly maxTries: number;
  private readonly unknownUrn: string;

  constructor(
    private readonly client: RedisClient,
    private readonly queue: string,
    private readonly handlers: BabelHandlers,
    private readonly options: RedisConsumerOptions = {},
  ) {
    this.transport = new RedisQueue(client, queue, options);
    this.blockTimeout = options.blockTimeout ?? 5;
    this.maxTries = options.maxTries ?? 3;
    this.unknownUrn = options.unknownUrn ?? UnknownUrnStrategy.FAIL;
  }

  /** The underlying transport (for diagnostics and tests). */
  get redisQueue(): RedisQueue {
    return this.transport;
  }

  /** Reserve + route + settle one message. Returns true if one was handled, false on timeout. */
  async poll(): Promise<boolean> {
    const startedAt = Date.now();
    let wait = SLEEP_START_MS;
    for (;;) {
      const result = await this.transport.reserve();
      if (result.outcome !== "empty") {
        await this.dispatch(result);
        return true;
      }
      const remainingMs = this.blockTimeout * 1000 - (Date.now() - startedAt);
      if (remainingMs <= 0) return false;
      if (this.transport.layout === "zset") {
        // Wake on a :notify token; migrate again after at most one second (ADR-0033).
        await this.client.blpop(this.transport.keys.notify, Math.min(1, remainingMs / 1000));
      } else {
        await sleep(Math.min(wait, remainingMs));
        wait = Math.min(wait * 2, SLEEP_CAP_MS);
      }
    }
  }

  /** Poll while `shouldContinue` returns true. */
  async run(shouldContinue: () => boolean): Promise<void> {
    while (shouldContinue()) {
      await this.poll();
    }
  }

  private async dispatch(result: ReserveResult): Promise<void> {
    const reservation = result.reservation as Reservation;
    // `rawBytes` is only set for a stored value that is not valid UTF-8: poison even when cjson could decode it
    // (broker-bindings section 1.1, "Invalid UTF-8"). It is never delivered, retried or re-published.
    if (result.outcome === "poison" || reservation.rawBytes !== undefined) {
      // Undecodable body, non-numeric attempts (A1) or invalid UTF-8: dead-letter it as a poison record
      // (ADR-0035) and leave no residue.
      const error = new BabelQueueError("Rejected an undecodable BabelQueue message from Redis.");
      this.report(error, null, reservation.raw);
      await this.transport.deadLetter(reservation, this.poisonRecord(reservation, error), true);
      return;
    }
    await this.handle(reservation);
  }

  /**
   * Route + settle one reserved message. Exposed for testing.
   *
   * Legacy layout: `reservation.raw` may be a transport-owned header frame or a bare envelope; it is
   * unframed first so the envelope is decoded from the verbatim wire body while the carried headers
   * (e.g. a `traceparent`, ADR-0028) reach the handler. The zset layout is never framed (A3).
   */
  async handle(reservation: Reservation): Promise<void> {
    const raw = reservation.raw;
    if (reservation.rawBytes !== undefined) {
      // Invalid UTF-8 is poison (broker-bindings section 1.1): never decoded, handled or re-published.
      const error = new BabelQueueError("Rejected a non-UTF-8 BabelQueue message from Redis.");
      this.report(error, null, raw);
      await this.transport.deadLetter(reservation, this.poisonRecord(reservation, error), true);
      return;
    }
    const [body, headers] = reservation.layout === "legacy" ? unframe(raw) : [raw, {} as HeaderCarrier];
    const decoded = EnvelopeCodec.decode(body);

    if (!EnvelopeCodec.accepts(decoded)) {
      const error = new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Redis.");
      this.report(error, decoded, raw);
      await this.transport.deadLetter(reservation, this.poisonRecord(reservation, error), true);
      return;
    }

    const envelope = decoded as Envelope;
    const urn = EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      await this.onUnknownUrn(envelope, urn, reservation);
      return;
    }

    try {
      await handler(envelope, raw, headers);
    } catch (error) {
      this.report(error, envelope, raw);
      await this.retryOrDeadLetter(envelope, reservation, error);
      return;
    }
    await this.transport.ack(reservation);
  }

  private async onUnknownUrn(envelope: Envelope, urn: string, reservation: Reservation): Promise<void> {
    switch (this.unknownUrn) {
      case UnknownUrnStrategy.DELETE:
        await this.transport.ack(reservation);
        return;
      case UnknownUrnStrategy.DEAD_LETTER:
        await this.deadLetter(envelope, reservation, "unknown_urn");
        return;
      case UnknownUrnStrategy.RELEASE:
        await this.requeue(envelope, reservation);
        return;
      default: {
        // FAIL: report, then take the bounded retry/dead-letter path (Go routeUnknown / Python
        // _route_unknown parity). Leaving the reservation in place would let retryAfter recovery
        // bring it back with an unchanged attempts count: an endless reserve/recover loop.
        const error = new UnknownUrnError(urn);
        this.report(error, envelope, reservation.raw);
        await this.retryOrDeadLetter(envelope, reservation, error);
        return;
      }
    }
  }

  private async retryOrDeadLetter(envelope: Envelope, reservation: Reservation, error: unknown): Promise<void> {
    // The counted attempt: the delivered attempts plus one for this delivery (ADR-0036 decision 8).
    const counted = (envelope.attempts ?? 0) + 1;
    if (counted < this.maxTries) {
      await this.requeue(envelope, reservation);
    } else {
      // An unknown URN dead-letters with its own reason, like Go and Python.
      await this.deadLetter(
        envelope,
        reservation,
        error instanceof UnknownUrnError ? "unknown_urn" : "failed",
        error,
        counted,
      );
    }
  }

  /** Give the message back with `attempts + 1` (at-least-once retry). */
  private async requeue(envelope: Envelope, reservation: Reservation): Promise<void> {
    if (reservation.layout === "zset") {
      // Default body: the raw message with only `attempts` raised, unknown keys preserved.
      await this.transport.release(reservation, 0);
      return;
    }
    const bumped: Envelope = { ...envelope, attempts: (envelope.attempts ?? 0) + 1 };
    await this.transport.release(reservation, 0, redisValue(bumped));
  }

  /**
   * Write the annotated envelope to the DLQ. `counted` is given on the exhaustion path (retries used up): the
   * delivered attempts plus one for the current delivery, written to both `dead_letter.attempts` and the
   * record's top-level `attempts`, like Go and Python. A direct dead-letter that makes no retry decision
   * (`unknownUrn: dead_letter`) omits it and records the delivered value, which is informational.
   */
  private async deadLetter(
    envelope: Envelope,
    reservation: Reservation,
    reason: string,
    error?: unknown,
    counted?: number,
  ): Promise<void> {
    const attempts = counted ?? envelope.attempts ?? 0;
    const annotated = annotate({ ...envelope, attempts }, reason, envelope.meta?.queue ?? this.queue, {
      attempts,
      error: error instanceof Error ? error.message : null,
      exception: error instanceof Error ? error.name : null,
    });
    await this.transport.deadLetter(reservation, redisValue(annotated));
  }

  /**
   * The poison DLQ record of a reservation (ADR-0035, broker-bindings section 1.1): the delivered body (a legacy
   * header frame is unwrapped first) with a `dead_letter` block of reason `poison`. A strict JSON object is
   * spliced, anything else is wrapped as `{"raw", "dead_letter"}`.
   *
   * A body that is not valid UTF-8 is read as exact bytes (`evalBuffer`, see {@link RedisClient.evalBuffer}) and
   * kept as stored (no frame is peeled off it), so it lands in the base64 wrapper, never as U+FFFD text.
   */
  private poisonRecord(reservation: Reservation, cause: BabelQueueError): string {
    const exact = reservation.rawBytes;
    const [body] = exact !== undefined || reservation.layout !== "legacy" ? [reservation.raw] : unframe(reservation.raw);
    const counted = attemptsOf(body);
    const attempts = Number.isSafeInteger(counted) && counted >= 0 ? counted : 0;
    const deadLetter = annotate({ attempts } as unknown as Envelope, "poison", this.queue, {
      attempts,
      error: cause.message,
      exception: cause.name,
    }).dead_letter;
    return buildPoisonRecord(exact ?? Buffer.from(body, "utf8"), JSON.stringify(deadLetter));
  }

  private report(error: unknown, envelope: IncomingEnvelope | null, raw: string): void {
    this.options.onError?.(error, envelope, raw);
  }
}
