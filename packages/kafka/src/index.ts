/**
 * Apache Kafka adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed, **process-then-commit** consumer over
 * Apache Kafka (KafkaJS), so a Kafka-based Node service speaks the same contract (envelope
 * shape, URN identity, trace propagation) as the .NET, Java, Python and Go SDKs.
 *
 * Kafka has **no native** per-message ack, delayed delivery, dead-letter queue, or delivery
 * counter — this adapter absorbs all four in the binding layer (the envelope stays
 * `schema_version: 1`): the envelope JSON is the record **value**; the contract fields are
 * mirrored onto `bq-` headers (route on `bq-job` without decoding the body) and the record
 * timestamp mirrors `meta.created_at`; **`bq-attempts` is the authoritative retry counter**;
 * consume is process-then-commit (manual commit, at-least-once); retry/delay use SDK-owned
 * tiered retry topics `<topic>.retry.<n>`; terminal failures go to an opt-in `<topic>.dlq`.
 *
 * This implements §6 of the broker-bindings contract. `kafkajs` is an optional peer — you
 * provide the producer/consumer (a KafkaJS `Producer`/`Consumer` satisfies the adapter
 * structurally).
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides as additional Kafka **record headers** beside the contract `bq-*` headers —
 * the contract headers win a key collision (merge-not-clobber), and blanks are dropped. On consume
 * the inbound record headers are surfaced as a `Record<string,string>` so the core's `otel` extract
 * sees the `traceparent` and links the consumer span as a child. A header-less publish is
 * byte-identical. GR-1: the wire envelope (the record value) is never touched.
 *
 * **Version gate (§6.7).** Before decoding the record value the consumer reads the `bq-schema-version`
 * header. A missing or blank value (empty or only ASCII whitespace) follows the normal decode path;
 * any other value is compared, untrimmed, to the supported `"1"`, and a record that does not match is
 * rejected undecoded down the poison path — reported to `onError`, the raw record (value, key and
 * headers) forwarded to the DLQ (when configured) and the offset committed.
 */

import { annotate, BabelQueueError, EnvelopeCodec, SCHEMA_VERSION, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { mergeInto } from "./headers.js";

// --- Minimal Kafka shapes (a structural subset of kafkajs) ---------------------

/** Incoming record headers (KafkaJS decodes header values to Buffers). */
export type IncomingHeaders = { [key: string]: Buffer | string | (Buffer | string)[] | null | undefined };

/** A record to produce (structural subset of a KafkaJS message). */
export interface KafkaProducerMessage {
  key?: Buffer | string | null;
  value: Buffer | string;
  /**
   * Header values are strings for records the adapter builds. Raw DLQ forwarding (gate rejection,
   * decode error, non-conformant envelope) instead passes the inbound values through as
   * `Buffer | Buffer[]` (KafkaJS `IHeaders`); a custom producer wrapper must tolerate both.
   */
  headers?: { [key: string]: string };
  timestamp?: string;
}

/** The subset of the KafkaJS producer this adapter calls. */
export interface KafkaProducerClient {
  send(record: { topic: string; messages: KafkaProducerMessage[] }): Promise<unknown>;
}

/** A received record (structural subset of a KafkaJS message). */
export interface KafkaIncomingMessage {
  value: Buffer | null;
  headers?: IncomingHeaders;
  offset: string;
  timestamp?: string;
  key?: Buffer | null;
}

/** The payload KafkaJS passes to `eachMessage`. */
export interface EachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaIncomingMessage;
}

/** The subset of the KafkaJS consumer this adapter calls (manual commit — `autoCommit: false`). */
export interface KafkaConsumerClient {
  run(config: { autoCommit?: boolean; eachMessage: (payload: EachMessagePayload) => Promise<void> }): Promise<void>;
  commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void>;
}

/** The subset of the KafkaJS consumer {@link KafkaRedriveIO} calls to rewind a record (`consumer.seek`). */
export interface KafkaSeekClient {
  seek(position: { topic: string; partition: number; offset: string }): void | Promise<void>;
}

// --- Header projection (contract §6.3) -----------------------------------------

/** Project the envelope's contract fields onto Kafka record headers (UTF-8 string values). */
export function kafkaHeaders(envelope: Envelope): { [key: string]: string } {
  const headers: { [key: string]: string } = {};
  if (envelope.job) headers["bq-job"] = envelope.job;
  if (envelope.trace_id) headers["bq-trace-id"] = envelope.trace_id;
  if (envelope.meta.id) headers["bq-message-id"] = envelope.meta.id;
  if (envelope.meta.schema_version != null) headers["bq-schema-version"] = String(envelope.meta.schema_version);
  if (envelope.meta.lang) headers["bq-source-lang"] = envelope.meta.lang;
  headers["bq-attempts"] = String(envelope.attempts ?? 0);
  return headers;
}

/**
 * Copy inbound record headers for a raw DLQ forward: every value becomes a `Buffer` copy (a string is
 * UTF-8 encoded), array-valued headers keep all their entries, and `undefined` / `null` values are dropped.
 */
function copyHeaders(headers: IncomingHeaders | undefined): { [key: string]: Buffer | Buffer[] } {
  const out: { [key: string]: Buffer | Buffer[] } = {};
  if (!headers) return out;
  const copy = (v: Buffer | string): Buffer => (Buffer.isBuffer(v) ? Buffer.from(v) : Buffer.from(v, "utf8"));
  for (const [name, value] of Object.entries(headers)) {
    if (value === undefined || value === null) continue;
    if (Array.isArray(value)) {
      out[name] = value.filter((v) => v !== undefined && v !== null).map(copy);
    } else {
      out[name] = copy(value);
    }
  }
  return out;
}

function headerString(headers: IncomingHeaders | undefined, key: string): string | undefined {
  const value = headers?.[key];
  // A null header value is "no header" (broker-bindings.md §6.7): never stringified to "null".
  if (value === undefined || value === null) return undefined;
  const v = Array.isArray(value) ? value[0] : value;
  if (v === undefined || v === null) return undefined;
  return Buffer.isBuffer(v) ? v.toString("utf8") : String(v);
}

function headerInt(headers: IncomingHeaders | undefined, key: string, fallback: number): number {
  const s = headerString(headers, key);
  if (s === undefined) return fallback;
  const n = Number.parseInt(s.trim(), 10);
  return Number.isNaN(n) ? fallback : n;
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

/** Blank = empty or only ASCII whitespace (GR-5: the same set in every SDK, never `String.trim()`). */
const BLANK_ASCII = /^[ \t\n\v\f\r]*$/;

/**
 * The inbound `bq-schema-version` header when it is present, non-blank and not exactly the supported
 * version (compared untrimmed), else `undefined` (the record may be decoded).
 */
function unsupportedSchemaVersion(message: KafkaIncomingMessage): string | undefined {
  const value = headerString(message.headers, "bq-schema-version");
  if (value === undefined || BLANK_ASCII.test(value)) return undefined;
  return value === String(SCHEMA_VERSION) ? undefined : value;
}

/**
 * Map an inbound record's headers onto a flat {@link HeaderCarrier}, stringifying KafkaJS's Buffer
 * values. Returns an empty object when there are none. Both the contract `bq-*` headers and any
 * out-of-band rider (e.g. `traceparent`) surface — the core's `otel` extract reads only the keys it
 * knows.
 */
export function headersOf(message: KafkaIncomingMessage): HeaderCarrier {
  const headers = message.headers;
  const out: HeaderCarrier = {};
  if (!headers) return out;
  for (const key of Object.keys(headers)) {
    const value = headerString(headers, key);
    if (value !== undefined && value !== "") out[key] = value;
  }
  return out;
}

// --- Retry / delay topology (contract §6.4–§6.5) -------------------------------

/** A single delay tier: the `<topic>.retry.<n>` topic and the delay (ms) it holds for. */
export interface Tier {
  topic: string;
  delayMs: number;
}

/**
 * The SDK-owned retry/delay topology for one work topic: tiered delay topics
 * `<topic>.retry.<n>` (ascending) plus an opt-in `<topic>.dlq`. A delay or release with no
 * tiers configured raises rather than silently dropping.
 */
export class RetryTopics {
  readonly tiers: Tier[];
  readonly dlqTopic: string | null;

  constructor(
    readonly workTopic: string,
    delaysMs: number[] = [],
    dlqTopic: string | null | undefined = undefined,
  ) {
    this.tiers = [...delaysMs]
      .sort((a, b) => a - b)
      .map((delayMs, i) => ({ topic: `${workTopic}.retry.${i + 1}`, delayMs }));
    this.dlqTopic = dlqTopic === undefined ? `${workTopic}.dlq` : dlqTopic;
  }

  hasTiers(): boolean {
    return this.tiers.length > 0;
  }

  /** The smallest tier whose delay ≥ `delayMs`; raises if none configured or the delay is too large. */
  tierForDelay(delayMs: number): Tier {
    this.requireTiers();
    for (const tier of this.tiers) {
      if (tier.delayMs >= delayMs) return tier;
    }
    throw new BabelQueueError(
      `Requested Kafka delay ${delayMs}ms exceeds the largest retry tier (${this.tiers[this.tiers.length - 1]!.delayMs}ms).`,
    );
  }

  /** The tier for a retry at `attempt` (0-based), clamped to the largest; raises if none configured. */
  tierForAttempt(attempt: number): Tier {
    this.requireTiers();
    return this.tiers[Math.min(Math.max(attempt, 0), this.tiers.length - 1)]!;
  }

  private requireTiers(): void {
    if (!this.tiers.length) {
      throw new BabelQueueError(`Kafka retry/delay requires retry topics; none are configured for '${this.workTopic}'.`);
    }
  }
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link KafkaPublisher.publish}. */
export interface PublishOptions {
  traceId?: string;
  /** Schedule via a retry tier (requires a {@link RetryTopics}); raises on a plain publisher. */
  delayMs?: number;
  /**
   * Out-of-band transport headers carried as Kafka record headers beside the contract `bq-*`
   * headers (ADR-0028) — e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s `publish`.
   * The contract headers win a key collision; an empty/omitted carrier leaves the record unchanged.
   */
  headers?: HeaderCarrier;
}

/** Sends canonical-envelope messages to one Kafka work topic with the §6 projection. */
export class KafkaPublisher {
  private constructor(
    private readonly producer: KafkaProducerClient,
    private readonly workTopic: string,
    private readonly retryTopics?: RetryTopics,
  ) {}

  /** A publisher onto `topic` with no retry topics (a delay raises). */
  static create(producer: KafkaProducerClient, topic: string): KafkaPublisher {
    return new KafkaPublisher(producer, topic);
  }

  /** A publisher onto the topology's work topic, with delay routed via its retry tiers. */
  static withRetryTopics(producer: KafkaProducerClient, retryTopics: RetryTopics): KafkaPublisher {
    return new KafkaPublisher(producer, retryTopics.workTopic, retryTopics);
  }

  /** Build + send the canonical envelope; returns the message id (`meta.id`). */
  async publish(urn: string, data: Record<string, unknown>, options: PublishOptions = {}): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, { queue: this.workTopic, traceId: options.traceId });
    if (options.delayMs != null && options.delayMs > 0) {
      if (!this.retryTopics) {
        throw new BabelQueueError("Kafka has no native delayed delivery; a delay requires retry topics (none configured).");
      }
      const tier = this.retryTopics.tierForDelay(options.delayMs);
      await this.sendRecord(tier.topic, envelope, options.headers, options.delayMs, this.workTopic);
    } else {
      await this.sendRecord(this.workTopic, envelope, options.headers);
    }
    return envelope.meta.id;
  }

  private async sendRecord(
    topic: string,
    envelope: Envelope,
    extra?: HeaderCarrier | null,
    delayMs?: number,
    originalTopic?: string,
  ): Promise<void> {
    // The contract bq-* headers are seeded first, then the out-of-band riders are merged beside
    // them without clobbering (the contract wins a collision).
    const headers = mergeInto(kafkaHeaders(envelope), extra);
    if (delayMs != null) headers["bq-delay"] = String(delayMs);
    if (originalTopic != null) headers["bq-original-topic"] = originalTopic;
    await this.producer.send({
      topic,
      messages: [{ value: EnvelopeCodec.encode(envelope), headers, timestamp: String(envelope.meta.created_at) }],
    });
  }
}

// --- DLQ redrive IO (ADR-0026, §6.5 "Record key on republish") -------------------

/**
 * A DLQ record reserved for {@link KafkaRedriveIO}: the structural shape `@babelqueue/core`'s
 * `RedriveMessage` expects (`body` + `ack`), plus the consumed record's `key` — `null`/absent for a
 * keyless record, a (possibly empty) `Buffer` otherwise.
 */
export interface KafkaRedriveMessage {
  body: string;
  key?: Buffer | null;
  /** The record's position, needed by {@link KafkaRedriveIO.rewind}; the offset is a KafkaJS string. */
  topic?: string;
  partition?: number;
  offset?: string;
  ack(): Promise<void>;
}

/** Options for {@link KafkaRedriveIO}. */
export interface KafkaRedriveIOOptions {
  /** The producer used for every write the redrive makes (redrive write + restore to the DLQ). */
  producer: KafkaProducerClient;
  /**
   * Reserve the next record from `queue` (the DLQ topic), or `null` when it is empty. Kafka consumption
   * is push-based in KafkaJS, so the caller supplies the pull (a bounded fetch with manual commit) and
   * its `ack` commits the offset. Return the record's `key` so the redrive can carry it.
   */
  pop(queue: string): Promise<KafkaRedriveMessage | null>;
  /**
   * The consumer the DLQ records are fetched with. When given, {@link KafkaRedriveIO.rewind} is offered:
   * when the run stops (a failed restore write, a failed redrive write, a throwing `select` or a failed
   * `ack`) it seeks this consumer back to the first unacknowledged record of every affected partition, so
   * a later (cumulative) offset commit cannot skip a record that was not safely handled. Without it the IO
   * has no `rewind` and a failed restore only leaves the record unacknowledged.
   *
   * The IO only knows the records it handed out. If {@link KafkaRedriveIOOptions.pop} reads ahead (buffers
   * records it has not returned yet), the caller must discard that buffer after a rewind and must not
   * reuse the IO: e.g. `pop` buffered p0@5..7, the run stopped at p0@6 and the seek went back to p0@5; a
   * buffered p0@7 returned and acknowledged afterwards would commit offset 8 and skip 5 and 6.
   */
  consumer?: KafkaSeekClient;
}

/**
 * A `@babelqueue/core` `RedriveIO` over Kafka that carries the DLQ record key byte-for-byte through the
 * redrive (contract §6.5): the redrive write back to the work topic and the restore write back to the
 * DLQ both go through {@link KafkaRedriveIO.publishFrom}, which copies the reserved record's key — a null
 * key stays null (no key on the record), an empty key stays empty — and never synthesises one. Partition
 * equality is not guaranteed (client partitioners differ). The §6.3 header projection is rebuilt from
 * the body being written (`bq-attempts` therefore reads 0 for a reset envelope); a body that is not a
 * canonical envelope (an undecodable record being restored) is written verbatim with no `bq-*` headers.
 *
 * Plain {@link KafkaRedriveIO.publish} writes a keyless record. `publishWithHeaders` is not offered, so
 * `bypass` is a no-op on this transport (as in the Go reference's keyed path).
 *
 * When `options.consumer` is supplied the IO also offers {@link KafkaRedriveIO.rewind} (core's optional
 * `RedriveIO.rewind`): if the restore write of a record back to the DLQ fails, the consumer is sought
 * back, because a Kafka offset commit is cumulative and a later commit would otherwise skip a record
 * for good. Core's `redrive()` pops the whole batch before processing it, so the consumer position is
 * already past every record handed out and not yet acknowledged; the IO tracks those and seeks every
 * affected partition back to its first unacknowledged record, not only the failed one.
 */
export class KafkaRedriveIO {
  /** Seek the consumer back to every unacknowledged record (`from` included unless it was acknowledged already); defined only when a `consumer` was supplied. */
  readonly rewind?: (from: KafkaRedriveMessage) => Promise<void>;

  /** Records handed out by {@link KafkaRedriveIO.pop} that carry a position and are not acknowledged yet. */
  private readonly inFlight = new Set<KafkaRedriveMessage>();

  /** Records handed out by {@link KafkaRedriveIO.pop} that were acknowledged since; a rewind never re-reads them. */
  private readonly acknowledged = new WeakSet<KafkaRedriveMessage>();

  constructor(private readonly options: KafkaRedriveIOOptions) {
    const consumer = options.consumer;
    if (consumer) {
      this.rewind = async (from: KafkaRedriveMessage): Promise<void> => {
        if (from.topic === undefined || from.partition === undefined || from.offset === undefined) {
          throw new Error("kafka redrive: cannot rewind a record without topic, partition and offset");
        }
        // First (lowest) unacknowledged offset per partition: everything still in flight, plus `from` unless it
        // was acknowledged already (a record restored and acked after a failed redrive write is committed past,
        // and seeking back to it would redeliver a copy).
        const pending = new Set<KafkaRedriveMessage>(this.inFlight);
        if (!this.acknowledged.has(from)) pending.add(from);
        const first = new Map<string, { topic: string; partition: number; offset: bigint }>();
        for (const m of pending) {
          if (m.topic === undefined || m.partition === undefined || m.offset === undefined) continue;
          const key = `${m.topic}\u0000${m.partition}`;
          const offset = BigInt(m.offset);
          const seen = first.get(key);
          if (!seen || offset < seen.offset) first.set(key, { topic: m.topic, partition: m.partition, offset });
        }
        this.inFlight.clear();
        const failures: unknown[] = [];
        for (const { topic, partition, offset } of first.values()) {
          try {
            await consumer.seek({ topic, partition, offset: offset.toString() });
          } catch (err) {
            failures.push(err); // keep rewinding the other partitions
          }
        }
        if (failures.length === 1) throw failures[0];
        if (failures.length > 1) throw new AggregateError(failures, "kafka redrive: seeking several partitions back failed");
      };
    }
  }

  async pop(queue: string): Promise<KafkaRedriveMessage | null> {
    const message = await this.options.pop(queue);
    if (message && this.rewind && message.topic !== undefined && message.partition !== undefined && message.offset !== undefined) {
      // Hand out a copy and leave the caller's message untouched (it may be frozen or expose accessors). The
      // fields are read one by one: a spread copies own enumerable properties only, so a class instance with
      // prototype getters (`get body()`) would lose them and its DLQ body would be restored as an empty record.
      // The copy forgets the record once it is acknowledged: a rewind must never re-read a committed record.
      const tracked: KafkaRedriveMessage = {
        body: message.body,
        key: message.key,
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        ack: async (): Promise<void> => {
          await message.ack();
          this.inFlight.delete(tracked);
          this.acknowledged.add(tracked);
        },
      };
      this.inFlight.add(tracked);
      return tracked;
    }
    return message;
  }

  /** Append an already-encoded body to `queue` as a keyless record. */
  async publish(queue: string, body: string): Promise<void> {
    await this.send(queue, body, null);
  }

  /** Append `body` to `queue`, carrying the key of the reserved record `from` byte-for-byte. */
  async publishFrom(queue: string, body: string, from: KafkaRedriveMessage): Promise<void> {
    await this.send(queue, body, from.key ?? null);
  }

  private async send(queue: string, body: string, key: Buffer | null): Promise<void> {
    const record: KafkaProducerMessage = { value: body };
    const decoded = EnvelopeCodec.decode(body);
    if (EnvelopeCodec.accepts(decoded)) {
      record.headers = kafkaHeaders(decoded);
      record.timestamp = String(decoded.meta.created_at);
    }
    if (key !== null) record.key = Buffer.from(key);
    await this.options.producer.send({ topic: queue, messages: [record] });
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw Kafka message, and the out-of-band
 * {@link HeaderCarrier} read from the record headers (empty when there are none). Pass `headers` to
 * `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child of the producer span
 * (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, message: KafkaIncomingMessage, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link KafkaConsumer}. */
export interface KafkaConsumerOptions {
  /** The producer used to republish retry/DLQ records (required for retry/DLQ). */
  producer?: KafkaProducerClient;
  /** The retry/DLQ topology; enables per-record retry, delay, and dead-lettering. */
  retryTopics?: RetryTopics;
  /** Attempts before terminal dead-lettering (default 3). */
  maxTries?: number;
  /** Strategy for a URN with no handler (default `fail`). */
  unknownUrn?: string;
  /** Called for a poison record, an unmapped URN, or a throwing handler. */
  onError?: (error: unknown, envelope: IncomingEnvelope | null, payload: EachMessagePayload) => void;
}

/**
 * Consumes a Kafka work topic in process-then-commit mode: each record is decoded, validated,
 * routed to the handler for its URN (read from `bq-job`), and its offset committed only after
 * the handler returns. A throwing handler republishes to a `<topic>.retry.<n>` tier with
 * `bq-attempts + 1`, then commits; terminal failures go to `<topic>.dlq` with a `dead_letter`
 * block. The `bq-attempts` header is the authoritative counter (the body is the fallback).
 */
export class KafkaConsumer {
  private readonly maxTries: number;
  private readonly unknownUrn: string;

  constructor(
    private readonly consumer: KafkaConsumerClient,
    private readonly handlers: BabelHandlers,
    private readonly options: KafkaConsumerOptions = {},
  ) {
    this.maxTries = options.maxTries ?? 3;
    this.unknownUrn = options.unknownUrn ?? UnknownUrnStrategy.FAIL;
  }

  /** Start the manual-commit consume loop (`autoCommit: false`); resolves when the consumer stops. */
  async run(): Promise<void> {
    await this.consumer.run({ autoCommit: false, eachMessage: (payload) => this.handle(payload) });
  }

  /** Route + settle one record (process-then-commit). Exposed for testing. */
  async handle(payload: EachMessagePayload): Promise<void> {
    const { message } = payload;

    // Version gate (§6.7): decided on the header alone, before the value is decoded.
    const unsupported = unsupportedSchemaVersion(message);
    if (unsupported !== undefined) {
      const rejected = new BabelQueueError(
        `Rejected a record from Kafka with unsupported bq-schema-version "${unsupported}" (supported: ${SCHEMA_VERSION}).`,
      );
      try {
        await this.deadLetterRaw(payload);
        await this.commit(payload);
      } finally {
        this.report(rejected, null, payload);
      }
      return;
    }

    const raw = message.value ? message.value.toString("utf8") : "";

    let envelope: IncomingEnvelope;
    try {
      envelope = this.reconcile(EnvelopeCodec.decode(raw), message);
    } catch (decodeError) {
      try {
        await this.deadLetterRaw(payload);
        await this.commit(payload);
      } finally {
        this.report(decodeError, null, payload);
      }
      return;
    }

    if (!EnvelopeCodec.accepts(envelope)) {
      // A non-conformant / poison envelope may be missing the fields needed to annotate, so
      // forward the raw record to the DLQ rather than building a dead_letter block.
      const rejected = new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Kafka.");
      try {
        await this.deadLetterRaw(payload);
        await this.commit(payload);
      } finally {
        this.report(rejected, envelope, payload);
      }
      return;
    }

    const urn = headerString(message.headers, "bq-job") ?? EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      await this.onUnknownUrn(payload, envelope, urn);
      return;
    }

    try {
      await handler(envelope, message, headersOf(message));
      await this.commit(payload);
    } catch (error) {
      try {
        await this.retryOrDeadLetter(payload, envelope as Envelope, error);
        await this.commit(payload);
      } finally {
        this.report(error, envelope, payload);
      }
    }
  }

  private reconcile(envelope: IncomingEnvelope, message: KafkaIncomingMessage): IncomingEnvelope {
    const body = typeof envelope.attempts === "number" ? envelope.attempts : 0;
    return { ...envelope, attempts: headerInt(message.headers, "bq-attempts", body) };
  }

  private async onUnknownUrn(payload: EachMessagePayload, envelope: IncomingEnvelope, urn: string): Promise<void> {
    switch (this.unknownUrn) {
      case UnknownUrnStrategy.DELETE:
        await this.commit(payload);
        return;
      case UnknownUrnStrategy.DEAD_LETTER:
        await this.deadLetter(envelope as Envelope, payload, "unknown_urn");
        await this.commit(payload);
        return;
      case UnknownUrnStrategy.RELEASE:
        await this.republishRetry(payload, envelope as Envelope);
        await this.commit(payload);
        return;
      default: {
        // FAIL: surface and do NOT commit — the record redelivers on the next poll.
        const error = new UnknownUrnError(urn);
        this.report(error, envelope, payload);
        throw error;
      }
    }
  }

  private async retryOrDeadLetter(payload: EachMessagePayload, envelope: Envelope, error: unknown): Promise<void> {
    const hasTiers = this.options.retryTopics?.hasTiers() ?? false;
    const hasDlq = (this.options.retryTopics?.dlqTopic ?? null) !== null;
    if (!hasTiers && !hasDlq) {
      void error; // reported via onError afterwards; Kafka has nowhere to put the failure
      throw new BabelQueueError("Kafka per-record retry requires retry topics and/or a DLQ; neither is configured.");
    }
    if (hasTiers && (envelope.attempts ?? 0) + 1 < this.maxTries) {
      await this.republishRetry(payload, envelope);
    } else {
      await this.deadLetter(envelope, payload, "failed", error);
    }
  }

  private async republishRetry(payload: EachMessagePayload, envelope: Envelope): Promise<void> {
    const topics = this.options.retryTopics;
    if (!topics || !topics.hasTiers()) {
      throw new BabelQueueError("Kafka retry/release requires retry topics; none are configured.");
    }
    const tier = topics.tierForAttempt(envelope.attempts ?? 0);
    const bumped: Envelope = { ...envelope, attempts: (envelope.attempts ?? 0) + 1 };
    const headers = kafkaHeaders(bumped);
    headers["bq-delay"] = String(tier.delayMs);
    headers["bq-original-topic"] = this.originalTopic(payload);
    await this.produce(tier.topic, bumped, headers, payload.message.key);
  }

  private async deadLetter(envelope: Envelope, payload: EachMessagePayload, reason: string, error?: unknown): Promise<void> {
    const dlq = this.options.retryTopics?.dlqTopic ?? null;
    if (!dlq) return; // dead-lettering disabled → degrade to commit-and-drop
    const original = this.originalTopic(payload);
    const annotated = annotate(envelope, reason, original, {
      attempts: envelope.attempts ?? 0,
      error: error instanceof Error ? error.message : null,
      exception: error instanceof Error ? error.name : null,
    });
    const headers = kafkaHeaders(annotated);
    headers["bq-original-topic"] = original;
    await this.produce(dlq, annotated, headers, payload.message.key);
  }

  private async deadLetterRaw(payload: EachMessagePayload): Promise<void> {
    const dlq = this.options.retryTopics?.dlqTopic ?? null;
    if (!dlq) return;
    this.requireProducer();
    const { message } = payload;
    // Forward the original record verbatim (value, key, headers) so the DLQ copy keeps its provenance
    // (Java reference). Only the timestamp is re-stamped.
    const forwarded: KafkaProducerMessage = {
      value: message.value ?? Buffer.alloc(0),
      // KafkaJS accepts Buffer / array header values; the published structural type stays string-only.
      headers: copyHeaders(message.headers) as unknown as KafkaProducerMessage["headers"],
      timestamp: String(Date.now()),
    };
    if (message.key !== undefined && message.key !== null) forwarded.key = Buffer.from(message.key);
    await this.options.producer!.send({ topic: dlq, messages: [forwarded] });
  }

  /**
   * Republish an envelope. The consumed record's key is carried over byte-for-byte (an empty key stays
   * empty); a null/absent key is left off the record, never synthesised. Partition equality is not
   * guaranteed.
   */
  private async produce(
    topic: string,
    envelope: Envelope,
    headers: { [key: string]: string },
    key?: Buffer | null,
  ): Promise<void> {
    this.requireProducer();
    const record: KafkaProducerMessage = { value: EnvelopeCodec.encode(envelope), headers, timestamp: String(Date.now()) };
    if (key !== undefined && key !== null) record.key = Buffer.from(key);
    await this.options.producer!.send({ topic, messages: [record] });
  }

  private async commit(payload: EachMessagePayload): Promise<void> {
    await this.consumer.commitOffsets([
      { topic: payload.topic, partition: payload.partition, offset: (Number(payload.message.offset) + 1).toString() },
    ]);
  }

  private originalTopic(payload: EachMessagePayload): string {
    return headerString(payload.message.headers, "bq-original-topic") ?? payload.topic;
  }

  private requireProducer(): void {
    if (!this.options.producer) {
      throw new BabelQueueError("This Kafka consumer needs a producer to republish (retry/DLQ).");
    }
  }

  /**
   * Hand a failure to the user's `onError` callback. Callers invoke this only AFTER the record has been
   * settled (dead-lettered / retried and committed). A callback that throws synchronously, or returns a
   * promise that later rejects (an `async` function is assignable to the `void` callback type), is
   * swallowed here so user code can never bypass settlement or stop the consumer.
   */
  private report(error: unknown, envelope: IncomingEnvelope | null, payload: EachMessagePayload): void {
    try {
      const returned: unknown = this.options.onError?.(error, envelope, payload);
      if (isThenable(returned)) {
        // An async callback's rejection would otherwise surface as an unhandled rejection.
        returned.then(undefined, () => undefined);
      }
    } catch {
      // user callback failure must not escape the consume loop
    }
  }
}
