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
 */

import { annotate, BabelQueueError, EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, IncomingEnvelope } from "@babelqueue/core";

// --- Minimal Kafka shapes (a structural subset of kafkajs) ---------------------

/** Incoming record headers (KafkaJS decodes header values to Buffers). */
export type IncomingHeaders = { [key: string]: Buffer | string | (Buffer | string)[] | undefined };

/** A record to produce (structural subset of a KafkaJS message). */
export interface KafkaProducerMessage {
  key?: Buffer | string | null;
  value: Buffer | string;
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

function headerString(headers: IncomingHeaders | undefined, key: string): string | undefined {
  const value = headers?.[key];
  if (value === undefined) return undefined;
  const v = Array.isArray(value) ? value[0] : value;
  if (v === undefined) return undefined;
  return Buffer.isBuffer(v) ? v.toString("utf8") : String(v);
}

function headerInt(headers: IncomingHeaders | undefined, key: string, fallback: number): number {
  const s = headerString(headers, key);
  if (s === undefined) return fallback;
  const n = Number.parseInt(s.trim(), 10);
  return Number.isNaN(n) ? fallback : n;
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
      await this.sendRecord(tier.topic, envelope, options.delayMs, this.workTopic);
    } else {
      await this.sendRecord(this.workTopic, envelope);
    }
    return envelope.meta.id;
  }

  private async sendRecord(topic: string, envelope: Envelope, delayMs?: number, originalTopic?: string): Promise<void> {
    const headers = kafkaHeaders(envelope);
    if (delayMs != null) headers["bq-delay"] = String(delayMs);
    if (originalTopic != null) headers["bq-original-topic"] = originalTopic;
    await this.producer.send({
      topic,
      messages: [{ value: EnvelopeCodec.encode(envelope), headers, timestamp: String(envelope.meta.created_at) }],
    });
  }
}

// --- Consumer ------------------------------------------------------------------

/** A URN handler. Receives the validated envelope and the raw Kafka message. */
export type BabelHandler = (envelope: Envelope, message: KafkaIncomingMessage) => unknown | Promise<unknown>;

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
    const raw = message.value ? message.value.toString("utf8") : "";

    let envelope: IncomingEnvelope;
    try {
      envelope = this.reconcile(EnvelopeCodec.decode(raw), message);
    } catch (decodeError) {
      this.report(decodeError, null, payload);
      await this.deadLetterRaw(payload);
      await this.commit(payload);
      return;
    }

    if (!EnvelopeCodec.accepts(envelope)) {
      // A non-conformant / poison envelope may be missing the fields needed to annotate, so
      // forward the raw record to the DLQ rather than building a dead_letter block.
      this.report(new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Kafka."), envelope, payload);
      await this.deadLetterRaw(payload);
      await this.commit(payload);
      return;
    }

    const urn = headerString(message.headers, "bq-job") ?? EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      await this.onUnknownUrn(payload, envelope, urn);
      return;
    }

    try {
      await handler(envelope, message);
      await this.commit(payload);
    } catch (error) {
      this.report(error, envelope, payload);
      await this.retryOrDeadLetter(payload, envelope as Envelope, error);
      await this.commit(payload);
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
      void error; // reported via onError before this; Kafka has nowhere to put the failure
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
    await this.produce(tier.topic, bumped, headers);
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
    await this.produce(dlq, annotated, headers);
  }

  private async deadLetterRaw(payload: EachMessagePayload): Promise<void> {
    const dlq = this.options.retryTopics?.dlqTopic ?? null;
    if (!dlq) return;
    this.requireProducer();
    const { message } = payload;
    await this.options.producer!.send({
      topic: dlq,
      messages: [{ value: message.value ?? Buffer.alloc(0), timestamp: String(Date.now()) }],
    });
  }

  private async produce(topic: string, envelope: Envelope, headers: { [key: string]: string }): Promise<void> {
    this.requireProducer();
    await this.options.producer!.send({
      topic,
      messages: [{ value: EnvelopeCodec.encode(envelope), headers, timestamp: String(Date.now()) }],
    });
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

  private report(error: unknown, envelope: IncomingEnvelope | null, payload: EachMessagePayload): void {
    this.options.onError?.(error, envelope, payload);
  }
}
