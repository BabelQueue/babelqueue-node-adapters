/**
 * RabbitMQ adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **consumer** over RabbitMQ (AMQP 0-9-1,
 * amqplib), so a RabbitMQ-based Node service speaks the same wire contract as the PHP, Python and
 * Go SDKs. Implements §2 of the broker-bindings contract.
 *
 * The envelope JSON is the message body; the contract fields are projected onto native AMQP 0-9-1
 * properties so a consumer routes without decoding the body: `type` = URN, `correlation_id` =
 * `trace_id`, `message_id` = `meta.id`, `app_id` = `babelqueue`, plus the native-typed
 * `x-schema-version` / `x-source-lang` / `x-attempts` headers (AMQP 0-9-1 field-tables carry typed
 * values — integers stay integers). Consume is `basic.get` + manual ack (at-least-once); attempts
 * live in the body and the runtime owns retry (republish with `attempts + 1`).
 *
 * `amqplib` is an optional peer — you provide the channel (an amqplib `Channel` satisfies the
 * adapter structurally).
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides in the native AMQP message header table (`properties.headers`) **beside** the
 * contract `x-*` headers — the contract headers are written last so they always win a key collision
 * (merge-not-clobber), and blank keys/values are dropped. On consume the inbound `properties.headers`
 * are surfaced as a `Record<string,string>` so the core's `otel` extract sees the `traceparent` and
 * links the consumer span as a child of the producer span. A header-less publish stays byte-identical.
 * This mirrors the Go `/amqp` and PHP `AmqpTransport` wiring. GR-1: the wire envelope is untouched.
 */

import { annotate, BabelQueueError, EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";

// --- Minimal AMQP 0-9-1 shapes (a structural subset of amqplib) ----------------

/** Publish/message options (a structural subset of amqplib's `Options.Publish` + message props). */
export interface AmqpProperties {
  contentType?: string;
  contentEncoding?: string;
  persistent?: boolean;
  messageId?: string;
  correlationId?: string;
  type?: string;
  appId?: string;
  headers?: Record<string, unknown>;
}

/** A received message (structural subset of amqplib's `GetMessage`). */
export interface AmqpMessage {
  content: Buffer | Uint8Array;
  properties?: AmqpProperties;
  fields?: { deliveryTag?: number };
}

/** The subset of an amqplib `Channel` this adapter calls. */
export interface AmqpChannel {
  sendToQueue(queue: string, content: Buffer, options?: AmqpProperties): boolean;
  get(queue: string, options?: { noAck?: boolean }): Promise<AmqpMessage | false>;
  ack(message: AmqpMessage): void;
}

// --- Projection (contract §2.2–§2.3) -------------------------------------------

/**
 * Project the envelope onto native AMQP 0-9-1 properties + headers (§2.2–§2.3). When `extra` is
 * given, those out-of-band headers (e.g. a `traceparent`, ADR-0028) are merged into the header
 * table **first**, then the contract `x-*` headers are written last so they always win a key
 * collision (merge-not-clobber); blank keys/values are dropped.
 */
export function amqpProperties(envelope: Envelope, extra?: HeaderCarrier | null): AmqpProperties {
  const headers: Record<string, unknown> = {};
  // Out-of-band riders go in first so the contract x-* headers below overwrite any collision.
  for (const [key, value] of Object.entries(sanitizeHeaders(extra))) headers[key] = value;
  headers["x-attempts"] = envelope.attempts ?? 0;
  if (envelope.meta.schema_version != null) headers["x-schema-version"] = envelope.meta.schema_version;
  if (envelope.meta.lang) headers["x-source-lang"] = envelope.meta.lang;

  const properties: AmqpProperties = {
    contentType: "application/json",
    contentEncoding: "utf-8",
    persistent: true,
    appId: "babelqueue",
    headers,
  };
  if (envelope.job) properties.type = envelope.job;
  if (envelope.trace_id) properties.correlationId = envelope.trace_id;
  if (envelope.meta.id) properties.messageId = envelope.meta.id;
  return properties;
}

function messageBody(message: AmqpMessage): string {
  const content = message.content;
  if (Buffer.isBuffer(content)) return content.toString("utf8");
  return Buffer.from(content).toString("utf8");
}

/**
 * Map an inbound AMQP header table onto a flat {@link HeaderCarrier}, stringifying values
 * defensively (AMQP field-tables are typed: strings, ints, byte buffers…). Returns an empty object
 * when there are no headers, so a header-less delivery surfaces no headers. Both the contract `x-*`
 * headers and any out-of-band rider (e.g. `traceparent`) surface — the core's `otel` extract reads
 * only the keys it knows.
 */
export function headersOf(message: AmqpMessage): HeaderCarrier {
  const table = message.properties?.headers;
  const out: HeaderCarrier = {};
  if (!table) return out;
  for (const key of Object.keys(table)) {
    const value = table[key];
    if (value == null) continue;
    const s = Buffer.isBuffer(value) ? value.toString("utf8") : String(value);
    if (s !== "") out[key] = s;
  }
  return out;
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link RabbitMQPublisher.publish}. */
export interface PublishOptions {
  traceId?: string;
  /**
   * Out-of-band transport headers carried in the AMQP header table beside the contract `x-*`
   * headers (ADR-0028) — e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s `publish`.
   * The contract headers win a key collision; an empty/omitted carrier leaves the publish unchanged.
   */
  headers?: HeaderCarrier;
}

/** Sends canonical-envelope messages to one RabbitMQ queue with the §2 projection. */
export class RabbitMQPublisher {
  private constructor(
    private readonly channel: AmqpChannel,
    private readonly queue: string,
  ) {}

  /** A publisher over `channel`, producing to `queue` (the default exchange, routing key = queue). */
  static create(channel: AmqpChannel, queue: string): RabbitMQPublisher {
    return new RabbitMQPublisher(channel, queue);
  }

  /** Build + publish the canonical envelope; returns the message id (`meta.id`). */
  async publish(urn: string, data: Record<string, unknown>, options: PublishOptions = {}): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, { queue: this.queue, traceId: options.traceId });
    this.channel.sendToQueue(
      this.queue,
      Buffer.from(EnvelopeCodec.encode(envelope), "utf8"),
      amqpProperties(envelope, options.headers),
    );
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw AMQP message, and the out-of-band
 * {@link HeaderCarrier} read from the delivery's header table (empty when there are none). Pass
 * `headers` to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child of the
 * producer span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, message: AmqpMessage, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link RabbitMQConsumer}. */
export interface RabbitMQConsumerOptions {
  /** Attempts before terminal dead-lettering (default 3). */
  maxTries?: number;
  /** The dead-letter queue (default `<queue>.dlq`); set to `null` to drop on terminal failure. */
  deadLetterQueue?: string | null;
  /** Strategy for a URN with no handler (default `fail`). */
  unknownUrn?: string;
  /** Called for a non-conformant message, an unmapped URN, or a throwing handler. */
  onError?: (error: unknown, envelope: IncomingEnvelope | null, message: AmqpMessage) => void;
}

/**
 * Consumes a RabbitMQ queue with `basic.get` + manual ack: decode + validate, route to the handler
 * for its URN (read from `properties.type`, falling back to the body URN), then `ack`. A throwing
 * handler republishes the envelope with `attempts + 1` (at-least-once) up to `maxTries`, then
 * dead-letters to `<queue>.dlq`. RabbitMQ has no native delivery count for this transport, so
 * `attempts` lives in the body.
 */
export class RabbitMQConsumer {
  private readonly maxTries: number;
  private readonly dlq: string | null;
  private readonly unknownUrn: string;

  constructor(
    private readonly channel: AmqpChannel,
    private readonly queue: string,
    private readonly handlers: BabelHandlers,
    private readonly options: RabbitMQConsumerOptions = {},
  ) {
    this.maxTries = options.maxTries ?? 3;
    this.dlq = options.deadLetterQueue === undefined ? `${queue}.dlq` : options.deadLetterQueue;
    this.unknownUrn = options.unknownUrn ?? UnknownUrnStrategy.FAIL;
  }

  /** Reserve + route + settle one message. Returns true if one was handled, false when empty. */
  async poll(): Promise<boolean> {
    const message = await this.channel.get(this.queue, { noAck: false });
    if (!message) return false;
    await this.handle(message);
    return true;
  }

  /** Poll while `shouldContinue` returns true. */
  async run(shouldContinue: () => boolean): Promise<void> {
    while (shouldContinue()) {
      await this.poll();
    }
  }

  /** Route + settle one message. Exposed for testing. */
  async handle(message: AmqpMessage): Promise<void> {
    const raw = messageBody(message);
    const decoded = EnvelopeCodec.decode(raw);

    if (!EnvelopeCodec.accepts(decoded)) {
      this.report(new BabelQueueError("Rejected a non-conformant BabelQueue envelope from RabbitMQ."), decoded, message);
      await this.deadLetterRaw(raw);
      this.channel.ack(message);
      return;
    }

    const envelope = decoded as Envelope;
    const urn = (typeof message.properties?.type === "string" && message.properties.type) || EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      await this.onUnknownUrn(message, envelope, urn);
      return;
    }

    try {
      await handler(envelope, message, headersOf(message));
      this.channel.ack(message);
    } catch (error) {
      this.report(error, envelope, message);
      await this.retryOrDeadLetter(envelope, error);
      this.channel.ack(message);
    }
  }

  private async onUnknownUrn(message: AmqpMessage, envelope: Envelope, urn: string): Promise<void> {
    switch (this.unknownUrn) {
      case UnknownUrnStrategy.DELETE:
        this.channel.ack(message);
        return;
      case UnknownUrnStrategy.DEAD_LETTER:
        await this.deadLetter(envelope, "unknown_urn");
        this.channel.ack(message);
        return;
      case UnknownUrnStrategy.RELEASE:
        await this.requeue(envelope);
        this.channel.ack(message);
        return;
      default: {
        // FAIL: surface and do NOT ack — the unacked delivery is redelivered on channel recovery.
        const error = new UnknownUrnError(urn);
        this.report(error, envelope, message);
        throw error;
      }
    }
  }

  private async retryOrDeadLetter(envelope: Envelope, error: unknown): Promise<void> {
    if ((envelope.attempts ?? 0) + 1 < this.maxTries) {
      await this.requeue(envelope);
    } else {
      await this.deadLetter(envelope, "failed", error);
    }
  }

  /** Republish with `attempts + 1` onto the work queue (at-least-once retry). */
  private async requeue(envelope: Envelope): Promise<void> {
    const bumped: Envelope = { ...envelope, attempts: (envelope.attempts ?? 0) + 1 };
    this.channel.sendToQueue(this.queue, Buffer.from(EnvelopeCodec.encode(bumped), "utf8"), amqpProperties(bumped));
  }

  private async deadLetter(envelope: Envelope, reason: string, error?: unknown): Promise<void> {
    if (!this.dlq) return;
    const annotated = annotate(envelope, reason, envelope.meta?.queue ?? this.queue, {
      attempts: envelope.attempts ?? 0,
      error: error instanceof Error ? error.message : null,
      exception: error instanceof Error ? error.name : null,
    });
    this.channel.sendToQueue(this.dlq, Buffer.from(EnvelopeCodec.encode(annotated), "utf8"), amqpProperties(annotated));
  }

  private async deadLetterRaw(raw: string): Promise<void> {
    if (!this.dlq) return;
    this.channel.sendToQueue(this.dlq, Buffer.from(raw, "utf8"));
  }

  private report(error: unknown, envelope: IncomingEnvelope | null, message: AmqpMessage): void {
    this.options.onError?.(error, envelope, message);
  }
}
