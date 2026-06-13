/**
 * Apache ActiveMQ Artemis adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **AMQP 1.0** consumer over Artemis (rhea),
 * so an Artemis-based Node service speaks the same contract (envelope shape, URN identity, trace
 * propagation) as the Java, .NET, Python and Go SDKs.
 *
 * Artemis speaks AMQP 1.0 (not RabbitMQ's 0-9-1) and gives the binding native primitives —
 * per-message settlement, scheduled delivery, a delivery counter and a dead-letter address — so
 * this adapter maps onto them (the envelope stays `schema_version: 1`): the envelope JSON is the
 * message **body**; the contract fields are mirrored onto the AMQP a JMS peer reads
 * (`correlation-id` = `trace_id`, `creation-time` = `meta.created_at`, the `x-opt-jms-type`
 * annotation = URN) plus the `bq_` application properties; consume settles per message
 * (`accept` after success, `release` for redelivery); **`attempts = max(body, delivery-count)`**
 * (the AMQP counter is 0-based — no −1); terminal failures go to an opt-in `<queue>.dlq` with a
 * `dead_letter` block.
 *
 * This implements §7 of the broker-bindings contract. `rhea` is an optional peer — you provide
 * the sender/receiver (a rhea sender/receiver satisfies the adapter structurally).
 */

import { annotate, BabelQueueError, EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, IncomingEnvelope } from "@babelqueue/core";

// --- Minimal AMQP 1.0 shapes (a structural subset of rhea) ---------------------

/** An AMQP message (structural subset of a rhea message). */
export interface AmqpMessage {
  body?: unknown;
  correlation_id?: unknown;
  creation_time?: number | Date;
  content_type?: string;
  application_properties?: Record<string, unknown>;
  message_annotations?: Record<string, unknown>;
  /** The AMQP header delivery-count (0-based: 0 on first delivery). */
  delivery_count?: number;
}

/** The disposition handle rhea passes alongside a received message. */
export interface AmqpDelivery {
  /** Settle (acknowledge) the message — it is removed from the queue. */
  accept(): void;
  /** Return the message for redelivery — the broker increments `delivery-count`. */
  release(params?: { delivery_failed?: boolean; undeliverable_here?: boolean }): void;
}

/** The event context rhea emits on a `message` event. */
export interface AmqpEventContext {
  message?: AmqpMessage;
  delivery?: AmqpDelivery;
}

/** The subset of a rhea sender this adapter calls. */
export interface AmqpSender {
  send(message: AmqpMessage): unknown;
}

/** The subset of a rhea receiver this adapter wires to (its `message` event). */
export interface AmqpReceiver {
  on(event: "message", handler: (context: AmqpEventContext) => void): unknown;
}

// --- Projection (contract §7.2) ------------------------------------------------

/** The message annotation carrying the URN (the AMQP-JMS mapping of `JMSType`). */
export const JMS_TYPE_KEY = "x-opt-jms-type";
/** The message annotation Artemis honours for AMQP scheduled delivery (absolute Unix ms). */
export const SCHEDULED_DELIVERY_KEY = "x-opt-delivery-time";

/**
 * Project the envelope onto the AMQP 1.0 message a JMS peer reads: body = envelope JSON,
 * `correlation-id` = `trace_id`, `creation-time` = `meta.created_at`, the `x-opt-jms-type`
 * annotation = URN, plus the string-valued `bq_` application properties. A positive `delayMs`
 * sets the `x-opt-delivery-time` annotation for native scheduled delivery.
 */
export function artemisMessage(envelope: Envelope, delayMs?: number): AmqpMessage {
  const annotations: Record<string, unknown> = {};
  if (envelope.job) annotations[JMS_TYPE_KEY] = envelope.job;

  // The bq_ property names use underscores, not hyphens: a JMS property name must be a valid
  // Java identifier, and every Artemis SDK uses the same JMS-legal form for cross-protocol parity.
  const applicationProperties: Record<string, unknown> = {
    "bq_schema_version": String(envelope.meta.schema_version),
    "bq_attempts": String(envelope.attempts ?? 0),
    "bq_app_id": "babelqueue",
  };
  if (envelope.meta.lang) applicationProperties["bq_source_lang"] = envelope.meta.lang;

  const message: AmqpMessage = {
    body: EnvelopeCodec.encode(envelope),
    content_type: "application/json",
    application_properties: applicationProperties,
    message_annotations: annotations,
  };
  if (envelope.trace_id) message.correlation_id = envelope.trace_id;
  if (envelope.meta.created_at != null) message.creation_time = envelope.meta.created_at;

  if (delayMs != null && delayMs > 0) {
    applicationProperties["bq_delay"] = String(delayMs);
    annotations[SCHEDULED_DELIVERY_KEY] = Date.now() + delayMs;
  }
  return message;
}

/** The message body as text (AMQP value string, or UTF-8-decoded binary / rhea Data section). */
export function messageBody(message: AmqpMessage): string {
  const body = message.body;
  if (body == null) return "";
  if (typeof body === "string") return body;
  if (Buffer.isBuffer(body)) return body.toString("utf8");
  if (typeof body === "object" && "content" in body) {
    const content = (body as { content: unknown }).content;
    if (Buffer.isBuffer(content)) return content.toString("utf8");
    if (typeof content === "string") return content;
  }
  return String(body);
}

function jmsType(message: AmqpMessage): string | undefined {
  const value = message.message_annotations?.[JMS_TYPE_KEY];
  return value == null ? undefined : String(value);
}

function deliveryCount(message: AmqpMessage): number {
  const value = message.delivery_count;
  return typeof value === "number" && value > 0 ? value : 0;
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link ArtemisPublisher.publish}. */
export interface PublishOptions {
  traceId?: string;
  /** Schedule via Artemis's native AMQP scheduled delivery (`x-opt-delivery-time`). */
  delayMs?: number;
}

/** Sends canonical-envelope messages to one Artemis address with the §7 projection. */
export class ArtemisPublisher {
  private constructor(
    private readonly sender: AmqpSender,
    private readonly queue: string,
  ) {}

  /** A publisher over a sender bound to `queue` (the address is stamped onto `meta.queue`). */
  static create(sender: AmqpSender, queue: string): ArtemisPublisher {
    return new ArtemisPublisher(sender, queue);
  }

  /** Build + send the canonical envelope; returns the message id (`meta.id`). */
  async publish(urn: string, data: Record<string, unknown>, options: PublishOptions = {}): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, { queue: this.queue, traceId: options.traceId });
    await this.sender.send(artemisMessage(envelope, options.delayMs));
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/** A URN handler. Receives the validated envelope and the raw AMQP message. */
export type BabelHandler = (envelope: Envelope, message: AmqpMessage) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link ArtemisConsumer}. */
export interface ArtemisConsumerOptions {
  /** A sender to `<queue>.dlq`; enables cross-language dead-lettering. Without it a terminal failure drops. */
  deadLetterSender?: AmqpSender;
  /** Attempts before terminal dead-lettering (default 3). */
  maxTries?: number;
  /** Strategy for a URN with no handler (default `fail`). */
  unknownUrn?: string;
  /** Called for a non-conformant message, an unmapped URN, or a throwing handler. */
  onError?: (error: unknown, envelope: IncomingEnvelope | null, context: AmqpEventContext) => void;
}

/**
 * Consumes an Artemis address over AMQP 1.0: each message is decoded, validated, routed to the
 * handler for its URN (read from the `x-opt-jms-type` annotation), and `accept`ed on success. A
 * throwing handler `release`s the message so the broker redelivers it (incrementing the AMQP
 * `delivery-count`); once max-tries is reached the envelope goes to `<queue>.dlq` with a
 * `dead_letter` block. `attempts` is reconciled to `max(body, delivery-count)` — the AMQP
 * counter is 0-based, so no −1.
 */
export class ArtemisConsumer {
  private readonly maxTries: number;
  private readonly unknownUrn: string;

  constructor(
    private readonly handlers: BabelHandlers,
    private readonly options: ArtemisConsumerOptions = {},
  ) {
    this.maxTries = options.maxTries ?? 3;
    this.unknownUrn = options.unknownUrn ?? UnknownUrnStrategy.FAIL;
  }

  /** Wire this consumer to a rhea receiver's `message` event (auto-accept must be off). */
  listen(receiver: AmqpReceiver): void {
    receiver.on("message", (context) => this.handle(context));
  }

  /** Route + settle one delivery. Exposed for testing. */
  async handle(context: AmqpEventContext): Promise<void> {
    const message = context.message ?? {};
    const delivery = context.delivery;
    const decoded = EnvelopeCodec.decode(messageBody(message));

    if (!EnvelopeCodec.accepts(decoded)) {
      // A non-conformant / poison message may lack the fields to annotate; forward it raw.
      this.report(new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Artemis."), decoded, context);
      await this.deadLetterRaw(messageBody(message));
      delivery?.accept();
      return;
    }

    const envelope = this.reconcile(decoded, message);
    const urn = jmsType(message) ?? EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      await this.onUnknownUrn(context, envelope, urn);
      return;
    }

    try {
      await handler(envelope, message);
      delivery?.accept();
    } catch (error) {
      this.report(error, envelope, context);
      await this.retryOrDeadLetter(context, envelope, error);
    }
  }

  /** `attempts = max(body, delivery-count)`: the AMQP counter is 0-based, so no −1. */
  private reconcile(envelope: IncomingEnvelope, message: AmqpMessage): Envelope {
    const body = typeof envelope.attempts === "number" ? envelope.attempts : 0;
    const count = deliveryCount(message);
    return { ...envelope, attempts: Math.max(body, count) } as Envelope;
  }

  private async onUnknownUrn(context: AmqpEventContext, envelope: Envelope, urn: string): Promise<void> {
    const delivery = context.delivery;
    switch (this.unknownUrn) {
      case UnknownUrnStrategy.DELETE:
        delivery?.accept();
        return;
      case UnknownUrnStrategy.DEAD_LETTER:
        await this.deadLetter(envelope, "unknown_urn");
        delivery?.accept();
        return;
      case UnknownUrnStrategy.RELEASE:
        delivery?.release({ delivery_failed: true });
        return;
      default: {
        // FAIL: surface and do NOT settle — the broker redelivers, then dead-letters.
        const error = new UnknownUrnError(urn);
        this.report(error, envelope, context);
        throw error;
      }
    }
  }

  private async retryOrDeadLetter(context: AmqpEventContext, envelope: Envelope, error: unknown): Promise<void> {
    const delivery = context.delivery;
    if ((envelope.attempts ?? 0) + 1 < this.maxTries) {
      // Release with delivery-failed leaves it for redelivery — the broker bumps delivery-count.
      delivery?.release({ delivery_failed: true });
    } else if (this.options.deadLetterSender) {
      await this.deadLetter(envelope, "failed", error);
      delivery?.accept();
    } else {
      delivery?.accept(); // terminal, no DLQ → drop
    }
  }

  private async deadLetter(envelope: Envelope, reason: string, error?: unknown): Promise<void> {
    const sender = this.options.deadLetterSender;
    if (!sender) return;
    const original = envelope.meta?.queue ?? "";
    const annotated = annotate(envelope, reason, original, {
      attempts: envelope.attempts ?? 0,
      error: error instanceof Error ? error.message : null,
      exception: error instanceof Error ? error.name : null,
    });
    await sender.send(artemisMessage(annotated));
  }

  private async deadLetterRaw(raw: string): Promise<void> {
    const sender = this.options.deadLetterSender;
    if (!sender) return;
    await sender.send({ body: raw });
  }

  private report(error: unknown, envelope: IncomingEnvelope | null, context: AmqpEventContext): void {
    this.options.onError?.(error, envelope, context);
  }
}
