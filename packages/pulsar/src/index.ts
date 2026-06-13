/**
 * Apache Pulsar adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **consumer** over Apache Pulsar, so a
 * Pulsar-based Node service speaks the same contract (envelope shape, URN identity, trace
 * propagation) as the .NET, Java, Python and Go SDKs.
 *
 *     import Pulsar from "pulsar-client";
 *     import { PulsarPublisher, PulsarConsumer } from "@babelqueue/pulsar";
 *
 *     const client = new Pulsar.Client({ serviceUrl: "pulsar://localhost:6650" });
 *
 *     const producer = await client.createProducer({ topic: "orders" });
 *     await new PulsarPublisher(producer).publish("urn:babel:orders:created", { order_id: 1042 });
 *
 *     const sub = await client.subscribe({
 *       topic: "orders", subscription: "babelqueue", subscriptionType: "Shared",
 *     });
 *     const consumer = new PulsarConsumer(sub, {
 *       "urn:babel:orders:created": async (env) => { ... },
 *     });
 *     await consumer.run();
 *
 * This implements §5 of the broker-bindings contract: the canonical envelope is the message
 * payload, projected onto native Pulsar message properties (string→string) — `bq-job` = URN,
 * `bq-trace-id` = trace_id, `bq-message-id` = meta.id, plus `bq-schema-version` /
 * `bq-source-lang` / `bq-attempts`. The envelope is unchanged (`schema_version` stays 1);
 * Pulsar is purely additive. A failed handler `negativeAcknowledge`s the message, so it is
 * redelivered (at-least-once) and `getRedeliveryCount()` is incremented; the authoritative
 * attempt count is the body's `bq-attempts`, reconciled to `max(bq-attempts,
 * getRedeliveryCount())` — no −1, because Pulsar's redelivery count is 0-based.
 */

import { BabelQueueError, EnvelopeCodec, UnknownUrnError } from "@babelqueue/core";
import type { Envelope, IncomingEnvelope } from "@babelqueue/core";

// --- Minimal Pulsar shapes (a structural subset of pulsar-client) --------------

/** A message to send (structural subset of pulsar-client `ProducerMessage`). */
export interface PulsarProducerMessage {
  data: Buffer;
  properties?: { [key: string]: string };
  /** Relative delay in milliseconds before the message becomes available (native `deliverAfter`). */
  deliverAfter?: number;
}

/**
 * The subset of the Pulsar producer this adapter calls. A `Producer` from `pulsar-client`
 * satisfies it structurally; a fake satisfies it in tests.
 */
export interface PulsarProducer {
  getTopic(): string;
  send(message: PulsarProducerMessage): Promise<unknown>;
}

/** A received message (structural subset of pulsar-client `Message`). */
export interface PulsarReceivedMessage {
  getData(): Buffer | string;
  getProperties(): { [key: string]: string };
  getRedeliveryCount(): number;
}

/**
 * The subset of the Pulsar consumer this adapter calls. A `Consumer` from `pulsar-client`
 * satisfies it structurally; a fake satisfies it in tests.
 */
export interface PulsarConsumerClient {
  receive(timeoutMs?: number): Promise<PulsarReceivedMessage>;
  acknowledge(message: PulsarReceivedMessage): Promise<void> | void;
  negativeAcknowledge(message: PulsarReceivedMessage): Promise<void> | void;
}

// --- Property projection (contract §5.2) ---------------------------------------

/** Bare topic name (the part after the last `/`), used as `meta.queue`. */
function topicName(topic: string): string {
  const slash = topic.lastIndexOf("/");
  return slash >= 0 ? topic.slice(slash + 1) : topic;
}

/**
 * Project the envelope's contract fields onto native Pulsar message properties
 * (string→string): `bq-job` = URN, `bq-trace-id` = trace_id, `bq-message-id` = meta.id, plus
 * `bq-schema-version` / `bq-source-lang` / `bq-attempts`. The body stays authoritative.
 */
export function pulsarProperties(envelope: Envelope): { [key: string]: string } {
  const properties: { [key: string]: string } = {};
  if (envelope.job) properties["bq-job"] = envelope.job;
  if (envelope.trace_id) properties["bq-trace-id"] = envelope.trace_id;
  if (envelope.meta.id) properties["bq-message-id"] = envelope.meta.id;
  if (envelope.meta.schema_version != null) {
    properties["bq-schema-version"] = String(envelope.meta.schema_version);
  }
  if (envelope.meta.lang) properties["bq-source-lang"] = envelope.meta.lang;
  properties["bq-attempts"] = String(envelope.attempts ?? 0);
  return properties;
}

/** Project the envelope onto a native Pulsar producer message (payload + properties). */
export function toPulsarMessage(envelope: Envelope): PulsarProducerMessage {
  return {
    data: Buffer.from(EnvelopeCodec.encode(envelope), "utf8"),
    properties: pulsarProperties(envelope),
  };
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link PulsarPublisher.publish}. */
export interface PublishOptions {
  /** Reuse an existing trace id (trace continuation). */
  traceId?: string;
  /** Schedule native delayed delivery this many milliseconds from now (`deliverAfter`). */
  delayMs?: number;
}

/** Sends canonical-envelope messages to one Pulsar topic with the §5 property projection. */
export class PulsarPublisher {
  constructor(private readonly producer: PulsarProducer) {}

  /**
   * Build the canonical envelope for `(urn, data)`, send it with the §5 property projection,
   * and return the message id (`meta.id`). A positive `delayMs` schedules native delayed
   * delivery via `deliverAfter` and mirrors `bq-delay`.
   */
  async publish(
    urn: string,
    data: Record<string, unknown>,
    options: PublishOptions = {},
  ): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, {
      queue: topicName(this.producer.getTopic()),
      traceId: options.traceId,
    });
    const message = toPulsarMessage(envelope);

    if (options.delayMs != null && options.delayMs > 0) {
      message.properties = { ...message.properties, "bq-delay": String(options.delayMs) };
      message.deliverAfter = options.delayMs;
    }

    await this.producer.send(message);
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/** A URN handler. Receives the validated envelope and the raw Pulsar message. */
export type BabelHandler = (envelope: Envelope, message: PulsarReceivedMessage) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link PulsarConsumer}. */
export interface PulsarConsumerOptions {
  /** Called instead of erroring when a message's URN has no handler (then the message is acknowledged). */
  onUnknownUrn?: (envelope: IncomingEnvelope, message: PulsarReceivedMessage) => unknown | Promise<unknown>;
  /** Called for a non-conformant envelope, an unmapped URN (no `onUnknownUrn`), or a throwing handler. The loop never stops. */
  onError?: (error: unknown, envelope: IncomingEnvelope, message: PulsarReceivedMessage) => void;
  /** Per-receive timeout in ms (default 1000). A timeout yields an empty poll, not an error. */
  receiveTimeoutMs?: number;
}

function isReceiveTimeout(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /timeout/i.test(message);
}

function decodePayload(data: Buffer | string): string {
  return typeof data === "string" ? data : data.toString("utf8");
}

/**
 * Receives from a Pulsar subscription, decodes + validates each message, routes it to the
 * handler for its URN, and acknowledges it on success. A throwing handler
 * `negativeAcknowledge`s the message — the broker redelivers it and increments
 * `getRedeliveryCount()` (at-least-once); `attempts` is reconciled to
 * `max(bq-attempts, getRedeliveryCount())` for the handler.
 */
export class PulsarConsumer {
  constructor(
    private readonly consumer: PulsarConsumerClient,
    private readonly handlers: BabelHandlers,
    private readonly options: PulsarConsumerOptions = {},
  ) {}

  /** Receive one message (up to the receive timeout), route + settle it. Returns 1, or 0 on timeout. */
  async poll(): Promise<number> {
    let message: PulsarReceivedMessage;
    try {
      message = await this.consumer.receive(this.options.receiveTimeoutMs ?? 1000);
    } catch (error) {
      if (isReceiveTimeout(error)) return 0;
      throw error;
    }
    await this.handle(message);
    return 1;
  }

  /** Poll until `signal` aborts. */
  async run(signal?: AbortSignal): Promise<void> {
    while (signal?.aborted !== true) {
      await this.poll();
    }
  }

  private async handle(message: PulsarReceivedMessage): Promise<void> {
    const envelope = EnvelopeCodec.decode(decodePayload(message.getData()));

    // attempts = max(current, getRedeliveryCount()): the redelivery count is 0-based, so it
    // maps directly with no −1; the max never lowers a higher body count republished from
    // another SDK (the Go/Python transports retry by re-sending with attempts+1).
    const redeliveryCount = typeof message.getRedeliveryCount === "function" ? message.getRedeliveryCount() : 0;
    const current = typeof envelope.attempts === "number" ? envelope.attempts : 0;
    if (redeliveryCount > current) {
      envelope.attempts = redeliveryCount;
    }

    if (!EnvelopeCodec.accepts(envelope)) {
      this.options.onError?.(
        new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Apache Pulsar."),
        envelope,
        message,
      );
      await this.consumer.negativeAcknowledge(message);
      return;
    }

    const urn = EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      if (this.options.onUnknownUrn) {
        await this.options.onUnknownUrn(envelope, message);
        await this.consumer.acknowledge(message);
      } else {
        this.options.onError?.(new UnknownUrnError(urn), envelope, message);
        await this.consumer.negativeAcknowledge(message);
      }
      return;
    }

    try {
      await handler(envelope as Envelope, message);
      await this.consumer.acknowledge(message);
    } catch (error) {
      // Negative-ack releases the message — the broker redelivers and increments the count.
      this.options.onError?.(error, envelope, message);
      await this.consumer.negativeAcknowledge(message);
    }
  }
}
