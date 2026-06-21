/**
 * Azure Service Bus adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **consumer** over Azure Service
 * Bus, so an ASB-based Node service speaks the same contract (envelope shape, URN
 * identity, trace propagation) as the .NET, Java, Python and Go SDKs.
 *
 *     import { ServiceBusClient } from "@azure/service-bus";
 *     import { AsbPublisher, AsbConsumer } from "@babelqueue/azure-service-bus";
 *
 *     const client = new ServiceBusClient(connectionString);
 *     await new AsbPublisher(client.createSender("orders"))
 *       .publish("urn:babel:orders:created", { order_id: 1042 });
 *
 *     const consumer = new AsbConsumer(client.createReceiver("orders"), {
 *       "urn:babel:orders:created": async (env) => { ... },
 *     });
 *     await consumer.run();
 *
 * This implements §4 of the broker-bindings contract: the canonical envelope is the
 * message body, projected onto native Service Bus fields (`subject` = URN,
 * `correlationId` = trace_id, `messageId` = meta.id, plus the `bq-` application
 * properties). The envelope is unchanged (`schema_version` stays 1); ASB is purely
 * additive. Retry is broker-native — a failed handler `abandon`s the message, so it is
 * redelivered and `deliveryCount` is incremented; the authoritative attempt count is the
 * native `deliveryCount`, surfaced to handlers as `attempts = deliveryCount − 1`.
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides as additional native `applicationProperties` beside the contract `bq-*`
 * properties — the contract properties win a key collision (merge-not-clobber), blanks dropped. On
 * consume the inbound `applicationProperties` are surfaced as a `Record<string,string>` so the
 * core's `otel` extract sees the `traceparent` and links the consumer span as a child. A
 * header-less publish is byte-identical. GR-1: the wire envelope body is never touched.
 */

import { BabelQueueError, EnvelopeCodec, UnknownUrnError } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";

// --- Minimal Service Bus shapes (a structural subset of @azure/service-bus) -----

/** A message to send (structural subset of @azure/service-bus `ServiceBusMessage`). */
export interface AsbMessage {
  body: unknown;
  subject?: string;
  correlationId?: string;
  messageId?: string;
  contentType?: string;
  applicationProperties?: { [key: string]: number | boolean | string | Date | null };
  scheduledEnqueueTimeUtc?: Date;
}

/** A received message (structural subset of @azure/service-bus `ServiceBusReceivedMessage`). */
export interface AsbReceivedMessage {
  body: unknown;
  subject?: string;
  correlationId?: string;
  messageId?: string;
  deliveryCount?: number;
  applicationProperties?: { [key: string]: unknown };
}

/**
 * The subset of the Service Bus sender this adapter calls. A `ServiceBusSender` from
 * `@azure/service-bus` satisfies it structurally; a fake satisfies it in tests.
 */
export interface AsbSender {
  entityPath: string;
  sendMessages(message: AsbMessage): Promise<void>;
  scheduleMessages(message: AsbMessage, scheduledEnqueueTimeUtc: Date): Promise<bigint[]>;
}

/**
 * The subset of the Service Bus receiver this adapter calls. A `ServiceBusReceiver`
 * from `@azure/service-bus` satisfies it structurally; a fake satisfies it in tests.
 */
export interface AsbReceiver {
  receiveMessages(maxMessageCount: number, options?: { maxWaitTimeInMs?: number }): Promise<AsbReceivedMessage[]>;
  completeMessage(message: AsbReceivedMessage): Promise<void>;
  abandonMessage(message: AsbReceivedMessage): Promise<void>;
}

// --- Native projection (contract §4.2–§4.3) ------------------------------------

/**
 * Project the envelope's contract fields onto a native Service Bus message — `subject`
 * = URN, `correlationId` = trace_id, `messageId` = meta.id, plus the `bq-` application
 * properties. The body stays authoritative.
 *
 * When `extra` is given, those out-of-band headers (e.g. a W3C `traceparent`, ADR-0028) are merged
 * into `applicationProperties` **first**, then the contract `bq-*` properties are written so they
 * always win a key collision (merge-not-clobber); blank keys/values are dropped. GR-1: the wire
 * envelope body is never touched.
 */
export function toServiceBusMessage(envelope: Envelope, extra?: HeaderCarrier | null): AsbMessage {
  const applicationProperties: { [key: string]: number | boolean | string } = {};
  // Out-of-band riders go in first so the contract bq-* properties below win any key collision.
  for (const [key, value] of Object.entries(sanitizeHeaders(extra))) applicationProperties[key] = value;
  if (envelope.meta.schema_version != null) {
    applicationProperties["bq-schema-version"] = envelope.meta.schema_version;
  }
  if (envelope.meta.lang) applicationProperties["bq-source-lang"] = envelope.meta.lang;
  if (envelope.meta.created_at != null) {
    applicationProperties["bq-created-at"] = envelope.meta.created_at;
  }

  const message: AsbMessage = {
    body: EnvelopeCodec.encode(envelope),
    contentType: "application/json",
  };
  if (envelope.job) message.subject = envelope.job;
  if (envelope.trace_id) message.correlationId = envelope.trace_id;
  if (envelope.meta.id) message.messageId = envelope.meta.id;
  if (Object.keys(applicationProperties).length > 0) {
    message.applicationProperties = applicationProperties;
  }
  return message;
}

/**
 * Read a received message's `applicationProperties` back into a flat {@link HeaderCarrier},
 * stringifying values defensively. Returns an empty object when there are none. Both the contract
 * `bq-*` properties and any out-of-band rider (e.g. `traceparent`) surface — the core's `otel`
 * extract reads only the keys it knows.
 */
export function headersOf(message: AsbReceivedMessage): HeaderCarrier {
  const props = message.applicationProperties;
  const out: HeaderCarrier = {};
  if (!props) return out;
  for (const key of Object.keys(props)) {
    const value = props[key];
    if (value == null) continue;
    const s = String(value);
    if (s !== "") out[key] = s;
  }
  return out;
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link AsbPublisher.publish}. */
export interface PublishOptions {
  /** Reuse an existing trace id (trace continuation). */
  traceId?: string;
  /** Schedule native delayed delivery this many milliseconds from now (`scheduledEnqueueTimeUtc`). */
  delayMs?: number;
  /**
   * Out-of-band transport headers carried as native `applicationProperties` beside the contract
   * `bq-*` properties (ADR-0028) — e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s
   * `publish`. The contract properties win a key collision; an empty/omitted carrier leaves the
   * message unchanged.
   */
  headers?: HeaderCarrier;
}

/** Sends canonical-envelope messages to one Service Bus entity with the §4 native projection. */
export class AsbPublisher {
  constructor(private readonly sender: AsbSender) {}

  /**
   * Build the canonical envelope for `(urn, data)`, send it with the native projection,
   * and return the message id (`meta.id`). A positive `delayMs` schedules native delayed
   * delivery.
   */
  async publish(
    urn: string,
    data: Record<string, unknown>,
    options: PublishOptions = {},
  ): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, {
      queue: this.sender.entityPath,
      traceId: options.traceId,
    });
    const message = toServiceBusMessage(envelope, options.headers);

    if (options.delayMs != null && options.delayMs > 0) {
      message.applicationProperties = { ...message.applicationProperties, "bq-delay": options.delayMs };
      await this.sender.scheduleMessages(message, new Date(Date.now() + options.delayMs));
    } else {
      await this.sender.sendMessages(message);
    }
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw Service Bus message, and the out-of-band
 * {@link HeaderCarrier} read from the message's `applicationProperties` (empty when there are none).
 * Pass `headers` to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child of
 * the producer span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, message: AsbReceivedMessage, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link AsbConsumer}. */
export interface AsbConsumerOptions {
  /** Called instead of erroring when a message's URN has no handler (then the message is completed). */
  onUnknownUrn?: (envelope: IncomingEnvelope, message: AsbReceivedMessage) => unknown | Promise<unknown>;
  /** Called for a non-conformant envelope, an unmapped URN (no `onUnknownUrn`), or a throwing handler. The loop never stops. */
  onError?: (error: unknown, envelope: IncomingEnvelope, message: AsbReceivedMessage) => void;
  /** Max messages per receive (default 10). */
  maxMessages?: number;
  /** Max time to wait for a batch, in ms. */
  maxWaitTimeInMs?: number;
}

/**
 * Receives from a Service Bus entity (PeekLock), decodes + validates each message, routes
 * it to the handler for its URN, and completes it on success. A throwing handler abandons
 * the message — the broker redelivers it and increments `deliveryCount` (at-least-once);
 * `attempts` is reconciled to `deliveryCount − 1` for the handler.
 */
export class AsbConsumer {
  constructor(
    private readonly receiver: AsbReceiver,
    private readonly handlers: BabelHandlers,
    private readonly options: AsbConsumerOptions = {},
  ) {}

  /** Receive one batch, route each message, settle each. Returns the batch size. */
  async poll(): Promise<number> {
    const messages = await this.receiver.receiveMessages(
      this.options.maxMessages ?? 10,
      this.options.maxWaitTimeInMs != null ? { maxWaitTimeInMs: this.options.maxWaitTimeInMs } : undefined,
    );
    for (const message of messages) {
      await this.handle(message);
    }
    return messages.length;
  }

  /** Poll until `signal` aborts. */
  async run(signal?: AbortSignal): Promise<void> {
    while (signal?.aborted !== true) {
      await this.poll();
    }
  }

  private async handle(message: AsbReceivedMessage): Promise<void> {
    const raw = typeof message.body === "string" ? message.body : String(message.body ?? "");
    const envelope = EnvelopeCodec.decode(raw);

    // attempts = max(current, deliveryCount − 1): deliveryCount (1-based) is the native
    // redelivery floor; the max never lowers a higher body count republished from another SDK.
    const deliveryCount = typeof message.deliveryCount === "number" ? message.deliveryCount : 0;
    const native = deliveryCount - 1;
    const current = typeof envelope.attempts === "number" ? envelope.attempts : 0;
    if (native > current) {
      envelope.attempts = native;
    }

    if (!EnvelopeCodec.accepts(envelope)) {
      this.options.onError?.(
        new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Azure Service Bus."),
        envelope,
        message,
      );
      await this.receiver.abandonMessage(message);
      return;
    }

    const urn = EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      if (this.options.onUnknownUrn) {
        await this.options.onUnknownUrn(envelope, message);
        await this.receiver.completeMessage(message);
      } else {
        this.options.onError?.(new UnknownUrnError(urn), envelope, message);
        await this.receiver.abandonMessage(message);
      }
      return;
    }

    try {
      await handler(envelope, message, headersOf(message));
      await this.receiver.completeMessage(message);
    } catch (error) {
      // Abandon releases the lock — the broker redelivers and increments deliveryCount.
      this.options.onError?.(error, envelope, message);
      await this.receiver.abandonMessage(message);
    }
  }
}
