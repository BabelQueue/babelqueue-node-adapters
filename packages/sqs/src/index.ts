/**
 * Amazon SQS adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **consumer** over Amazon SQS,
 * so an SQS-based Node service speaks the same contract (envelope shape, URN
 * identity, trace propagation) as the PHP/Laravel, Python, Go, Java and .NET SDKs.
 *
 *     import { SQS } from "@aws-sdk/client-sqs";
 *     import { SqsPublisher, SqsConsumer } from "@babelqueue/sqs";
 *
 *     const sqs = new SQS({ region: "eu-central-1" });
 *     const url = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";
 *
 *     await new SqsPublisher(sqs, url).publish("urn:babel:orders:created", { order_id: 1042 });
 *
 *     const consumer = new SqsConsumer(sqs, url, {
 *       "urn:babel:orders:created": async (env) => { ... },
 *     });
 *     await consumer.run();
 *
 * This implements §3 of the broker-bindings contract: the canonical envelope is the
 * message body, projected onto native SQS `MessageAttributes`. The envelope is
 * unchanged (`schema_version` stays 1); SQS is purely additive. Retry is SQS-native
 * (a failed handler leaves the message for visibility-timeout redelivery); the
 * authoritative attempt count is `ApproximateReceiveCount`, surfaced to handlers as
 * `attempts = count − 1`.
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides as additional String `MessageAttributes` **beside** the contract `bq-*`
 * attributes where `bq-trace-id` already lives — the contract attributes win a key collision, and
 * the merged set is bounded by SQS's **10-attribute limit** (contract attributes are seeded first,
 * so a rider only lands while headroom remains). On consume the inbound `MessageAttributes` are
 * surfaced as a `Record<string,string>` so the core's `otel` extract sees the `traceparent` and
 * links the consumer span as a child. A header-less publish is byte-identical. This mirrors the Go
 * `/sqs` and PHP `SqsTransport` wiring. GR-1: the wire envelope body is never touched.
 */

import { BabelQueueError, EnvelopeCodec, UnknownUrnError } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";

/** SQS allows at most 10 user message attributes per message. */
const MAX_ATTRIBUTES = 10;

// --- Minimal SQS shapes (a structural subset of @aws-sdk/client-sqs) -----------

export interface SqsMessageAttributeValue {
  DataType: string;
  StringValue?: string;
}

export interface SqsMessage {
  Body?: string;
  ReceiptHandle?: string;
  MessageAttributes?: Record<string, SqsMessageAttributeValue>;
  Attributes?: Record<string, string>;
}

export interface SendMessageInput {
  QueueUrl: string;
  MessageBody: string;
  MessageAttributes?: Record<string, SqsMessageAttributeValue>;
  MessageGroupId?: string;
  MessageDeduplicationId?: string;
}

export interface ReceiveMessageInput {
  QueueUrl: string;
  MaxNumberOfMessages?: number;
  WaitTimeSeconds?: number;
  VisibilityTimeout?: number;
  MessageAttributeNames?: string[];
  AttributeNames?: string[];
}

/**
 * The subset of the AWS SQS client this adapter calls. The aggregated `SQS` class
 * from `@aws-sdk/client-sqs` satisfies it structurally; a fake satisfies it in tests.
 */
export interface SqsApi {
  sendMessage(input: SendMessageInput): Promise<{ MessageId?: string } | unknown>;
  receiveMessage(input: ReceiveMessageInput): Promise<{ Messages?: SqsMessage[] }>;
  deleteMessage(input: { QueueUrl: string; ReceiptHandle: string }): Promise<unknown>;
}

// --- Attribute projection (contract §3.2) --------------------------------------

const str = (value: unknown): SqsMessageAttributeValue => ({
  DataType: "String",
  StringValue: String(value),
});
const num = (value: unknown): SqsMessageAttributeValue => ({
  DataType: "Number",
  StringValue: String(value),
});

/**
 * Project the envelope's contract fields onto native SQS `MessageAttributes` — a
 * redundant, routable view of the body (the body stays authoritative).
 */
export function toMessageAttributes(envelope: Envelope): Record<string, SqsMessageAttributeValue> {
  const attrs: Record<string, SqsMessageAttributeValue> = {};
  if (envelope.job) attrs["bq-job"] = str(envelope.job);
  if (envelope.trace_id) attrs["bq-trace-id"] = str(envelope.trace_id);
  if (envelope.meta.id) attrs["bq-message-id"] = str(envelope.meta.id);
  if (envelope.meta.schema_version != null) {
    attrs["bq-schema-version"] = num(envelope.meta.schema_version);
  }
  if (envelope.meta.lang) attrs["bq-source-lang"] = str(envelope.meta.lang);
  if (envelope.meta.created_at != null) {
    attrs["bq-created-at"] = num(envelope.meta.created_at);
  }
  return attrs;
}

/**
 * Overlay the out-of-band `headers` onto the contract attribute projection as String
 * `MessageAttributes`, without overwriting an existing `bq-*` attribute (the contract wins a key
 * collision) and skipping blanks. Keys are merged in sorted order and the merge stops at the
 * 10-attribute SQS ceiling, so unbounded riders can never push the message past the limit (SQS
 * rejects the whole send otherwise) — the contract attributes are always preserved first. Mirrors
 * Go's `mergeAttributes` / PHP's `SqsTransport::attributes`.
 */
export function mergeAttributes(
  base: Record<string, SqsMessageAttributeValue>,
  headers: HeaderCarrier | null | undefined,
): Record<string, SqsMessageAttributeValue> {
  const clean = sanitizeHeaders(headers);
  for (const key of Object.keys(clean).sort()) {
    if (key in base) continue; // never clobber a contract bq-* attribute
    if (Object.keys(base).length >= MAX_ATTRIBUTES) break; // respect the SQS 10-attribute cap
    base[key] = str(clean[key]);
  }
  return base;
}

/**
 * Map inbound SQS `MessageAttributes` onto a flat {@link HeaderCarrier} (the consume-side
 * counterpart of {@link mergeAttributes}), reading each attribute's `StringValue`. Returns an empty
 * object when there are none. Both the contract `bq-*` attributes and any out-of-band rider (e.g.
 * `traceparent`) surface — the core's `otel` extract reads only the keys it knows.
 */
export function headersOf(message: SqsMessage): HeaderCarrier {
  const attrs = message.MessageAttributes;
  const out: HeaderCarrier = {};
  if (!attrs) return out;
  for (const key of Object.keys(attrs)) {
    const value = attrs[key]?.StringValue;
    if (value != null && value !== "") out[key] = value;
  }
  return out;
}

function queueNameFromUrl(queueUrl: string): string {
  const segments = queueUrl.split("/").filter(Boolean);
  return segments[segments.length - 1] ?? "default";
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link SqsPublisher}. */
export interface SqsPublisherOptions {
  /** Treat the queue as FIFO: set `MessageGroupId` and (unless content dedup) `MessageDeduplicationId`. */
  fifo?: boolean;
  /** FIFO ordering group (default: the queue name from the URL). */
  messageGroupId?: string;
  /** Use the queue's content-based dedup instead of `meta.id` as the dedup id. */
  contentDedup?: boolean;
}

/** Options for {@link SqsPublisher.publish}. */
export interface PublishOptions {
  /** Reuse an existing trace id (trace continuation). */
  traceId?: string;
  /**
   * Out-of-band transport headers carried as String `MessageAttributes` beside the contract `bq-*`
   * attributes (ADR-0028) — e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s `publish`.
   * The contract attributes win a key collision and the merged set is capped at SQS's 10-attribute
   * limit; an empty/omitted carrier leaves the publish unchanged.
   */
  headers?: HeaderCarrier;
}

/** Sends canonical-envelope messages to one SQS queue with the §3 attribute projection. */
export class SqsPublisher {
  constructor(
    private readonly client: SqsApi,
    private readonly queueUrl: string,
    private readonly options: SqsPublisherOptions = {},
  ) {}

  /**
   * Build the canonical envelope for `(urn, data)`, send it as the message body with
   * the projected `MessageAttributes`, and return the message id (`meta.id`).
   */
  async publish(
    urn: string,
    data: Record<string, unknown>,
    options: PublishOptions = {},
  ): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, {
      queue: queueNameFromUrl(this.queueUrl),
      traceId: options.traceId,
    });
    const input: SendMessageInput = {
      QueueUrl: this.queueUrl,
      MessageBody: EnvelopeCodec.encode(envelope),
      MessageAttributes: mergeAttributes(toMessageAttributes(envelope), options.headers),
    };
    if (this.options.fifo) {
      input.MessageGroupId = this.options.messageGroupId ?? queueNameFromUrl(this.queueUrl);
      if (!this.options.contentDedup) {
        input.MessageDeduplicationId = envelope.meta.id;
      }
    }
    await this.client.sendMessage(input);
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw SQS message, and the out-of-band
 * {@link HeaderCarrier} read from the message's `MessageAttributes` (empty when there are none).
 * Pass `headers` to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child of
 * the producer span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, message: SqsMessage, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link SqsConsumer}. */
export interface SqsConsumerOptions {
  /** Called instead of erroring when a message's URN has no handler (then the message is deleted). */
  onUnknownUrn?: (envelope: IncomingEnvelope, message: SqsMessage) => unknown | Promise<unknown>;
  /** Called for a non-conformant envelope, an unmapped URN (no `onUnknownUrn`), or a throwing handler. The loop never stops. */
  onError?: (error: unknown, envelope: IncomingEnvelope, message: SqsMessage) => void;
  /** Long-poll wait seconds (default 20). */
  waitTimeSeconds?: number;
  /** Reservation window applied on receive (seconds). */
  visibilityTimeout?: number;
  /** Max messages per receive (default 10). */
  maxMessages?: number;
}

/**
 * Polls an SQS queue, decodes + validates each message, routes it to the handler
 * registered for its URN, and deletes it on success. A throwing handler leaves the
 * message undeleted — SQS redelivers it after the visibility timeout (at-least-once);
 * `attempts` is reconciled to `ApproximateReceiveCount − 1` for the handler.
 */
export class SqsConsumer {
  constructor(
    private readonly client: SqsApi,
    private readonly queueUrl: string,
    private readonly handlers: BabelHandlers,
    private readonly options: SqsConsumerOptions = {},
  ) {}

  /** Receive one batch, route each message, delete the ones handled. Returns the batch size. */
  async poll(): Promise<number> {
    const input: ReceiveMessageInput = {
      QueueUrl: this.queueUrl,
      MaxNumberOfMessages: this.options.maxMessages ?? 10,
      WaitTimeSeconds: this.options.waitTimeSeconds ?? 20,
      MessageAttributeNames: ["All"],
      AttributeNames: ["ApproximateReceiveCount"],
    };
    if (this.options.visibilityTimeout != null) {
      input.VisibilityTimeout = this.options.visibilityTimeout;
    }
    const result = await this.client.receiveMessage(input);
    const messages = result.Messages ?? [];
    for (const message of messages) {
      await this.handle(message);
    }
    return messages.length;
  }

  /** Poll until `signal` aborts (each poll long-polls, so this does not busy-loop). */
  async run(signal?: AbortSignal): Promise<void> {
    while (signal?.aborted !== true) {
      await this.poll();
    }
  }

  private async handle(message: SqsMessage): Promise<void> {
    const envelope = EnvelopeCodec.decode(message.Body ?? "");

    const receiveCount = message.Attributes?.["ApproximateReceiveCount"];
    if (receiveCount !== undefined) {
      const native = Number.parseInt(receiveCount, 10) - 1;
      const current = typeof envelope.attempts === "number" ? envelope.attempts : 0;
      if (Number.isFinite(native) && native > current) {
        envelope.attempts = native;
      }
    }

    if (!EnvelopeCodec.accepts(envelope)) {
      this.options.onError?.(
        new BabelQueueError("Rejected a non-conformant BabelQueue envelope from SQS."),
        envelope,
        message,
      );
      return;
    }

    const urn = EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      if (this.options.onUnknownUrn) {
        await this.options.onUnknownUrn(envelope, message);
        await this.delete(message);
      } else {
        this.options.onError?.(new UnknownUrnError(urn), envelope, message);
      }
      return;
    }

    try {
      await handler(envelope, message, headersOf(message));
      await this.delete(message);
    } catch (error) {
      // Leave the message undeleted — SQS redelivers after the visibility timeout.
      this.options.onError?.(error, envelope, message);
    }
  }

  private async delete(message: SqsMessage): Promise<void> {
    if (!message.ReceiptHandle) return;
    await this.client.deleteMessage({
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
    });
  }
}
