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
 * (a failed handler's message is released via `ChangeMessageVisibility` after
 * `releaseDelay`, default 0 s — pair the queue with a `RedrivePolicy`); the
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

import { BabelQueueError, EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy, annotate } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";

/** SQS allows at most 10 user message attributes per message. */
const MAX_ATTRIBUTES = 10;

/** The SQS ceiling for a `VisibilityTimeout` (12 hours, in seconds). */
export const MAX_VISIBILITY_TIMEOUT = 43_200;

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
  /**
   * The §3.5 release primitive. Optional so existing structural clients keep compiling; it is
   * required (checked at construction) when a consumer sets `releaseDelay` or the `release`
   * unknown-URN strategy explicitly. Without it the default handler-failure release degrades to
   * leaving the message for visibility-timeout redelivery. The aggregated `SQS` class from
   * `@aws-sdk/client-sqs` provides it.
   */
  changeMessageVisibility?(input: ChangeMessageVisibilityInput): Promise<unknown>;
}

export interface ChangeMessageVisibilityInput {
  QueueUrl: string;
  ReceiptHandle: string;
  VisibilityTimeout: number;
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
  /**
   * Called when a message's URN has no handler. Without `unknownUrn` the message is then deleted
   * (the original behaviour); with `unknownUrn` set it is a notification hook and the strategy
   * decides the message's fate.
   */
  onUnknownUrn?: (envelope: IncomingEnvelope, message: SqsMessage) => unknown | Promise<unknown>;
  /**
   * Unknown-URN strategy per contract §3.5 — `fail` | `delete` | `release` | `dead_letter`
   * ({@link UnknownUrnStrategy}). Omitted keeps the original behaviour (`onUnknownUrn` → delete,
   * otherwise report and leave for visibility-timeout redelivery, i.e. `fail`). `release` uses
   * `ChangeMessageVisibility` with {@link unknownUrnReleaseDelay}; `dead_letter` sends the annotated
   * envelope to {@link deadLetterQueueUrl} then deletes, degrading to `delete` when no DLQ is set.
   */
  unknownUrn?: string;
  /** Backoff (seconds) for the `release` unknown-URN strategy (default 0 = redeliver now). */
  unknownUrnReleaseDelay?: number;
  /**
   * Backoff before a failed message is redelivered. A failed handler always releases the message
   * via `ChangeMessageVisibility(ReceiptHandle, VisibilityTimeout)` (contract §3.5); this sets the
   * `VisibilityTimeout` — a number of seconds, or a function of the (reconciled) `attempts`
   * returning seconds (e.g. an exponential backoff). Clamped to `0…43200`. Default `0`: the message
   * is visible again immediately. A permanently failing handler therefore retries without pause
   * until the queue's `RedrivePolicy` (`maxReceiveCount`) moves it to the DLQ — configure one.
   */
  releaseDelay?: number | ((attempts: number) => number);
  /**
   * The cross-language `<queue>.dlq` URL for the `dead_letter` strategy (opt-in; default none). A
   * `.fifo` DLQ is sent with `MessageGroupId` (the source queue name) and `MessageDeduplicationId`
   * (`meta.id`).
   */
  deadLetterQueueUrl?: string | null;
  /**
   * Called for a non-conformant envelope, an unmapped URN (no `onUnknownUrn`), a throwing handler,
   * a throwing `onUnknownUrn` under an `unknownUrn` strategy, a failed release / dead-letter send, or
   * a failed delete after successful processing (an {@link SqsDeleteError} wrapping the broker
   * error; never released). The message is then left for visibility-timeout redelivery. The loop
   * never stops on these.
   */
  onError?: (error: unknown, envelope: IncomingEnvelope, message: SqsMessage) => void;
  /** Long-poll wait seconds (default 20). */
  waitTimeSeconds?: number;
  /** Reservation window applied on receive (seconds). */
  visibilityTimeout?: number;
  /** Max messages per receive (default 10). */
  maxMessages?: number;
}

/**
 * Reported via `onError` when `DeleteMessage` fails for a message whose processing already succeeded
 * (a handler returned normally, or the unknown-URN strategy chose `delete` / `dead_letter`). The
 * broker error is `cause`. Distinct from a handler failure: the message is NOT released, so it is
 * redelivered only after its visibility timeout expires (at-least-once; dedupe on `meta.id` with the
 * idempotency helper if the side effect must not repeat).
 */
export class SqsDeleteError extends BabelQueueError {
  constructor(cause: unknown) {
    super("Failed to delete a processed SQS message; it will be redelivered after its visibility timeout.");
    this.name = "SqsDeleteError";
    this.cause = cause;
  }
}

/**
 * Polls an SQS queue, decodes + validates each message, routes it to the handler
 * registered for its URN, and deletes it on success. A throwing handler's message is released via
 * `ChangeMessageVisibility` after `releaseDelay` (default 0 s, contract §3.5); if the release itself
 * fails the message is left for visibility-timeout redelivery (at-least-once);
 * `attempts` is reconciled to `ApproximateReceiveCount − 1` for the handler. An unmapped URN
 * follows the `unknownUrn` strategy (`fail` | `delete` | `release` | `dead_letter`).
 */
export class SqsConsumer {
  constructor(
    private readonly client: SqsApi,
    private readonly queueUrl: string,
    private readonly handlers: BabelHandlers,
    private readonly options: SqsConsumerOptions = {},
  ) {
    const strategy = options.unknownUrn;
    if (strategy !== undefined && !(Object.values(UnknownUrnStrategy) as string[]).includes(strategy)) {
      throw new BabelQueueError(`Unknown unknownUrn strategy "${strategy}".`);
    }
    const needsRelease = options.releaseDelay != null || strategy === UnknownUrnStrategy.RELEASE;
    if (needsRelease && typeof client.changeMessageVisibility !== "function") {
      throw new BabelQueueError(
        "SqsConsumer release requires a client with changeMessageVisibility (contract §3.5).",
      );
    }
  }

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
      await this.unknown(urn, envelope as Envelope, message);
      return;
    }

    try {
      await handler(envelope, message, headersOf(message));
    } catch (error) {
      this.options.onError?.(error, envelope, message);
      await this.guard(envelope, message, async () => {
        const delay = this.options.releaseDelay ?? 0;
        const attempts = typeof envelope.attempts === "number" ? envelope.attempts : 0;
        await this.release(message, typeof delay === "function" ? delay(attempts) : delay);
      });
      return;
    }
    // Outside the handler's try: a failed delete is not a handler failure. It is reported as an
    // SqsDeleteError and the message is NOT released — releasing at the (default 0 s) backoff would
    // redeliver an already-processed message immediately; it returns on visibility expiry instead.
    await this.guard(envelope, message, () => this.deleteHandled(message));
  }

  /**
   * Run a release / dead-letter step; a failure (e.g. `MessageNotInflight`, a missing IAM grant, a
   * throwing `releaseDelay`) is reported to `onError` and the message is left undeleted, so
   * visibility expiry still redelivers it. Never lets the error stop the consume loop.
   */
  private async guard(envelope: IncomingEnvelope, message: SqsMessage, step: () => Promise<void>): Promise<void> {
    try {
      await step();
    } catch (error) {
      this.options.onError?.(error, envelope, message);
    }
  }

  /** Apply the unknown-URN strategy (contract §3.5). */
  private async unknown(urn: string, envelope: Envelope, message: SqsMessage): Promise<void> {
    const strategy = this.options.unknownUrn;
    if (strategy === undefined) {
      if (this.options.onUnknownUrn) {
        await this.options.onUnknownUrn(envelope, message);
        await this.delete(message);
      } else {
        this.options.onError?.(new UnknownUrnError(urn), envelope, message);
      }
      return;
    }

    await this.guard(envelope, message, async () => {
      await this.options.onUnknownUrn?.(envelope, message);
    });
    switch (strategy) {
      case UnknownUrnStrategy.DELETE:
        await this.guard(envelope, message, () => this.deleteHandled(message));
        return;
      case UnknownUrnStrategy.RELEASE:
        await this.guard(envelope, message, () => this.release(message, this.options.unknownUrnReleaseDelay ?? 0));
        return;
      case UnknownUrnStrategy.DEAD_LETTER:
        await this.guard(envelope, message, async () => {
          await this.deadLetter(envelope, "unknown_urn");
          await this.deleteHandled(message);
        });
        return;
      default:
        // FAIL: surface and do NOT delete — visibility expiry redelivers, native redrive quarantines.
        this.options.onError?.(new UnknownUrnError(urn), envelope, message);
    }
  }

  /** Make the message visible again after `seconds` via `ChangeMessageVisibility`; never deletes. */
  private async release(message: SqsMessage, seconds: number): Promise<void> {
    if (!message.ReceiptHandle || !this.client.changeMessageVisibility) return;
    const timeout = Number.isFinite(seconds)
      ? Math.min(MAX_VISIBILITY_TIMEOUT, Math.max(0, Math.floor(seconds)))
      : 0;
    await this.client.changeMessageVisibility({
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
      VisibilityTimeout: timeout,
    });
  }

  /** Send the annotated envelope to the opt-in DLQ; a no-op (→ plain delete) when none is set. */
  private async deadLetter(envelope: Envelope, reason: string): Promise<void> {
    const dlq = this.options.deadLetterQueueUrl;
    if (!dlq) return;
    const source = envelope.meta?.queue ?? queueNameFromUrl(this.queueUrl);
    const annotated = annotate(envelope, reason, source, {
      attempts: envelope.attempts ?? 0,
    });
    const input: SendMessageInput = {
      QueueUrl: dlq,
      MessageBody: EnvelopeCodec.encode(annotated),
      MessageAttributes: toMessageAttributes(annotated),
    };
    if (dlq.endsWith(".fifo")) {
      // A FIFO DLQ (`<queue>.dlq.fifo`) rejects a send without a group; dedup on meta.id (§3.2).
      input.MessageGroupId = source;
      if (annotated.meta?.id) input.MessageDeduplicationId = annotated.meta.id;
    }
    await this.client.sendMessage(input);
  }

  /** Delete a message whose processing already succeeded; a broker failure becomes an SqsDeleteError. */
  private async deleteHandled(message: SqsMessage): Promise<void> {
    try {
      await this.delete(message);
    } catch (error) {
      throw new SqsDeleteError(error);
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
