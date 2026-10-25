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
 * **Version gate (§3.7, ADR-0037 decision 1).** Before the body is decoded the consumer reads the `bq-schema-version`
 * message attribute. A missing or blank value (empty or only ASCII whitespace) changes nothing; any
 * value other than the core's supported `SCHEMA_VERSION` (`"2"`, `"x"`, `"01"`, ...) rejects the
 * message undecoded (`onError` with an error naming the version and an empty envelope) and settles it on its
 * **first delivery**: never decoded, never given to a handler, `onUnknownUrn` or a retry. Under
 * `unknownUrn: "delete"` it is deleted; with a `deadLetterQueueUrl` the body, byte for byte and without a
 * `dead_letter` block, is sent to the DLQ and deleted only after the send succeeded (a failed send leaves it
 * undeleted: visibility expiry redelivers it and the write is tried again); without a `deadLetterQueueUrl`
 * it is left to the **native** mechanism — not deleted, so visibility expiry redelivers it and the queue's
 * `RedrivePolicy` moves it to the native DLQ (pair the queue with one). Only the explicitly configured
 * `unknownUrn` strategy is read: an `onUnknownUrn` hook never changes it (a hook-only consumer behaves as `fail`).
 *
 * **Poison (ADR-0037 decision 2, ADR-0035).** A body that does not decode into a valid envelope is never given
 * to a handler and never retried. `unknownUrn: "delete"` deletes it; with a `deadLetterQueueUrl` the ADR-0035
 * record (`reason: "poison"`, built with the core's `buildPoisonRecord`) is sent to the DLQ and the message
 * deleted only after the send succeeded; without one it is left to the native mechanism, as above.
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

import { createHash } from "node:crypto";

import {
  BabelQueueError,
  EnvelopeCodec,
  SCHEMA_VERSION,
  UnknownUrnError,
  UnknownUrnStrategy,
  annotate,
  buildPoisonRecord,
  retryBackoffBound,
  retryBackoffDelay,
} from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope, RetryBackoffOptions } from "@babelqueue/core";

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
  MessageId?: string;
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

/** Blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r) — not `String.prototype.trim()`'s wider set. */
const BLANK_ASCII = /^[ \t\n\v\f\r]*$/;

/**
 * The `bq-schema-version` message attribute when it is present, non-blank and not the supported
 * version (compared against the core's `SCHEMA_VERSION`, exact string match: `"01"`, `" 1"`, `"1.0"`
 * are rejected). Blank means the empty string or only ASCII whitespace (the same set in every SDK,
 * GR-5); NBSP, U+0085, U+FEFF, U+3000 and the like are not blank. `undefined` when the message may
 * be decoded.
 */
function unsupportedSchemaVersion(message: SqsMessage): string | undefined {
  const raw: unknown = message.MessageAttributes?.["bq-schema-version"]?.StringValue;
  if (typeof raw !== "string" || BLANK_ASCII.test(raw)) return undefined;
  return raw === String(SCHEMA_VERSION) ? undefined : raw;
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
export interface SqsConsumerOptions extends RetryBackoffOptions {
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
   *
   * The version gate and the poison route read **only this explicitly configured strategy** (ADR-0037): an
   * `onUnknownUrn` hook, and the `delete` it implies when no strategy is set, never apply to them.
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
   *
   * This is the SQS spelling of the cross-SDK A9 backoff and stays supported as an alias: when it is
   * set it wins over `retryBackoff` / `retryBackoffMultiplier` / `retryBackoffMax` / `retryJitter`
   * (inherited from {@link RetryBackoffOptions}), which compute `d(n) = min(max, base * multiplier^(n-1))`
   * seconds for the 1-based attempt `n = attempts + 1` (the SQS 12 h ceiling still applies on top).
   */
  releaseDelay?: number | ((attempts: number) => number);
  /**
   * The cross-language `<queue>.dlq` URL (opt-in; default none): the `dead_letter` strategy, poison bodies
   * (ADR-0035 record) and version-gated deliveries (the raw body) are written here. Without it a poison or
   * version-gated delivery is left to the queue's native `RedrivePolicy` (ADR-0037). A
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

function hasRetryBackoff(options: RetryBackoffOptions): boolean {
  return (
    options.retryBackoff !== undefined ||
    options.retryBackoffMultiplier !== undefined ||
    options.retryBackoffMax !== undefined ||
    options.retryJitter !== undefined
  );
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
    // Fail fast on an invalid A9 parameter instead of at the first failed message.
    retryBackoffBound(options, 1);
    if (options.retryJitter !== undefined && options.retryJitter !== "none" && options.retryJitter !== "full") {
      throw new BabelQueueError(`retryJitter must be "none" or "full", got ${String(options.retryJitter)}.`);
    }
    const needsRelease =
      options.releaseDelay != null || hasRetryBackoff(options) || strategy === UnknownUrnStrategy.RELEASE;
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
    // §3.7: gate on the bq-schema-version attribute before the body is decoded (ADR-0037 decision 1).
    const rejectedVersion = unsupportedSchemaVersion(message);
    if (rejectedVersion !== undefined) {
      await this.settleVersionGated(
        message,
        new BabelQueueError(
          `Rejected a message from SQS with unsupported bq-schema-version "${rejectedVersion}" (supported: ${SCHEMA_VERSION}).`,
        ),
      );
      return;
    }

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
      await this.settlePoison(message, envelope, new BabelQueueError("Rejected a non-conformant BabelQueue envelope from SQS."));
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
        const attempts = typeof envelope.attempts === "number" ? envelope.attempts : 0;
        await this.release(message, this.failureDelay(attempts));
      });
      return;
    }
    // Outside the handler's try: a failed delete is not a handler failure. It is reported as an
    // SqsDeleteError and the message is NOT released — releasing at the (default 0 s) backoff would
    // redeliver an already-processed message immediately; it returns on visibility expiry instead.
    await this.guard(envelope, message, () => this.deleteHandled(message));
  }

  /**
   * Version-gated delivery (ADR-0037 decision 1). `unknownUrn: "delete"` deletes it; with a DLQ the raw body goes
   * to `<queue>.dlq` and the message is deleted only after the send; otherwise it is reported and left for the
   * queue's native mechanism (never deleted, never released by the SDK). Only the configured strategy is read.
   */
  private async settleVersionGated(message: SqsMessage, gateError: BabelQueueError): Promise<void> {
    this.options.onError?.(gateError, {}, message);
    const dlq = this.options.deadLetterQueueUrl;
    if (this.options.unknownUrn === UnknownUrnStrategy.DELETE) {
      await this.guard({}, message, () => this.deleteHandled(message));
      return;
    }
    if (!dlq) return; // native: SQS redelivers on visibility expiry, RedrivePolicy moves it after maxReceiveCount
    await this.guard({}, message, async () => {
      await this.sendRaw(dlq, message, message.Body ?? "");
      await this.deleteHandled(message);
    });
  }

  /**
   * Poison body (ADR-0037 decision 2). `unknownUrn: "delete"` deletes it; with a DLQ the ADR-0035 record is sent
   * and the message deleted only after the send; otherwise it is reported and left for the native mechanism.
   */
  private async settlePoison(message: SqsMessage, envelope: IncomingEnvelope, error: BabelQueueError): Promise<void> {
    this.options.onError?.(error, envelope, message);
    const dlq = this.options.deadLetterQueueUrl;
    if (this.options.unknownUrn === UnknownUrnStrategy.DELETE) {
      await this.guard(envelope, message, () => this.deleteHandled(message));
      return;
    }
    if (!dlq) return; // native, as above
    await this.guard(envelope, message, async () => {
      const source = queueNameFromUrl(this.queueUrl);
      const count = Number.parseInt(message.Attributes?.["ApproximateReceiveCount"] ?? "", 10);
      const delivered = Number.isFinite(count) && count > 1 ? count - 1 : 0; // informational: deliveries before this one
      const deadLetter = annotate({ attempts: delivered } as unknown as Envelope, "poison", source, {
        attempts: delivered,
        error: error.message,
        exception: error.name,
      }).dead_letter;
      const record = buildPoisonRecord(Buffer.from(message.Body ?? "", "utf8"), JSON.stringify(deadLetter));
      await this.sendRaw(dlq, message, record);
      await this.deleteHandled(message);
    });
  }

  /** Send `body` to the DLQ as is (a `.fifo` DLQ gets the source queue as group and a stable dedup id). */
  private async sendRaw(dlq: string, message: SqsMessage, body: string): Promise<void> {
    const input: SendMessageInput = { QueueUrl: dlq, MessageBody: body };
    if (dlq.endsWith(".fifo")) {
      input.MessageGroupId = queueNameFromUrl(this.queueUrl);
      input.MessageDeduplicationId = message.MessageId ?? createHash("sha256").update(message.Body ?? "").digest("hex");
    }
    await this.client.sendMessage(input);
  }

  /** The release delay (seconds) for a failed handler: `releaseDelay` (alias) wins, else the A9 backoff, else 0. */
  private failureDelay(attempts: number): number {
    const delay = this.options.releaseDelay;
    if (delay != null) return typeof delay === "function" ? delay(attempts) : delay;
    if (!hasRetryBackoff(this.options)) return 0;
    return retryBackoffDelay(this.options, Math.max(0, Math.floor(attempts)) + 1);
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
