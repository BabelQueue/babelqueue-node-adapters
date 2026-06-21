/**
 * Redis adapter for BabelQueue.
 *
 * A canonical-envelope **publisher** and a URN-routed **consumer** over the §1 reliable-queue
 * pattern (`RPUSH` to produce, `BRPOPLPUSH` the head to a `<queue>:processing` list to reserve so
 * an in-flight message survives a crash, `LREM` to acknowledge), so a Redis-based Node service
 * speaks the same wire contract as the PHP, Python and Go SDKs.
 *
 * Redis lists carry **no native metadata** — the list element **is** the canonical envelope JSON,
 * byte-for-byte, with **no wrapping** (unlike `@babelqueue/bullmq`, which uses BullMQ's own job
 * layout). Routing and tracing read the body's `job` / `trace_id`. This is a Node-owned reliable
 * queue; full parity with Laravel's reserved-sorted-set reservation on a *shared* Redis queue is a
 * separate task — for a mixed PHP+Node fleet on one queue, prefer a queue this consumer owns
 * end-to-end.
 *
 * This implements §1 of the broker-bindings contract. `ioredis` is an optional peer — you provide
 * the client (an `ioredis` instance satisfies the adapter structurally).
 *
 * **Out-of-band headers (ADR-0028).** A Redis list element *is* the canonical envelope JSON (the
 * `LREM` ack handle is that exact value), so — unlike AMQP headers or SQS `MessageAttributes` —
 * there is no native per-message metadata channel. To carry a {@link HeaderCarrier} (e.g. a W3C
 * `traceparent` for cross-hop span linkage) the adapter owns a tiny JSON *frame* distinct from the
 * wire envelope:
 *
 *     {"__bq_frame":1,"headers":{"traceparent":"00-…"},"body":"<raw wire envelope>"}
 *
 * `RPUSH` stores the frame, so the `LREM` ack handle stays byte-for-byte what was pushed and the
 * reliable-queue semantics (`RPUSH`/`BRPOPLPUSH`/`LREM`) are untouched. Framing is **opt-in and
 * backward compatible**: only `publish` with a non-empty `headers` carrier writes a frame; a plain
 * publish stores the **bare** envelope byte-for-byte, exactly as before. The consumer detects
 * frame-vs-bare by the reserved `__bq_frame` sentinel (a frozen envelope can never carry it), so a
 * bare value consumes with empty headers and cross-version queues interoperate. This mirrors the
 * Go `/redis` and PHP `RedisTransport` framing byte-for-byte. GR-1: the wire envelope is never
 * touched.
 */

import { annotate, BabelQueueError, EnvelopeCodec, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";

// --- Minimal Redis shape (a structural subset of ioredis) ----------------------

/** The subset of an ioredis client this adapter calls. */
export interface RedisClient {
  /** Append a value to the tail of a list; returns the new length. */
  rpush(key: string, value: string): Promise<number>;
  /** Atomically pop the head of `source` and push it to the tail of `destination`, blocking up
   *  to `timeout` seconds; resolves to the value, or `null` on timeout. */
  brpoplpush(source: string, destination: string, timeout: number): Promise<string | null>;
  /** Remove `count` occurrences of `value` from a list; returns how many were removed. */
  lrem(key: string, count: number, value: string): Promise<number>;
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

// --- Publisher -----------------------------------------------------------------

/** Options for {@link RedisPublisher.publish}. */
export interface PublishOptions {
  traceId?: string;
  /**
   * Out-of-band transport headers to carry beside the frozen envelope (ADR-0028) — e.g. a W3C
   * `traceparent` written by `@babelqueue/core/otel`'s `publish`. A non-empty carrier RPUSHes a
   * transport-owned frame; an empty/omitted carrier stays a byte-identical bare publish.
   */
  headers?: HeaderCarrier;
}

/** Sends canonical-envelope messages to one Redis list (§1 reliable-queue). */
export class RedisPublisher {
  private constructor(
    private readonly client: RedisClient,
    private readonly queue: string,
  ) {}

  /** A publisher over `client`, producing to the list named `queue`. */
  static create(client: RedisClient, queue: string): RedisPublisher {
    return new RedisPublisher(client, queue);
  }

  /**
   * Build + `RPUSH` the canonical envelope; returns the message id (`meta.id`). When
   * `options.headers` carries any out-of-band header (e.g. a `traceparent`, ADR-0028) the value
   * pushed is a transport-owned frame that wraps the envelope; otherwise the bare envelope is
   * pushed byte-for-byte (GR-1, no regression).
   */
  async publish(urn: string, data: Record<string, unknown>, options: PublishOptions = {}): Promise<string> {
    const envelope = EnvelopeCodec.make(urn, data, { queue: this.queue, traceId: options.traceId });
    await this.client.rpush(this.queue, frameValue(redisValue(envelope), options.headers));
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw stored list element, and the out-of-band
 * {@link HeaderCarrier} carried beside it (empty for a bare value). Pass `headers` to
 * `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child of the producer
 * span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, raw: string, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link RedisConsumer}. */
export interface RedisConsumerOptions {
  /** Suffix for the per-queue reservation list (default `":processing"`). */
  processingSuffix?: string;
  /** Block timeout (seconds) for a single reserve (default 5). */
  blockTimeout?: number;
  /** Attempts before terminal dead-lettering (default 3). */
  maxTries?: number;
  /** The dead-letter list (default `<queue>.dlq`); set to `null` to drop on terminal failure. */
  deadLetterQueue?: string | null;
  /** Strategy for a URN with no handler (default `fail`). */
  unknownUrn?: string;
  /** Called for a non-conformant message, an unmapped URN, or a throwing handler. */
  onError?: (error: unknown, envelope: IncomingEnvelope | null, raw: string) => void;
}

/**
 * Consumes a Redis list: reserve the head into `<queue>:processing` (`BRPOPLPUSH`), decode +
 * validate, route to the handler for its URN, then `LREM` it from the reservation list on success.
 * A throwing handler requeues the envelope with `attempts + 1` (at-least-once) up to `maxTries`,
 * then dead-letters to `<queue>.dlq`. Redis has no native delivery count, so `attempts` lives in
 * the body and the runtime owns retry.
 */
export class RedisConsumer {
  private readonly processing: string;
  private readonly blockTimeout: number;
  private readonly maxTries: number;
  private readonly dlq: string | null;
  private readonly unknownUrn: string;

  constructor(
    private readonly client: RedisClient,
    private readonly queue: string,
    private readonly handlers: BabelHandlers,
    private readonly options: RedisConsumerOptions = {},
  ) {
    this.processing = queue + (options.processingSuffix ?? ":processing");
    this.blockTimeout = options.blockTimeout ?? 5;
    this.maxTries = options.maxTries ?? 3;
    this.dlq = options.deadLetterQueue === undefined ? `${queue}.dlq` : options.deadLetterQueue;
    this.unknownUrn = options.unknownUrn ?? UnknownUrnStrategy.FAIL;
  }

  /** Reserve + route + settle one message. Returns true if one was handled, false on timeout. */
  async poll(): Promise<boolean> {
    const raw = await this.client.brpoplpush(this.queue, this.processing, this.blockTimeout);
    if (raw == null) return false;
    await this.handle(raw);
    return true;
  }

  /** Poll while `shouldContinue` returns true. */
  async run(shouldContinue: () => boolean): Promise<void> {
    while (shouldContinue()) {
      await this.poll();
    }
  }

  /**
   * Route + settle one reserved element. Exposed for testing.
   *
   * `raw` is the **stored** list value (the `LREM` ack handle); it may be a transport-owned header
   * frame or a bare envelope. It is unframed first so the envelope is decoded from the verbatim
   * wire body while the carried out-of-band headers (e.g. a `traceparent`, ADR-0028) are surfaced
   * to the handler. Settling/dead-lettering still operate on the original stored value.
   */
  async handle(raw: string): Promise<void> {
    const [body, headers] = unframe(raw);
    const decoded = EnvelopeCodec.decode(body);

    if (!EnvelopeCodec.accepts(decoded)) {
      this.report(new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Redis."), decoded, raw);
      await this.deadLetterRaw(raw);
      await this.settle(raw);
      return;
    }

    const envelope = decoded as Envelope;
    const urn = EnvelopeCodec.urn(envelope);
    const handler = this.handlers[urn];
    if (!handler) {
      await this.onUnknownUrn(envelope, urn, raw);
      return;
    }

    try {
      await handler(envelope, raw, headers);
      await this.settle(raw);
    } catch (error) {
      this.report(error, envelope, raw);
      await this.retryOrDeadLetter(envelope, raw, error);
    }
  }

  private async onUnknownUrn(envelope: Envelope, urn: string, raw: string): Promise<void> {
    switch (this.unknownUrn) {
      case UnknownUrnStrategy.DELETE:
        await this.settle(raw);
        return;
      case UnknownUrnStrategy.DEAD_LETTER:
        await this.deadLetter(envelope, "unknown_urn");
        await this.settle(raw);
        return;
      case UnknownUrnStrategy.RELEASE:
        await this.requeue(envelope);
        await this.settle(raw);
        return;
      default: {
        // FAIL: surface and leave the element in the reservation list (recoverable).
        const error = new UnknownUrnError(urn);
        this.report(error, envelope, raw);
        throw error;
      }
    }
  }

  private async retryOrDeadLetter(envelope: Envelope, raw: string, error: unknown): Promise<void> {
    if ((envelope.attempts ?? 0) + 1 < this.maxTries) {
      await this.requeue(envelope);
    } else {
      await this.deadLetter(envelope, "failed", error);
    }
    await this.settle(raw);
  }

  /** Republish with `attempts + 1` onto the work list (at-least-once retry). */
  private async requeue(envelope: Envelope): Promise<void> {
    const bumped: Envelope = { ...envelope, attempts: (envelope.attempts ?? 0) + 1 };
    await this.client.rpush(this.queue, redisValue(bumped));
  }

  private async deadLetter(envelope: Envelope, reason: string, error?: unknown): Promise<void> {
    if (!this.dlq) return; // dead-lettering disabled → drop
    const annotated = annotate(envelope, reason, envelope.meta?.queue ?? this.queue, {
      attempts: envelope.attempts ?? 0,
      error: error instanceof Error ? error.message : null,
      exception: error instanceof Error ? error.name : null,
    });
    await this.client.rpush(this.dlq, redisValue(annotated));
  }

  private async deadLetterRaw(raw: string): Promise<void> {
    if (!this.dlq) return;
    await this.client.rpush(this.dlq, raw);
  }

  /** Remove the element from the reservation list (acknowledge). */
  private async settle(raw: string): Promise<void> {
    await this.client.lrem(this.processing, 1, raw);
  }

  private report(error: unknown, envelope: IncomingEnvelope | null, raw: string): void {
    this.options.onError?.(error, envelope, raw);
  }
}
