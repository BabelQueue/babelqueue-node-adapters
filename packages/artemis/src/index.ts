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
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides as additional AMQP `application_properties` beside the contract `bq_`
 * properties — the contract properties win a key collision (merge-not-clobber), blanks dropped.
 * `traceparent`/`tracestate` are W3C keys with no hyphens, so they are already JMS-legal (§7 uses
 * underscores because JMS property names must be Java identifiers). On consume the inbound
 * `application_properties` are surfaced as a `Record<string,string>` so the core's `otel` extract
 * sees the `traceparent` and links the consumer span as a child. A header-less publish is
 * byte-identical. GR-1: the wire envelope body is never touched.
 *
 * **Version gate (§7.7).** Before decoding the body the consumer reads the `bq_schema_version`
 * application property (underscores, ADR-0017). A missing or blank value (empty or only ASCII
 * whitespace) follows the normal decode path; any other value is compared, untrimmed, to the supported
 * `"1"`, and a message that does not match is rejected undecoded — reported to `onError`, the raw body
 * forwarded to the DLQ sender (when configured) and the message accepted **only after the broker has
 * accepted the DLQ delivery**. If the DLQ send fails, is refused (rejected/released/modified), the link
 * closes or `deadLetterTimeoutMs` elapses, the failure is reported and, after `deadLetterRetryDelayMs`,
 * the source message gets a plain `released` outcome (no `delivery_failed`: the broker's delivery
 * counter and `max-delivery-attempts` stay untouched) — never accepted, never handed to a handler.
 */

import { annotate, BabelQueueError, EnvelopeCodec, SCHEMA_VERSION, UnknownUrnError, UnknownUrnStrategy } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { sanitizeHeaders } from "./headers.js";

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

/**
 * The context rhea passes to a sender link event (`accepted`, `rejected`, `sender_close`, ...). Only
 * `delivery` is inspected (to match a verdict to its delivery); everything else is opaque. Deliberately
 * no index signature: an interface-typed handler parameter (e.g. rhea's `EventContext`) must stay assignable.
 */
export interface AmqpLinkEventContext {
  delivery?: unknown;
}

/** The subset of a rhea sender this adapter calls. */
export interface AmqpSender {
  /** Queue a delivery. rhea returns the delivery synchronously; the broker's verdict arrives later as an event. */
  send(message: AmqpMessage): unknown;
  /**
   * Optional (a rhea sender has it). When both `on` and `removeListener` exist, the consumer's
   * version-gate path waits for the broker's `accepted` disposition on the DLQ delivery before it
   * settles the source message; without them only a synchronous `send` failure is observable.
   */
  on?(event: string, handler: (context: AmqpLinkEventContext) => void): unknown;
  removeListener?(event: string, handler: (context: AmqpLinkEventContext) => void): unknown;
  /**
   * Optional (a rhea link has it). rhea does not bubble an event to the session/connection while the
   * link has a listener for it, so the temporary confirmation listeners re-dispatch what is not theirs
   * (other deliveries' dispositions, link close/error) here.
   */
  session?: { dispatch?(event: string, context?: AmqpLinkEventContext): unknown };
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
export function artemisMessage(envelope: Envelope, delayMs?: number, extra?: HeaderCarrier | null): AmqpMessage {
  const annotations: Record<string, unknown> = {};
  if (envelope.job) annotations[JMS_TYPE_KEY] = envelope.job;

  // Out-of-band riders (e.g. traceparent) go in first; the contract bq_ properties below win any
  // key collision (merge-not-clobber). The bq_ names use underscores, not hyphens: a JMS property
  // name must be a valid Java identifier, and every Artemis SDK uses the same JMS-legal form for
  // cross-protocol parity. traceparent/tracestate have no hyphens, so they are already JMS-legal.
  const applicationProperties: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(sanitizeHeaders(extra))) applicationProperties[key] = value;
  applicationProperties["bq_schema_version"] = String(envelope.meta.schema_version);
  applicationProperties["bq_attempts"] = String(envelope.attempts ?? 0);
  applicationProperties["bq_app_id"] = "babelqueue";
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

/** Blank = empty or only ASCII whitespace (GR-5: the same set in every SDK, never `String.trim()`). */
const BLANK_ASCII = /^[ \t\n\v\f\r]*$/;

/**
 * The inbound `bq_schema_version` application property when it is present, non-blank and not exactly
 * the supported version (compared untrimmed), else `undefined` (the message may be decoded).
 */
function unsupportedSchemaVersion(message: AmqpMessage): string | undefined {
  const raw = message.application_properties?.["bq_schema_version"];
  if (raw == null) return undefined;
  const value = Buffer.isBuffer(raw) ? raw.toString("utf8") : String(raw);
  if (BLANK_ASCII.test(value)) return undefined;
  return value === String(SCHEMA_VERSION) ? undefined : value;
}

function deliveryCount(message: AmqpMessage): number {
  const value = message.delivery_count;
  return typeof value === "number" && value > 0 ? value : 0;
}

/**
 * Read a message's `application_properties` back into a flat {@link HeaderCarrier}, stringifying
 * values defensively. Returns an empty object when there are none. Both the contract `bq_`
 * properties and any out-of-band rider (e.g. `traceparent`) surface — the core's `otel` extract
 * reads only the keys it knows.
 */
export function headersOf(message: AmqpMessage): HeaderCarrier {
  const props = message.application_properties;
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

/** Options for {@link ArtemisPublisher.publish}. */
export interface PublishOptions {
  traceId?: string;
  /** Schedule via Artemis's native AMQP scheduled delivery (`x-opt-delivery-time`). */
  delayMs?: number;
  /**
   * Out-of-band transport headers carried as AMQP `application_properties` beside the contract `bq_`
   * properties (ADR-0028) — e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s `publish`.
   * The contract properties win a key collision; an empty/omitted carrier leaves the message
   * unchanged.
   */
  headers?: HeaderCarrier;
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
    await this.sender.send(artemisMessage(envelope, options.delayMs, options.headers));
    return envelope.meta.id;
  }
}

// --- DLQ delivery confirmation ---------------------------------------------------

/** Default wait (ms) for the broker to settle a gate-path DLQ delivery. */
const DEFAULT_DLQ_CONFIRM_TIMEOUT_MS = 30_000;
/** Default pause (ms) before a gate message whose DLQ write failed is released back to the broker. */
const DEFAULT_DLQ_RETRY_DELAY_MS = 1_000;

/** One in-flight DLQ confirmation registered on a {@link ConfirmationHub}. */
interface PendingConfirmation {
  /** The delivery `send` returned; `undefined` while `send` is still running synchronously. */
  delivery: unknown;
  /** `true` when `context` is (or may be) this confirmation's verdict. */
  matches(context: AmqpLinkEventContext | undefined): boolean;
  /** Resolve (no argument) or reject the confirmation. */
  settle(error?: Error): void;
}

const CONFIRMATION_VERDICTS = ["accepted", "rejected", "released", "modified"] as const;
const CONFIRMATION_CLOSES = ["sender_close", "sender_error"] as const;

/**
 * One shared set of temporary sender listeners per {@link AmqpSender}, however many DLQ confirmations
 * are in flight. rhea bubbles a link event to the session only while the link has no listener for it,
 * so while confirmations are pending the hub owns the listeners and re-dispatches every event it does
 * not consume to `sender.session` exactly once (per-confirmation listeners would forward each event
 * once per pending confirmation). The listeners are removed as soon as no confirmation is pending.
 */
class ConfirmationHub {
  private readonly pending = new Set<PendingConfirmation>();
  private readonly handlers: [string, (context: AmqpLinkEventContext) => void][] = [];

  constructor(
    private readonly sender: AmqpSender,
    private readonly on: (event: string, handler: (context: AmqpLinkEventContext) => void) => unknown,
    private readonly off: (event: string, handler: (context: AmqpLinkEventContext) => void) => unknown,
  ) {}

  add(confirmation: PendingConfirmation): void {
    this.pending.add(confirmation);
    if (this.handlers.length === 0) {
      for (const name of [...CONFIRMATION_VERDICTS, ...CONFIRMATION_CLOSES]) {
        const handler = (context: AmqpLinkEventContext): void => this.dispatch(name, context);
        this.handlers.push([name, handler]);
        this.on(name, handler);
      }
    }
  }

  remove(confirmation: PendingConfirmation): void {
    this.pending.delete(confirmation);
    if (this.pending.size === 0) {
      for (const [name, handler] of this.handlers) this.off(name, handler);
      this.handlers.length = 0;
      hubs.delete(this.sender);
    }
  }

  private dispatch(name: string, context: AmqpLinkEventContext): void {
    if ((CONFIRMATION_CLOSES as readonly string[]).includes(name)) {
      for (const confirmation of [...this.pending]) {
        confirmation.settle(new BabelQueueError(`The dead-letter link closed before the broker accepted the delivery (${name}).`));
      }
      this.forward(name, context);
      return;
    }
    const owners = [...this.pending].filter((confirmation) => confirmation.matches(context));
    if (owners.length === 0) {
      this.forward(name, context);
      return;
    }
    for (const owner of owners) {
      owner.settle(name === "accepted" ? undefined : new BabelQueueError(`The broker did not accept the dead-letter delivery (${name}).`));
    }
  }

  private forward(name: string, context: AmqpLinkEventContext): void {
    try {
      this.sender.session?.dispatch?.(name, context);
    } catch {
      // forwarding is best effort; never let it change a confirmation outcome
    }
  }
}

const hubs = new WeakMap<AmqpSender, ConfirmationHub>();

/**
 * Send `message` and resolve only once the broker has `accepted` the delivery. A rhea `send` returns
 * synchronously and reports the verdict (`accepted` / `rejected` / `released` / `modified`) as a
 * sender event, so awaiting `send` alone cannot see a broker refusal. Rejects on any non-accepted
 * disposition, a link close/error, or when `timeoutMs` elapses (an unsettled delivery that is lost on
 * reconnect raises no event). A sender without event support degrades to awaiting `send` itself.
 * Events that are not this delivery's verdict (other deliveries, link close/error) are re-dispatched on
 * the sender's session, once per event, so the application still sees them (rhea suppresses bubbling
 * while the link has a listener) — see {@link ConfirmationHub}.
 */
function sendConfirmed(sender: AmqpSender, message: AmqpMessage, timeoutMs: number): Promise<void> {
  const on = sender.on?.bind(sender);
  const off = sender.removeListener?.bind(sender);
  if (!on || !off) return Promise.resolve(sender.send(message)).then(() => undefined);

  return new Promise<void>((resolve, reject) => {
    let hub = hubs.get(sender);
    if (!hub) {
      hub = new ConfirmationHub(sender, on, off);
      hubs.set(sender, hub);
    }
    const activeHub = hub;
    let finished = false;

    const confirmation: PendingConfirmation = {
      delivery: undefined,
      matches: (context) =>
        confirmation.delivery === undefined || context?.delivery === undefined || context.delivery === confirmation.delivery,
      settle: (error) => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        activeHub.remove(confirmation);
        if (error) reject(error);
        else resolve();
      },
    };
    activeHub.add(confirmation);

    // `settle` (above) reads `timer`; it cannot run before this line (nothing is sent or listened to yet).
    const timer = setTimeout(
      () => confirmation.settle(new BabelQueueError(`Timed out after ${timeoutMs}ms waiting for the broker to accept the dead-letter delivery.`)),
      timeoutMs,
    );
    try {
      confirmation.delivery = sender.send(message);
    } catch (error) {
      confirmation.settle(error instanceof Error ? error : new BabelQueueError(String(error)));
      return;
    }
    // A pre-settled delivery (or a sender that returns none) raises no disposition event.
    const delivery = confirmation.delivery;
    if (delivery === null || typeof delivery !== "object" || (delivery as { settled?: unknown }).settled === true) confirmation.settle();
  });
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw AMQP message, and the out-of-band
 * {@link HeaderCarrier} read from the message's `application_properties` (empty when there are
 * none). Pass `headers` to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a
 * child of the producer span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, message: AmqpMessage, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link ArtemisConsumer}. */
export interface ArtemisConsumerOptions {
  /** A sender to `<queue>.dlq`; enables cross-language dead-lettering. Without it a terminal failure drops. */
  deadLetterSender?: AmqpSender;
  /**
   * How long (ms) every dead-letter path (version gate, poison, unknown-URN `dead_letter`, terminal
   * handler failure) waits for the broker to accept the DLQ delivery before it releases the source
   * message instead of accepting it (default 30000).
   */
  deadLetterTimeoutMs?: number;
  /**
   * Pause (ms) before a dead-letter path releases a message whose DLQ write failed (default 1000).
   * The release is a plain `released` outcome (no `delivery_failed`, so the broker's delivery counter and
   * `max-delivery-attempts` are untouched); the pause keeps a persistently failing DLQ from becoming a
   * hot redelivery loop. The delivery stays unsettled (holding one credit) while it waits.
   * Note: on the terminal handler-failure path the message has already been through the handler, and
   * a plain release does not bump the delivery counter, so the redelivery runs the handler again (and
   * reports its failure again) until the DLQ write succeeds.
   */
  deadLetterRetryDelayMs?: number;
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

    // Version gate (§7.7): decided on the application property alone, before the body is decoded.
    const unsupported = unsupportedSchemaVersion(message);
    if (unsupported !== undefined) {
      const gateError = new BabelQueueError(
        `Rejected a message from Artemis with unsupported bq_schema_version "${unsupported}" (supported: ${SCHEMA_VERSION}).`,
      );
      // Accept only after the broker has accepted the DLQ copy; otherwise release (plain, after a pause) so
      // the message is never lost and never reaches a handler. Reports come after the settlement, or are
      // isolated, so a throwing onError can never leave the message unsettled.
      try {
        await this.deadLetterRawConfirmed(messageBody(message));
      } catch (dlqError) {
        this.safeReport(gateError, null, context);
        this.safeReport(dlqError, null, context);
        await this.releaseAfterBackoff(delivery);
        return;
      }
      delivery?.accept();
      this.safeReport(gateError, null, context);
      return;
    }

    const decoded = EnvelopeCodec.decode(messageBody(message));

    if (!EnvelopeCodec.accepts(decoded)) {
      // A non-conformant / poison message may lack the fields to annotate; forward it raw.
      this.report(new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Artemis."), decoded, context);
      await this.settleAfterDeadLetter(context, decoded, () => this.deadLetterRaw(messageBody(message)));
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
      await handler(envelope, message, headersOf(message));
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
        await this.settleAfterDeadLetter(context, envelope, () => this.deadLetter(envelope, "unknown_urn"));
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
      await this.settleAfterDeadLetter(context, envelope, () => this.deadLetter(envelope, "failed", error));
    } else {
      delivery?.accept(); // terminal, no DLQ → drop
    }
  }

  /**
   * Dead-letter write → `accept`, or, when the write fails, report + plain `release` after the retry
   * delay (never `accept`, so a failed DLQ write cannot lose the source message). Without a DLQ sender
   * the write is a no-op and the message is accepted (terminal drop), as before.
   */
  private async settleAfterDeadLetter(
    context: AmqpEventContext,
    envelope: IncomingEnvelope | null,
    write: () => Promise<void>,
  ): Promise<void> {
    try {
      await write();
    } catch (dlqError) {
      this.safeReport(dlqError, envelope, context);
      await this.releaseAfterBackoff(context.delivery);
      return;
    }
    context.delivery?.accept();
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
    await sendConfirmed(sender, artemisMessage(annotated), this.options.deadLetterTimeoutMs ?? DEFAULT_DLQ_CONFIRM_TIMEOUT_MS);
  }

  /** Poison path: forward the raw body, confirmed like the gate path. */
  private deadLetterRaw(raw: string): Promise<void> {
    return this.deadLetterRawConfirmed(raw);
  }

  /** Forward the raw body and wait for the broker's `accepted` (a no-op without a DLQ sender). */
  private async deadLetterRawConfirmed(raw: string): Promise<void> {
    const sender = this.options.deadLetterSender;
    if (!sender) return;
    await sendConfirmed(sender, { body: raw }, this.options.deadLetterTimeoutMs ?? DEFAULT_DLQ_CONFIRM_TIMEOUT_MS);
  }

  /** Plain `released` outcome (the broker's delivery counter is not touched), after the retry delay. */
  private async releaseAfterBackoff(delivery: AmqpDelivery | undefined): Promise<void> {
    const delayMs = this.options.deadLetterRetryDelayMs ?? DEFAULT_DLQ_RETRY_DELAY_MS;
    if (delayMs > 0) await new Promise<void>((resolve) => setTimeout(resolve, delayMs).unref?.());
    delivery?.release();
  }

  /** `onError` isolated: a throwing callback must not skip the settlement that follows or precedes it. */
  private safeReport(error: unknown, envelope: IncomingEnvelope | null, context: AmqpEventContext): void {
    try {
      this.report(error, envelope, context);
    } catch {
      // swallowed by design
    }
  }

  private report(error: unknown, envelope: IncomingEnvelope | null, context: AmqpEventContext): void {
    this.options.onError?.(error, envelope, context);
  }
}
