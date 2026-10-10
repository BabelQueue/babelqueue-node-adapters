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
 * redelivered (at-least-once) and `getRedeliveryCount()` is incremented. Per §5.2 / §5.7 the
 * consumer routes on the `bq-job` property (the body's URN is used only when the property is
 * absent or blank) and takes `attempts` from the authoritative `bq-attempts` property (the
 * body's value is used only when the property is absent or not a plain ASCII-decimal integer
 * that fits a 32-bit int), reconciled to `max(bq-attempts, getRedeliveryCount())` — no −1,
 * because Pulsar's redelivery count is 0-based. The handler still receives the body's envelope.
 *
 * **Version gate (§5.7 step 3).** Before the body is decoded the consumer reads the
 * `bq-schema-version` property. A missing or blank value (empty or only ASCII whitespace) changes nothing; any value other than
 * the core's supported `SCHEMA_VERSION` (`"2"`, `"x"`, `"01"`, ...) rejects the message undecoded
 * through the same path as a non-conformant envelope — `onError` (with an error naming the
 * version, and an empty envelope since nothing was decoded) and a `negativeAcknowledge`.
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides as additional native Pulsar **message properties** beside the contract `bq-*`
 * properties — the contract properties win a key collision (merge-not-clobber), and blanks are
 * dropped. On consume the inbound `getProperties()` are surfaced as a `Record<string,string>` so
 * the core's `otel` extract sees the `traceparent` and links the consumer span as a child. A
 * header-less publish is byte-identical. GR-1: the wire envelope (the payload) is never touched.
 */

import { BabelQueueError, EnvelopeCodec, SCHEMA_VERSION, UnknownUrnError } from "@babelqueue/core";
import type { Envelope, HeaderCarrier, IncomingEnvelope } from "@babelqueue/core";

import { mergeInto } from "./headers.js";

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

/**
 * Project the envelope onto a native Pulsar producer message (payload + properties). When `extra`
 * is given, those out-of-band headers (e.g. a `traceparent`, ADR-0028) are merged beside the
 * contract `bq-*` properties without clobbering them (the contract wins a key collision).
 */
export function toPulsarMessage(envelope: Envelope, extra?: HeaderCarrier | null): PulsarProducerMessage {
  return {
    data: Buffer.from(EnvelopeCodec.encode(envelope), "utf8"),
    properties: mergeInto(pulsarProperties(envelope), extra),
  };
}

/**
 * Read a received message's native properties back into a flat {@link HeaderCarrier} via
 * `getProperties()`. Returns an empty object when there are none. Both the contract `bq-*`
 * properties and any out-of-band rider (e.g. `traceparent`) surface — the core's `otel` extract
 * reads only the keys it knows.
 */
export function headersOf(message: PulsarReceivedMessage): HeaderCarrier {
  const props = typeof message.getProperties === "function" ? message.getProperties() : undefined;
  const out: HeaderCarrier = {};
  if (!props) return out;
  for (const key of Object.keys(props)) {
    const value = props[key];
    if (value != null && value !== "") out[key] = value;
  }
  return out;
}

// --- Publisher -----------------------------------------------------------------

/** Options for {@link PulsarPublisher.publish}. */
export interface PublishOptions {
  /** Reuse an existing trace id (trace continuation). */
  traceId?: string;
  /** Schedule native delayed delivery this many milliseconds from now (`deliverAfter`). */
  delayMs?: number;
  /**
   * Out-of-band transport headers carried as native Pulsar message properties beside the contract
   * `bq-*` properties (ADR-0028) — e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s
   * `publish`. The contract properties win a key collision; an empty/omitted carrier leaves the
   * message unchanged.
   */
  headers?: HeaderCarrier;
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
    const message = toPulsarMessage(envelope, options.headers);

    if (options.delayMs != null && options.delayMs > 0) {
      message.properties = { ...message.properties, "bq-delay": String(options.delayMs) };
      message.deliverAfter = options.delayMs;
    }

    await this.producer.send(message);
    return envelope.meta.id;
  }
}

// --- Consumer ------------------------------------------------------------------

/**
 * A URN handler. Receives the validated envelope, the raw Pulsar message, and the out-of-band
 * {@link HeaderCarrier} read from the message's properties (empty when there are none). Pass
 * `headers` to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child of the
 * producer span (ADR-0028).
 */
export type BabelHandler = (envelope: Envelope, message: PulsarReceivedMessage, headers: HeaderCarrier) => unknown | Promise<unknown>;

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
  /**
   * Optional diagnostic hook, called when the `bq-job` property and the body's URN differ (the
   * property wins and the message is routed on it). `envelope` is the body's envelope. Separate
   * from {@link PulsarConsumerOptions.onError}: it is not an error, and anything it throws (or
   * rejects with) is swallowed — it never changes how the message is handled or acknowledged.
   */
  onRoutingMismatch?: (propertyUrn: string, bodyUrn: string, envelope: IncomingEnvelope) => unknown;
}

/** `bq-attempts` is valid only as ASCII digits that fit a signed 32-bit int (matches Java/.NET, GR-5). */
const MAX_ATTEMPTS = 2 ** 31 - 1;

function parseAttempts(raw: string | undefined): number | undefined {
  if (typeof raw !== "string" || !/^[0-9]+$/.test(raw)) return undefined;
  const value = Number(raw);
  return value <= MAX_ATTEMPTS ? value : undefined;
}

/** Blank = empty or only ASCII whitespace (space, \t, \n, \v, \f, \r) — not `String.prototype.trim()`'s wider set. */
const BLANK_ASCII = /^[ \t\n\v\f\r]*$/;

/**
 * The `bq-schema-version` value when it is present, non-blank and not the supported version
 * (compared against the core's `SCHEMA_VERSION`, exact string match: `"01"`, `" 1"`, `"1.0"` are
 * rejected). Blank means the empty string or only ASCII whitespace (the same set in every SDK, GR-5);
 * NBSP, U+0085, U+FEFF, U+3000 and the like are not blank. `undefined` when the message may be decoded.
 */
function unsupportedSchemaVersion(properties: { [key: string]: string }): string | undefined {
  const raw: unknown = properties["bq-schema-version"];
  if (typeof raw !== "string" || BLANK_ASCII.test(raw)) return undefined;
  return raw === String(SCHEMA_VERSION) ? undefined : raw;
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
 * `getRedeliveryCount()` (at-least-once). Routing uses the `bq-job` property (body URN as the
 * fallback); `attempts` is the `bq-attempts` property (body value as the fallback) reconciled to
 * `max(.., getRedeliveryCount())` for the handler.
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
    const properties = safeProperties(message);

    // §5.7 step 3: gate on bq-schema-version before the body is decoded.
    const rejectedVersion = unsupportedSchemaVersion(properties);
    if (rejectedVersion !== undefined) {
      this.options.onError?.(
        new BabelQueueError(
          `Rejected a message from Apache Pulsar with unsupported bq-schema-version "${rejectedVersion}" (supported: ${SCHEMA_VERSION}).`,
        ),
        {},
        message,
      );
      await this.consumer.negativeAcknowledge(message);
      return;
    }

    const envelope = EnvelopeCodec.decode(decodePayload(message.getData()));

    // attempts: the authoritative bq-attempts property (the body's value when it is absent or
    // invalid), then max(.., getRedeliveryCount()): the redelivery count is 0-based, so it maps
    // directly with no −1; the max never lowers a higher count republished from another SDK
    // (the Go/Python transports retry by re-sending with attempts+1).
    const redeliveryCount = typeof message.getRedeliveryCount === "function" ? message.getRedeliveryCount() : 0;
    // A body without an integer attempts stays as sent, so accepts() below still rejects it
    // (the property can't repair a non-conformant body); the value is written only on a change.
    const bodyAttempts = envelope.attempts;
    let attempts = typeof bodyAttempts === "number" && Number.isInteger(bodyAttempts) ? bodyAttempts : 0;
    const fromProperty = parseAttempts(properties["bq-attempts"]);
    if (fromProperty !== undefined) attempts = fromProperty;
    if (redeliveryCount > attempts) attempts = redeliveryCount;
    if (typeof bodyAttempts === "number" && Number.isInteger(bodyAttempts) && bodyAttempts !== attempts) {
      envelope.attempts = attempts;
    }

    // routing: the bq-job property wins; the body's URN is the fallback. A body with no URN is
    // still rejected below, whatever bq-job says.
    const bodyUrn = EnvelopeCodec.urn(envelope);
    const rawJob: unknown = properties["bq-job"];
    const propertyUrn = typeof rawJob === "string" ? rawJob.trim() : "";
    const urn = propertyUrn !== "" ? propertyUrn : bodyUrn;

    if (!EnvelopeCodec.accepts(envelope)) {
      this.options.onError?.(
        new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Apache Pulsar."),
        envelope,
        message,
      );
      await this.consumer.negativeAcknowledge(message);
      return;
    }

    // Diagnostics only, and only for an accepted envelope (07a Java/.NET order): after accepts()
    // and the attempts reconcile, so the hook sees the same envelope the handler gets.
    if (propertyUrn !== "" && bodyUrn !== "" && propertyUrn !== bodyUrn) {
      this.notifyRoutingMismatch(propertyUrn, bodyUrn, envelope);
    }

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
      await handler(envelope as Envelope, message, headersOf(message));
      await this.consumer.acknowledge(message);
    } catch (error) {
      // Negative-ack releases the message — the broker redelivers and increments the count.
      this.options.onError?.(error, envelope, message);
      await this.consumer.negativeAcknowledge(message);
    }
  }

  private notifyRoutingMismatch(propertyUrn: string, bodyUrn: string, envelope: IncomingEnvelope): void {
    const hook = this.options.onRoutingMismatch;
    if (hook === undefined) return;
    try {
      const result = hook(propertyUrn, bodyUrn, envelope);
      Promise.resolve(result).catch(() => undefined);
    } catch {
      // A diagnostic hook must never disturb consumption.
    }
  }
}

function safeProperties(message: PulsarReceivedMessage): { [key: string]: string } {
  try {
    const properties = typeof message.getProperties === "function" ? message.getProperties() : undefined;
    return properties ?? {};
  } catch {
    return {};
  }
}
