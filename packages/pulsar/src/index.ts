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
 *     }, { subscriptionType: "Shared" }); // declare it (or pass `client` together with `topic`): see PulsarConsumerOptions
 *     await consumer.run();
 *
 * This implements §5 of the broker-bindings contract: the canonical envelope is the message
 * payload, projected onto native Pulsar message properties (string→string) — `bq-job` = URN,
 * `bq-trace-id` = trace_id, `bq-message-id` = meta.id, plus `bq-schema-version` /
 * `bq-source-lang` / `bq-attempts`. The envelope is unchanged (`schema_version` stays 1);
 * Pulsar is purely additive. A failed handler `negativeAcknowledge`s the message, so it is
 * redelivered (at-least-once) and `getRedeliveryCount()` is incremented — up to `maxAttempts`
 * deliveries (default 3, ADR-0036): the message that exhausts them is written to `<queue>.dlq`
 * (when a `deadLetterProducer` is given) or acknowledged and dropped. A body that does not decode
 * into an envelope (including an object with no valid identity) is poison: it never reaches a
 * handler, and with dead-lettering on it is recorded in `<queue>.dlq` in the ADR-0035 shape without
 * retry. Per §5.2 / §5.7 the
 * consumer routes on the `bq-job` property (the body's URN is used only when the property is
 * absent or blank) and takes `attempts` from the authoritative `bq-attempts` property (the
 * body's value is used only when the property is absent or not a plain ASCII-decimal integer
 * that fits a 32-bit int), reconciled to `max(bq-attempts, getRedeliveryCount())` — no −1,
 * because Pulsar's redelivery count is 0-based. The handler still receives the body's envelope.
 *
 * **Version gate (§5.7 step 3).** Before the body is decoded the consumer reads the
 * `bq-schema-version` property. A missing or blank value (empty or only ASCII whitespace) changes nothing; any value other than
 * the core's supported `SCHEMA_VERSION` (`"2"`, `"x"`, `"01"`, ...) rejects the message undecoded:
 * `onError` (with an error naming the version, and an empty envelope since nothing was decoded),
 * then it is settled on its first delivery (ADR-0037 decision 1) — with a `deadLetterProducer` the
 * body, byte for byte, goes to `<queue>.dlq` and the message is acknowledged after the write (a failed
 * write is a `negativeAcknowledge`, never an acknowledgement); without one it is acknowledged and
 * dropped (logged). Never retried, whatever `bq-attempts`, the redelivery count or `maxAttempts` say.
 *
 * **Out-of-band headers (ADR-0028).** A `headers` carrier (e.g. a W3C `traceparent` for cross-hop
 * span linkage) rides as additional native Pulsar **message properties** beside the contract `bq-*`
 * properties — the contract properties win a key collision (merge-not-clobber), and blanks are
 * dropped. On consume the inbound `getProperties()` are surfaced as a `Record<string,string>` so
 * the core's `otel` extract sees the `traceparent` and links the consumer span as a child. A
 * header-less publish is byte-identical. GR-1: the wire envelope (the payload) is never touched.
 */

import { BabelQueueError, EnvelopeCodec, SCHEMA_VERSION, UnknownUrnError, annotate, buildPoisonRecord } from "@babelqueue/core";
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
  /**
   * The topic the subscription reads. pulsar-client's `Consumer` has **no** such method (only its `Producer`
   * does), so with it the topic comes from the {@link PulsarConsumerOptions.topic} option; a custom
   * subscription wrapper may provide this to make `topic` unnecessary.
   */
  getTopic?(): string;
  receive(timeoutMs?: number): Promise<PulsarReceivedMessage>;
  acknowledge(message: PulsarReceivedMessage): Promise<void> | void;
  negativeAcknowledge(message: PulsarReceivedMessage): Promise<void> | void;
}

/**
 * The subset of the Pulsar client this adapter calls to build its own retry producer. A `Client` from
 * `pulsar-client` satisfies it structurally.
 */
export interface PulsarClientLike {
  createProducer(config: { topic: string }): Promise<PulsarProducer>;
}

/** The subscription type, as pulsar-client names it (`Key_Shared` is accepted as an alias of `KeyShared`). */
export type PulsarSubscriptionType = "Exclusive" | "Shared" | "Failover" | "KeyShared" | "Key_Shared";

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
  /**
   * The total number of deliveries a message gets, the first included (default 3; `1` = no retry;
   * ADR-0036 decision 1). A failing handler, an unmapped URN without {@link onUnknownUrn} and a poison
   * body with dead-lettering off are retried (`negativeAcknowledge`) while `reconciled attempts + 1`
   * is below it; the delivery that reaches it is dead-lettered (when {@link deadLetterProducer} is
   * set) or acknowledged and dropped. Must be an integer of at least 1.
   */
  maxAttempts?: number;
  /**
   * A producer on the dead-letter topic (by convention `<queue>.dlq`). Dead-lettering is on exactly
   * when it is set; the SDK does not use Pulsar's native `DeadLetterPolicy`. The original message is
   * acknowledged only after the record was sent; a failed send leaves it negatively acknowledged so
   * it is redelivered and the write is tried again.
   */
  deadLetterProducer?: PulsarProducer;
  /**
   * A producer on the consumer's **own** topic. A retry re-publishes the message to that topic with
   * `bq-attempts` (and the body's `attempts`) raised to the counted value and then acknowledges the
   * original — the Go model. The count then lives in the message itself, so the bound holds on every
   * subscription type. Optional when {@link client} is given (the consumer builds the producer itself);
   * a failed re-publish leaves the original negatively acknowledged (never lost).
   *
   * Without a producer or a client a retry is a `negativeAcknowledge`, which advances the native
   * redelivery count only on `Shared` / `Key_Shared`: on `Exclusive` / `Failover` it stays 0 and the retry
   * would never reach `maxAttempts`, so that combination is rejected at construction (see
   * {@link subscriptionType}).
   */
  retryProducer?: PulsarProducer;
  /**
   * The Pulsar client the consumer builds its retry producer from, lazily on the first retry, on
   * {@link topic}. Needs `topic` (pulsar-client's `Consumer` cannot tell its topic). Ignored when
   * {@link retryProducer} is given.
   */
  client?: PulsarClientLike;
  /**
   * The topic the retry producer is created on — the topic the consumer reads. Required together with
   * {@link client} (a pulsar-client `Consumer` has no `getTopic()`); a subscription wrapper that provides
   * `getTopic()` makes it optional.
   */
  topic?: string;
  /**
   * The subscription type the consumer was created with. `Exclusive` and `Failover` cannot bound a
   * nack-driven retry (a negative acknowledgement does not advance the redelivery count there), so with
   * `maxAttempts > 1` they require a {@link retryProducer} or a {@link client}. **Not declaring it counts as
   * `Exclusive`** (pulsar-client's own default, and the fail-closed choice): without a `retryProducer` or a
   * `client` the constructor throws `BabelQueueError`. Declare `"Shared"` or `"Key_Shared"` to keep the
   * `negativeAcknowledge` retry without a producer.
   */
  subscriptionType?: PulsarSubscriptionType;
  /**
   * The queue name written to `dead_letter.original_queue` of a poison record (a decodable envelope
   * uses its own `meta.queue`). Defaults to the {@link deadLetterProducer} topic without its `.dlq`
   * suffix.
   */
  queue?: string;
  /** Receives the "message dropped" and "dead-letter write failed" warnings (default `console.warn`). */
  onWarn?: (message: string) => void;
}

/** What a retryable outcome is about: the reason written to `dead_letter.reason`. */
type SettleKind = "failed" | "unknown_urn" | "poison";

/** Default total number of deliveries (ADR-0036 decision 1). */
export const DEFAULT_MAX_ATTEMPTS = 3;

/** `bq-attempts` is valid only as ASCII digits that fit a signed 32-bit int (matches Java/.NET, GR-5). */
const MAX_ATTEMPTS = 2 ** 31 - 1;

function parseAttempts(raw: string | undefined): number | undefined {
  if (typeof raw !== "string" || !/^[0-9]+$/.test(raw)) return undefined;
  const value = Number(raw);
  return value <= MAX_ATTEMPTS ? value : undefined;
}

/**
 * The reconciled attempt count of a delivery (broker-bindings §5.5): the `bq-attempts` property when
 * valid, else `bodyAttempts` (0 when the body has none), floored by the 0-based native redelivery count.
 */
function reconcileAttempts(
  properties: { [key: string]: string },
  bodyAttempts: number,
  redeliveryCount: number,
): number {
  let attempts = bodyAttempts;
  const fromProperty = parseAttempts(properties["bq-attempts"]);
  if (fromProperty !== undefined) attempts = fromProperty;
  return redeliveryCount > attempts ? redeliveryCount : attempts;
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
 * `getRedeliveryCount()` (at-least-once; with a `retryProducer` the message is instead re-published with
 * `bq-attempts` + 1, which also bounds `Exclusive` / `Failover` subscriptions) — until `maxAttempts` deliveries are used up; the last one is
 * dead-lettered to `<queue>.dlq` or acknowledged and dropped (ADR-0036). Routing uses the `bq-job`
 * property (body URN as the fallback); `attempts` is the `bq-attempts` property (body value as the
 * fallback) reconciled to `max(.., getRedeliveryCount())` for the handler.
 */
export class PulsarConsumer {
  private readonly maxAttempts: number;
  private builtRetryProducer: Promise<PulsarProducer | undefined> | undefined;
  private readonly warn: (message: string) => void;

  constructor(
    private readonly consumer: PulsarConsumerClient,
    private readonly handlers: BabelHandlers,
    private readonly options: PulsarConsumerOptions = {},
  ) {
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new BabelQueueError(`maxAttempts must be an integer of at least 1, got ${String(maxAttempts)}.`);
    }
    this.maxAttempts = maxAttempts;
    this.checkRetryBound(maxAttempts);
    this.warn = options.onWarn ?? ((message) => console.warn(message));
  }

  /**
   * Setup check: on `Exclusive` / `Failover` (and an undeclared type) a negative acknowledgement does not advance the native
   * redelivery count, so a retry needs a re-publish. Fail loudly here instead of looping at run time.
   */
  private checkRetryBound(maxAttempts: number): void {
    const declared = this.options.subscriptionType;
    if (declared !== undefined && !["Exclusive", "Shared", "Failover", "KeyShared"].includes(declared.replace("_", ""))) {
      throw new BabelQueueError(`Unknown subscriptionType "${String(declared)}".`);
    }
    // An undeclared type is treated as Exclusive (fail closed).
    const type = declared === undefined ? "Exclusive" : declared.replace("_", "");
    if (type !== "Exclusive" && type !== "Failover") return;
    if (maxAttempts === 1 || this.options.retryProducer !== undefined) return;
    if (this.options.client !== undefined && this.retryTopic() !== undefined) return;
    const shown = declared === undefined ? "An undeclared subscriptionType (treated as Exclusive)" : `A ${type} subscription`;
    if (this.options.client !== undefined) {
      // A client alone cannot name the topic: pulsar-client's Consumer has no getTopic().
      throw new BabelQueueError(
        `${shown} with a client needs the \`topic\` option (the topic this consumer reads) to build the retry producer: ` +
          "pulsar-client's Consumer cannot report its own topic.",
      );
    }
    throw new BabelQueueError(
      `${shown} needs a retryProducer (or a client plus a topic) to bound retries: ` +
        "a negative acknowledgement does not advance the redelivery count there, so a retry would never reach maxAttempts. " +
        'Pass a client plus topic (the topic you subscribed to), or a retryProducer, or declare subscriptionType: "Shared" / "Key_Shared".',
    );
  }

  private retryTopic(): string | undefined {
    const configured = this.options.topic;
    if (configured !== undefined && configured !== "") return configured;
    const own = typeof this.consumer.getTopic === "function" ? this.consumer.getTopic() : undefined;
    return own !== undefined && own !== "" ? own : undefined;
  }

  /** The retry producer: the one given, else one built (once) from the client; undefined when neither exists. */
  private async resolveRetryProducer(): Promise<PulsarProducer | undefined> {
    if (this.options.retryProducer !== undefined) return this.options.retryProducer;
    const client = this.options.client;
    const topic = this.retryTopic();
    if (client === undefined || topic === undefined) return undefined;
    this.builtRetryProducer ??= client.createProducer({ topic }).catch((error: unknown) => {
      this.builtRetryProducer = undefined; // try again on the next retry
      this.warn(`babelqueue: creating the retry producer on ${topic} failed (${errorText(error)}).`);
      return undefined;
    });
    return this.builtRetryProducer;
  }

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
      await this.settleVersionGated(message);
      return;
    }

    const redeliveryCount = typeof message.getRedeliveryCount === "function" ? message.getRedeliveryCount() : 0;
    const envelope = EnvelopeCodec.decode(decodePayload(message.getData()));

    // routing: the bq-job property wins; the body's URN is the fallback. A body with no URN is
    // still rejected below, whatever bq-job says.
    const bodyUrn = EnvelopeCodec.urn(envelope);
    const rawJob: unknown = properties["bq-job"];
    const propertyUrn = typeof rawJob === "string" ? rawJob.trim() : "";
    const urn = propertyUrn !== "" ? propertyUrn : bodyUrn;

    if (!EnvelopeCodec.accepts(envelope)) {
      // Poison (ADR-0036 decisions 2 and 4): a body that does not decode into an envelope, including an
      // object with no valid identity, never reaches a handler. Its attempts come from the transport
      // alone (bq-attempts, the redelivery count): there is no decodable body to read them from.
      const error = new BabelQueueError("Rejected a non-conformant BabelQueue envelope from Apache Pulsar.");
      this.options.onError?.(error, envelope, message);
      await this.settle("poison", reconcileAttempts(properties, 0, redeliveryCount) + 1, null, message, error);
      return;
    }

    // attempts: the authoritative bq-attempts property (the body's value when it is absent or
    // invalid), then max(.., getRedeliveryCount()): the redelivery count is 0-based, so it maps
    // directly with no −1; the max never lowers a higher count republished from another SDK
    // (the Go/Python transports retry by re-sending with attempts+1).
    // accepts() above guarantees an integer body attempts; the value is written only on a change.
    const bodyAttempts = envelope.attempts as number;
    const attempts = reconcileAttempts(properties, bodyAttempts, redeliveryCount);
    if (bodyAttempts !== attempts) {
      envelope.attempts = attempts;
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
        const error = new UnknownUrnError(urn);
        this.options.onError?.(error, envelope, message);
        await this.settle("unknown_urn", attempts + 1, envelope as Envelope, message, error);
      }
      return;
    }

    try {
      await handler(envelope as Envelope, message, headersOf(message));
      await this.consumer.acknowledge(message);
    } catch (error) {
      this.options.onError?.(error, envelope, message);
      await this.settle("failed", attempts + 1, envelope as Envelope, message, error);
    }
  }

  /**
   * Settle a version-gated delivery on its first delivery (ADR-0037 decision 1, broker-bindings §5.7): it is
   * never decoded, never reaches a handler or {@link PulsarConsumerOptions.onUnknownUrn}, and is never retried,
   * whatever `bq-attempts`, the redelivery count or `maxAttempts` say. This consumer has no unknown-URN
   * strategy option, so it behaves as `fail`: with a {@link PulsarConsumerOptions.deadLetterProducer} the body is
   * written to `<queue>.dlq` exactly as received (no `dead_letter` block, no re-encoding) and acknowledged only
   * after the write; without one it is acknowledged and dropped (logged). A failed write leaves the delivery
   * negatively acknowledged (never acknowledged, never counted as a retry): the gate rejects the redelivery
   * again, so the write is tried again.
   */
  private async settleVersionGated(message: PulsarReceivedMessage): Promise<void> {
    const producer = this.options.deadLetterProducer;
    if (producer === undefined) {
      this.warn("babelqueue: dropped a version-gated Apache Pulsar message (dead-lettering is off).");
      await this.consumer.acknowledge(message);
      return;
    }
    try {
      const data = message.getData();
      await producer.send({
        data: typeof data === "string" ? Buffer.from(data, "utf8") : Buffer.from(data),
        properties: { ...safeProperties(message) },
      });
    } catch (writeError) {
      this.warn(
        `babelqueue: dead-letter write of a version-gated message to ${producer.getTopic()} failed (${errorText(writeError)}); the message is left for redelivery.`,
      );
      await this.consumer.negativeAcknowledge(message);
      return;
    }
    await this.consumer.acknowledge(message);
  }

  /**
   * The settle decision of a retryable outcome (ADR-0036 decisions 1 and 2). `counted` is the reconciled
   * attempts plus one for this delivery. A poison body with dead-lettering on is recorded without retry;
   * every other outcome retries (`negativeAcknowledge`: the native redelivery count advances, so the
   * next delivery counts one more) while `counted < maxAttempts`, then dead-letters or drops.
   */
  private async settle(
    kind: SettleKind,
    counted: number,
    envelope: Envelope | null,
    message: PulsarReceivedMessage,
    error: unknown,
  ): Promise<void> {
    const producer = this.options.deadLetterProducer;
    if (kind === "poison" && producer !== undefined) {
      await this.deadLetter(producer, kind, counted, null, message, error);
      return;
    }
    if (counted < this.maxAttempts) {
      await this.retry(counted, envelope, message);
      return;
    }
    if (producer !== undefined) {
      await this.deadLetter(producer, kind, counted, envelope, message, error);
      return;
    }
    this.warn(
      `babelqueue: dropped a ${kind} Apache Pulsar message after ${counted} of ${this.maxAttempts} deliveries (dead-lettering is off).`,
    );
    await this.consumer.acknowledge(message);
  }

  /**
   * Retry one delivery. With a {@link PulsarConsumerOptions.retryProducer}: re-publish to the consumer's
   * own topic carrying `bq-attempts = counted` (reconcile = max(bq-attempts, RedeliveryCount) on the next
   * delivery), then acknowledge the original — only after the send succeeded. Otherwise `negativeAcknowledge`.
   */
  private async retry(counted: number, envelope: Envelope | null, message: PulsarReceivedMessage): Promise<void> {
    const producer = await this.resolveRetryProducer();
    if (producer === undefined) {
      await this.consumer.negativeAcknowledge(message);
      return;
    }
    try {
      await producer.send(this.retryMessage(counted, envelope, message));
    } catch (writeError) {
      this.warn(
        `babelqueue: retry re-publish to ${producer.getTopic()} failed (${errorText(writeError)}); the message is left for redelivery.`,
      );
      await this.consumer.negativeAcknowledge(message);
      return;
    }
    await this.consumer.acknowledge(message);
  }

  private retryMessage(counted: number, envelope: Envelope | null, message: PulsarReceivedMessage): PulsarProducerMessage {
    // Out-of-band headers (e.g. a traceparent) ride along; the contract bq-* properties are rebuilt and win,
    // and the one-shot bq-delay of the first publish is not repeated.
    const carried = headersOf(message);
    delete carried["bq-delay"];
    if (envelope !== null) {
      return keepRoute(toPulsarMessage({ ...envelope, attempts: counted }, carried), message);
    }
    // A poison body is retried byte for byte (there is no envelope to re-encode); only the counter moves.
    const data = message.getData();
    return keepRoute(
      {
        data: typeof data === "string" ? Buffer.from(data, "utf8") : Buffer.from(data),
        properties: { ...carried, "bq-attempts": String(counted) },
      },
      message,
    );
  }

  /**
   * Write the DLQ record, then acknowledge the original — only after the send succeeded. A failed send
   * leaves the original negatively acknowledged, so it is redelivered and the write is tried again.
   */
  private async deadLetter(
    producer: PulsarProducer,
    kind: SettleKind,
    counted: number,
    envelope: Envelope | null,
    message: PulsarReceivedMessage,
    error: unknown,
  ): Promise<void> {
    try {
      await producer.send(this.deadLetterMessage(producer, kind, counted, envelope, message, error));
    } catch (writeError) {
      this.warn(
        `babelqueue: dead-letter write to ${producer.getTopic()} failed (${errorText(writeError)}); the message is left for redelivery.`,
      );
      await this.consumer.negativeAcknowledge(message);
      return;
    }
    await this.consumer.acknowledge(message);
  }

  private deadLetterMessage(
    producer: PulsarProducer,
    kind: SettleKind,
    counted: number,
    envelope: Envelope | null,
    message: PulsarReceivedMessage,
    error: unknown,
  ): PulsarProducerMessage {
    const details = {
      attempts: counted,
      error: error instanceof Error ? error.message : null,
      exception: error instanceof Error ? error.name : null,
    };
    if (envelope === null) {
      // ADR-0035: the original bytes are kept; only the dead_letter block is added.
      const original = this.options.queue ?? stripDlqSuffix(topicName(producer.getTopic()));
      // No retry decision was taken, so the value is informational: the delivered count.
      const delivered = counted - 1;
      const deadLetter = annotate({ attempts: delivered } as unknown as Envelope, kind, original, {
        ...details,
        attempts: delivered,
      }).dead_letter;
      const data = message.getData();
      const body = typeof data === "string" ? Buffer.from(data, "utf8") : Buffer.from(data);
      const record = buildPoisonRecord(body, JSON.stringify(deadLetter));
      return keepRoute({ data: Buffer.from(record, "utf8"), properties: poisonProperties(record) }, message);
    }
    // The envelope as the next hop will see it: attempts is the counted value, the same one that was
    // compared against maxAttempts and that dead_letter.attempts records.
    const original = envelope.meta?.queue || this.options.queue || stripDlqSuffix(topicName(producer.getTopic()));
    const annotated = annotate({ ...envelope, attempts: counted }, kind, original, details);
    return keepRoute(toPulsarMessage(annotated), message);
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

/**
 * Keep the route a delivery arrived on: when the incoming message carries a non-blank `bq-job` property
 * (the consumer routes on it, §5.2/§5.7) the re-published retry or DLQ message carries the same value, even
 * if the body names another URN. Otherwise the second delivery could reach a different handler (Go's
 * `publishKeepingRoute`).
 */
function keepRoute(outgoing: PulsarProducerMessage, incoming: PulsarReceivedMessage): PulsarProducerMessage {
  const route: unknown = safeProperties(incoming)["bq-job"];
  if (typeof route === "string" && route.trim() !== "") {
    outgoing.properties = { ...outgoing.properties, "bq-job": route };
  }
  return outgoing;
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** `orders.dlq` -> `orders`: the queue a DLQ topic belongs to (a name without the suffix is kept). */
function stripDlqSuffix(topic: string): string {
  return topic.endsWith(".dlq") && topic.length > 4 ? topic.slice(0, -4) : topic;
}

/**
 * Contract `bq-*` properties for a poison DLQ record, projected from whatever the body carries (a body
 * that is not an object, or carries none of them, yields none).
 */
function poisonProperties(recordJson: string): { [key: string]: string } {
  const properties: { [key: string]: string } = {};
  let record: unknown;
  try {
    record = JSON.parse(recordJson);
  } catch {
    return properties; // nested deeper than JSON.parse can recurse
  }
  if (record === null || typeof record !== "object") return properties;
  const top = record as Record<string, unknown>;
  const str = (value: unknown): string | undefined => (typeof value === "string" && value !== "" ? value : undefined);
  const job = str(top["job"]) ?? str(top["urn"]);
  if (job) properties["bq-job"] = job;
  const trace = str(top["trace_id"]);
  if (trace) properties["bq-trace-id"] = trace;
  const meta = top["meta"] !== null && typeof top["meta"] === "object" ? (top["meta"] as Record<string, unknown>) : {};
  const id = str(meta["id"]);
  if (id) properties["bq-message-id"] = id;
  return properties;
}
