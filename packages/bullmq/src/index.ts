/**
 * BullMQ adapter for BabelQueue.
 *
 * It makes your BullMQ jobs carry the canonical BabelQueue envelope and routes
 * them by URN, so a BullMQ-based Node service speaks the same contract (envelope
 * shape, URN identity, trace propagation) as the PHP/Laravel, Python, Go, Java and
 * .NET SDKs.
 *
 *     import { Queue, Worker } from "bullmq";
 *     import { publish, processor } from "@babelqueue/bullmq";
 *
 *     const queue = new Queue("orders", { connection });
 *     await publish(queue, "urn:babel:orders:created", { order_id: 1042 });
 *
 *     new Worker("orders", processor({
 *       "urn:babel:orders:created": async (env) => { ... },
 *     }), { connection });
 */

import {
  BabelQueueError,
  EnvelopeCodec,
  UnknownUrnError,
  annotate,
  buildPoisonRecord,
  type Envelope,
  type HeaderCarrier,
  type IncomingEnvelope,
} from "@babelqueue/core";
import { Queue, UnrecoverableError } from "bullmq";
import type { Job, JobsOptions, QueueOptions } from "bullmq";

import { decodeMetadata, encodeMetadata } from "./headers.js";

export { decodeMetadata, encodeMetadata } from "./headers.js";

/**
 * The out-of-band {@link HeaderCarrier} carried by a job, read from BullMQ's native
 * `telemetry.metadata` slot (`job.opts.telemetry.metadata`). Empty for a job published without
 * headers. Pass it to `@babelqueue/core/otel`'s `wrapHandler` to link the consumer span as a child
 * of the producer span (ADR-0028).
 */
export function headersOf(job: Job): HeaderCarrier {
  return decodeMetadata(job.opts?.telemetry?.metadata);
}

/**
 * A URN handler. Receives the validated envelope, the raw BullMQ job, and the out-of-band
 * {@link HeaderCarrier} carried in the job's `telemetry.metadata` (empty when there are none).
 */
export type BabelHandler = (envelope: Envelope, job: Job, headers: HeaderCarrier) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link publish}. */
export interface PublishOptions {
  /** Reuse an existing trace id (trace continuation). */
  traceId?: string;
  /** BullMQ job options (delay, attempts, backoff, …). */
  jobsOptions?: JobsOptions;
  /**
   * Out-of-band transport headers carried in BullMQ's native `telemetry.metadata` slot (ADR-0028) —
   * e.g. a W3C `traceparent` written by `@babelqueue/core/otel`'s `publish`. The canonical envelope
   * (`job.data`) stays byte-identical (GR-1); an empty/omitted carrier leaves the job options
   * unchanged. An explicit `jobsOptions.telemetry.metadata` wins (it is not clobbered).
   */
  headers?: HeaderCarrier;
}

/**
 * Build the canonical envelope for `(urn, data)` and add it as a BullMQ job (the
 * job name is the URN, the job data is the envelope). Returns the message id
 * (`meta.id`).
 *
 * When `options.headers` carries any out-of-band header (e.g. a `traceparent`) it is serialized
 * into the job's `telemetry.metadata` so a consumer can read it back via {@link headersOf} — the
 * envelope itself is never touched.
 */
export async function publish(
  queue: Queue,
  urn: string,
  data: Record<string, unknown>,
  options: PublishOptions = {},
): Promise<string> {
  const envelope = EnvelopeCodec.make(urn, data, {
    queue: queue.name,
    traceId: options.traceId,
  });
  await queue.add(urn, envelope, withHeaders(options.jobsOptions, options.headers));
  return envelope.meta.id;
}

/**
 * Merge an out-of-band header carrier into a job's `telemetry.metadata` without clobbering an
 * explicit one the caller already set. Returns the original options unchanged when there is nothing
 * to carry, so a header-less publish stays byte-identical.
 */
function withHeaders(jobsOptions: JobsOptions | undefined, headers: HeaderCarrier | undefined): JobsOptions | undefined {
  const metadata = encodeMetadata(headers);
  if (metadata === undefined) return jobsOptions;
  const telemetry = jobsOptions?.telemetry;
  if (telemetry?.metadata != null) return jobsOptions; // an explicit metadata wins (merge-not-clobber)
  return { ...jobsOptions, telemetry: { ...telemetry, metadata } };
}

/** Options for {@link processor}. */
export interface ProcessorOptions {
  /** Called instead of throwing when a job's URN has no handler. */
  onUnknownUrn?: (envelope: IncomingEnvelope, job: Job) => unknown | Promise<unknown>;
  /**
   * A dead-letter queue for **poison** jobs (ADR-0037 decision 3; opt-in, absent = none): a job whose data does
   * not decode into a valid envelope. When set, the ADR-0035 record (`reason: "poison"`, built with the core's
   * `buildPoisonRecord`) is **also** added to it as a job before the job fails.
   *
   * - a BullMQ `Queue` instance is used as given;
   * - a `string` is a queue name: the adapter creates a `Queue` with that name on the worker's connection;
   * - `true` creates a `Queue` named `<queue>.dlq` (`<queue>` = the worker's queue name) on the worker's connection.
   *
   * A `Queue` the adapter creates is closed by the `close()` of the returned processor; a `Queue` instance you pass is yours to close.
   * The job data is JSON that BullMQ has already parsed, so a value `JSON.parse` cannot carry (`1e400`, an integer
   * beyond 2^53) is lost before the adapter sees it: a property of BullMQ, not of the record rule.
   */
  deadLetterQueue?: Queue | string | true;
  /** The `dead_letter.original_queue` of a poison record. Defaults to the worker's queue name (`job.queueName`). */
  queue?: string;
  /** Called when adding the poison record to {@link deadLetterQueue} fails. The job still fails as unrecoverable. */
  onError?: (error: unknown, job: Job) => void;
}

/** The worker's `Queue` connection options and prefix, read from the job's queue (BullMQ keeps them on `queue.opts`). */
function workerQueueOptions(job: Job): QueueOptions {
  const opts = (job as unknown as { queue?: { opts?: Partial<QueueOptions> } }).queue?.opts;
  return { connection: opts?.connection as QueueOptions["connection"], ...(opts?.prefix === undefined ? {} : { prefix: opts.prefix }) };
}

/**
 * Build a BullMQ processor that decodes each job's envelope, validates it, and
 * routes it to the handler registered for its URN. A **poison** job (data that is
 * not a valid envelope) fails with BullMQ's `UnrecoverableError` — no retry, straight
 * to the failed set (ADR-0037 decision 3), after the ADR-0035 record was added to the
 * optional `deadLetterQueue`; an unmapped URN throws unless `onUnknownUrn` is given
 * (BullMQ retries / fails the job per its options), as does a throwing handler.
 *
 *     new Worker("orders", processor(handlers), { connection });
 */
export function processor(
  handlers: BabelHandlers,
  options: ProcessorOptions = {},
): ((job: Job) => Promise<unknown>) & { close(): Promise<void> } {
  // One adapter-created Queue per (prefix, name): a processor shared by several Workers must not send queue B's poison
  // records to queue A's `<queue>.dlq` (ADR-0037 decision 3: `<queue>` is the *worker's* queue name).
  const created = new Map<string, Queue>();

  const resolveDeadLetterQueue = (job: Job): Queue | undefined => {
    const target = options.deadLetterQueue;
    if (target === undefined) return undefined;
    if (typeof target === "object") return target;
    const name = target === true ? `${job.queueName}.dlq` : target;
    const queueOptions = workerQueueOptions(job);
    const key = `${queueOptions.prefix ?? ""}\u0000${name}`;
    let queue = created.get(key);
    if (queue === undefined) {
      queue = new Queue(name, queueOptions);
      created.set(key, queue);
    }
    return queue;
  };

  /** Add the ADR-0035 record to the DLQ; a failure is reported and never replaces the unrecoverable failure. */
  const deadLetterPoison = async (job: Job, error: BabelQueueError): Promise<void> => {
    try {
      const dlq = resolveDeadLetterQueue(job);
      if (dlq === undefined) return;
      // The body is the job data serialised as JSON (BullMQ already parsed what it stored).
      const body = JSON.stringify(job.data) ?? "null";
      const delivered = typeof job.attemptsMade === "number" ? job.attemptsMade : 0; // informational
      const deadLetter = annotate({ attempts: delivered } as unknown as Envelope, "poison", options.queue ?? job.queueName, {
        attempts: delivered,
        error: error.message,
        exception: error.name,
      }).dead_letter;
      const record = buildPoisonRecord(Buffer.from(body, "utf8"), JSON.stringify(deadLetter));
      await dlq.add(job.name, JSON.parse(record) as Record<string, unknown>);
    } catch (writeError) {
      try {
        options.onError?.(writeError, job);
      } catch {
        // a throwing report must not replace the unrecoverable failure
      }
    }
  };

  const run = async (job: Job): Promise<unknown> => {
    const envelope = job.data as IncomingEnvelope;

    if (!EnvelopeCodec.accepts(envelope)) {
      const error = new BabelQueueError(
        `Rejected a non-conformant BabelQueue envelope on job ${job.id ?? "?"}.`,
      );
      await deadLetterPoison(job, error);
      // Never retried: BullMQ moves an UnrecoverableError straight to the failed set, which keeps the job data.
      throw new UnrecoverableError(error.message);
    }

    const urn = EnvelopeCodec.urn(envelope);
    const handler = handlers[urn];
    if (!handler) {
      if (options.onUnknownUrn) {
        return options.onUnknownUrn(envelope, job);
      }
      throw new UnknownUrnError(urn);
    }

    return handler(envelope, job, headersOf(job));
  };

  // Closes the `Queue` the adapter created for a `string` / `true` deadLetterQueue (a no-op otherwise: a
  // `Queue` instance you passed is yours to close).
  return Object.assign(run, {
    async close(): Promise<void> {
      const queues = [...created.values()];
      created.clear();
      await Promise.all(queues.map((queue) => queue.close()));
    },
  });
}
