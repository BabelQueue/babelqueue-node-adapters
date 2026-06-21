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
  type Envelope,
  type HeaderCarrier,
  type IncomingEnvelope,
} from "@babelqueue/core";
import type { Job, JobsOptions, Queue } from "bullmq";

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
}

/**
 * Build a BullMQ processor that decodes each job's envelope, validates it, and
 * routes it to the handler registered for its URN. A non-conformant envelope
 * throws (so BullMQ retries / fails the job per its options); an unmapped URN
 * throws unless `onUnknownUrn` is given.
 *
 *     new Worker("orders", processor(handlers), { connection });
 */
export function processor(
  handlers: BabelHandlers,
  options: ProcessorOptions = {},
): (job: Job) => Promise<unknown> {
  return async (job: Job): Promise<unknown> => {
    const envelope = job.data as IncomingEnvelope;

    if (!EnvelopeCodec.accepts(envelope)) {
      throw new BabelQueueError(
        `Rejected a non-conformant BabelQueue envelope on job ${job.id ?? "?"}.`,
      );
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
}
