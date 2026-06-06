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
  type IncomingEnvelope,
} from "@babelqueue/core";
import type { Job, JobsOptions, Queue } from "bullmq";

/** A URN handler. Receives the validated envelope and the raw BullMQ job. */
export type BabelHandler = (envelope: Envelope, job: Job) => unknown | Promise<unknown>;

/** A map of URN → handler. */
export type BabelHandlers = Record<string, BabelHandler>;

/** Options for {@link publish}. */
export interface PublishOptions {
  /** Reuse an existing trace id (trace continuation). */
  traceId?: string;
  /** BullMQ job options (delay, attempts, backoff, …). */
  jobsOptions?: JobsOptions;
}

/**
 * Build the canonical envelope for `(urn, data)` and add it as a BullMQ job (the
 * job name is the URN, the job data is the envelope). Returns the message id
 * (`meta.id`).
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
  await queue.add(urn, envelope, options.jobsOptions);
  return envelope.meta.id;
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

    return handler(envelope, job);
  };
}
