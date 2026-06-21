/**
 * NestJS adapter for BabelQueue.
 *
 * `BabelQueueModule.forRoot(...)` provides an injectable {@link BabelQueuePublisher}
 * backed by a BullMQ queue, so NestJS services emit the canonical BabelQueue
 * envelope and interoperate with the PHP/Laravel, Python, Go, Java and .NET SDKs.
 * Consume with a BullMQ worker built from the re-exported {@link processor}.
 *
 *     @Module({ imports: [BabelQueueModule.forRoot({ queue: "orders", connection })] })
 *     export class AppModule {}
 *
 *     constructor(private readonly babelQueue: BabelQueuePublisher) {}
 *     this.babelQueue.publish("urn:babel:orders:created", { order_id: 1042 });
 */

import { publish } from "@babelqueue/bullmq";
import type { HeaderCarrier } from "@babelqueue/core";
import { Module, type DynamicModule, type Provider } from "@nestjs/common";
import { Queue, type ConnectionOptions, type QueueOptions } from "bullmq";

/** DI token for the underlying BullMQ queue. */
export const BABELQUEUE_QUEUE = "BABELQUEUE_QUEUE";

/** Injectable producer: builds the canonical envelope and adds it to the queue. */
export class BabelQueuePublisher {
  constructor(private readonly queue: Queue) {}

  /**
   * Publish a `(urn, data)` message; returns the message id (`meta.id`). An optional out-of-band
   * `headers` carrier (e.g. a W3C `traceparent`, ADR-0028) is carried on the underlying BullMQ
   * job's native `telemetry.metadata` slot — the canonical envelope is never touched (GR-1). Consume
   * with the re-exported `processor`, whose handler receives the carried headers as its third
   * argument.
   */
  publish(
    urn: string,
    data: Record<string, unknown>,
    options: { traceId?: string; headers?: HeaderCarrier } = {},
  ): Promise<string> {
    return publish(this.queue, urn, data, { traceId: options.traceId, headers: options.headers });
  }
}

/** Options for {@link BabelQueueModule.forRoot}. */
export interface BabelQueueModuleOptions {
  /** Queue name (also the BullMQ queue and routing target). */
  queue: string;
  /** BullMQ/ioredis connection options. */
  connection: ConnectionOptions;
  /** Extra BullMQ queue options. */
  queueOptions?: Omit<QueueOptions, "connection">;
}

@Module({})
export class BabelQueueModule {
  /** Register the publisher (and its BullMQ queue) as providers. */
  static forRoot(options: BabelQueueModuleOptions): DynamicModule {
    const queueProvider: Provider = {
      provide: BABELQUEUE_QUEUE,
      useFactory: () =>
        new Queue(options.queue, { connection: options.connection, ...options.queueOptions }),
    };

    const publisherProvider: Provider = {
      provide: BabelQueuePublisher,
      useFactory: (queue: Queue) => new BabelQueuePublisher(queue),
      inject: [BABELQUEUE_QUEUE],
    };

    return {
      module: BabelQueueModule,
      providers: [queueProvider, publisherProvider],
      exports: [BabelQueuePublisher, BABELQUEUE_QUEUE],
    };
  }
}

export { processor } from "@babelqueue/bullmq";
export type { BabelHandler, BabelHandlers } from "@babelqueue/bullmq";
