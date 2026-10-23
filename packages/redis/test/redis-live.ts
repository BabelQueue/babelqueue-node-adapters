// Live-Redis helpers shared by the conformance runner and the integration tests.
//
// Contract (conformance manifest, `redis_layout` RUNNER CONTRACT): with BQ_REQUIRE_REDIS set an
// unreachable Redis is a FAILURE; unset, the live tests are skipped with a log message so a
// laptop without Redis still gets a green `npm test`.
import { Redis } from "ioredis";

import type { RedisClient } from "../src/index.js";

export const REDIS_URL = process.env["BABELQUEUE_REDIS_URL"] ?? "redis://127.0.0.1:6379";
export const REQUIRE_REDIS = (process.env["BQ_REQUIRE_REDIS"] ?? "") !== "";

export function connect(): Redis {
  return new Redis(REDIS_URL, { lazyConnect: true, maxRetriesPerRequest: 1, retryStrategy: () => null });
}

/** An ioredis connection that answered PING, or `null` (after logging why) when none did. */
export async function tryConnect(): Promise<Redis | null> {
  const redis = connect();
  redis.on("error", () => undefined);
  try {
    await redis.connect();
    await redis.ping();
    return redis;
  } catch (error) {
    redis.disconnect();
    const reason = error instanceof Error ? error.message : String(error);
    if (REQUIRE_REDIS) {
      throw new Error(`BQ_REQUIRE_REDIS is set but Redis at ${REDIS_URL} is unreachable: ${reason}`, { cause: error });
    }
    console.log(`# live Redis tests skipped: ${REDIS_URL} unreachable (${reason}); set BQ_REQUIRE_REDIS to make this a failure`);
    return null;
  }
}

/** The ioredis client typed as the package's structural {@link RedisClient}. */
export function asClient(redis: Redis): RedisClient {
  return redis as unknown as RedisClient;
}

let sequence = 0;

/** A queue name unique to this run and call, so concurrent runs never share keys. */
export function uniqueQueue(label: string): string {
  sequence += 1;
  return `bqtest:${process.pid}:${Date.now().toString(36)}:${sequence}:${label}`;
}

/** Delete every key the queue can own (both layouts, DLQ included). */
export async function cleanup(redis: Redis, queue: string, extra: string[] = []): Promise<void> {
  const keys = [
    queue,
    `${queue}:processing`,
    `${queue}:processing:deadlines`,
    `${queue}.dlq`,
    `queues:${queue}`,
    `queues:${queue}:delayed`,
    `queues:${queue}:reserved`,
    `queues:${queue}:notify`,
    ...extra,
  ];
  await redis.del(...keys);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
