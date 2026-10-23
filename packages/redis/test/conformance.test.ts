import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, before, test } from "node:test";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";
import type { Redis } from "ioredis";

import { ConformanceResults } from "../../../test-support/results.js";
import { RedisPublisher, RedisQueue, ZSET_KEY_PREFIX, zsetKeys } from "../src/index.js";
import type { Reservation, RedisQueueOptions } from "../src/index.js";
import { FakeRedis } from "./fake-redis.js";
import { asClient, cleanup, tryConnect, uniqueQueue } from "./redis-live.js";

// The vendored canonical suite (synced from conformance/): the `redis` payload-identity block, the
// `redis_layout` key names + 15 behavioural blocks, and the pure `redis_delay` score function.
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(packageRoot, "test", "conformance");
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as Manifest;
const results = new ConformanceResults(packageRoot, "redis", ["redis"]);

interface Manifest {
  redis: { payload_identity: { envelope_file: string } };
  redis_layout: Record<string, unknown> & { cases: LayoutNameCase[] };
  redis_delay: { cases: DelayCase[] };
}

interface LayoutNameCase {
  name: string;
  queue: string;
  key_prefix: string;
  expect: { ready: string; delayed: string; reserved: string; notify: string };
}

interface DelayCase {
  name: string;
  now: number;
  delay: number;
  expect_score: number;
}

type Json = unknown;

interface ZsetEntry {
  member?: string;
  member_json?: Json;
  body_json?: Json;
  score: number | string;
}

interface DeadlineEntry {
  token?: string;
  raw?: string;
  member?: string;
  score: number | string;
}

interface Generate {
  count: number;
  member: string;
  score?: number | string;
}

interface State {
  ready?: string[];
  processing?: string[];
  delayed?: ZsetEntry[];
  reserved?: ZsetEntry[];
  deadlines?: DeadlineEntry[];
  notify?: number;
  delayed_generate?: Generate;
  ready_generate?: Generate;
}

interface Expect extends State {
  outcome?: string;
  handler_body?: string;
  reported_attempts?: number;
  ready_count?: number;
  dlq_count?: number;
  tokens_distinct?: boolean;
}

type ReservationRef = string | { token: string; raw: string };

interface Step {
  op: "push_delayed" | "migrate" | "reserve" | "release" | "ack" | "sweep" | "dead_letter";
  now?: number;
  delay?: number;
  body?: string;
  as?: string;
  reservation?: ReservationRef;
  expect?: Expect;
}

interface BehaviourCase {
  name: string;
  layout: "zset" | "legacy";
  queue: string;
  retry_after?: number | null;
  migration_batch_size?: number;
  state: State;
  steps: Step[];
}

const behaviourBlocks = Object.entries(manifest.redis_layout)
  .filter(([key, value]) => key !== "description" && key !== "cases" && typeof value === "object")
  .map(([key, value]) => [key, (value as { cases: BehaviourCase[] }).cases] as const);

let redis: Redis | null = null;

before(async () => {
  redis = await tryConnect();
});

after(async () => {
  results.write();
  if (redis !== null) redis.disconnect();
});

// --- redis: payload identity (no Redis needed) ------------------------------------

test("redis conformance: produce stores the byte-identical envelope (no wrapping)", () =>
  results.run("redis", "payload_identity", async () => {
    const envelope = EnvelopeCodec.decode(readFileSync(join(dir, manifest.redis.payload_identity.envelope_file), "utf8"));
    if (!EnvelopeCodec.accepts(envelope)) {
      throw new Error("conformance fixture is not a conformant envelope");
    }
    const client = new FakeRedis();
    await RedisPublisher.create(client, (envelope as Envelope).meta.queue).publish(
      (envelope as Envelope).job,
      (envelope as Envelope).data,
      { traceId: (envelope as Envelope).trace_id },
    );
    assert.equal(client.pushed.length, 1);
    const stored = EnvelopeCodec.decode(client.pushed[0]!.value) as Envelope;
    assert.equal(stored.job, (envelope as Envelope).job);
    assert.equal(stored.trace_id, (envelope as Envelope).trace_id);
    assert.deepEqual(stored.data, (envelope as Envelope).data);
    assert.ok("job" in JSON.parse(client.pushed[0]!.value) && "meta" in JSON.parse(client.pushed[0]!.value));
  }));

// --- redis_layout.cases: key names (no Redis needed) -------------------------------

manifest.redis_layout.cases.forEach((c, index) => {
  test(`redis_layout keys: ${c.name}`, () =>
    results.run("redis_layout", ConformanceResults.caseId("redis_layout", c, index), () => {
      assert.equal(c.key_prefix, ZSET_KEY_PREFIX);
      const keys = zsetKeys(c.queue, c.key_prefix);
      assert.deepEqual(keys, c.expect);
      assert.deepEqual(zsetKeys(c.queue), c.expect);
      // The queue the transport is asked to use is exactly what the layout is derived from.
      const queue = new RedisQueue(new FakeRedis(), c.queue, { layout: "zset", keyPrefix: c.key_prefix });
      assert.deepEqual(queue.keys, c.expect);
    }));
});

// --- redis_delay: the pure score function (floor(now) + ceil(delay)) ----------------

manifest.redis_delay.cases.forEach((c, index) => {
  test(`redis_delay: ${c.name}`, () =>
    results.run("redis_delay", ConformanceResults.caseId("redis_delay", c, index), async () => {
      const client = new FakeRedis();
      const queue = new RedisQueue(client, "orders", { layout: "zset", clock: () => c.now });
      await queue.pushDelayed("body", c.delay);
      assert.equal(client.zadded.length, 1);
      assert.equal(client.zadded[0]!.score, c.expect_score);
    }));
});

// --- redis_layout behavioural blocks (live Redis) -----------------------------------

function expand(spec: Generate): string[] {
  return Array.from({ length: spec.count }, (_, i) => spec.member.replace("%03d", String(i).padStart(3, "0")));
}

function toScore(score: number | string): number {
  if (score === "+inf" || score === "inf") return Infinity;
  if (score === "-inf") return -Infinity;
  return Number(score);
}

async function zrange(client: Redis, key: string): Promise<Array<{ member: string; score: number }>> {
  const flat = await client.zrange(key, 0, -1, "WITHSCORES");
  const out: Array<{ member: string; score: number }> = [];
  for (let i = 0; i < flat.length; i += 2) out.push({ member: flat[i]!, score: toScore(flat[i + 1]!) });
  return out;
}

class Harness {
  readonly names: string[] = [];
  now = 0;
  readonly warnings: string[] = [];
  readonly queue: RedisQueue;
  readonly name: string;
  readonly reservations = new Map<string, Reservation>();

  constructor(
    private readonly client: Redis,
    private readonly c: BehaviourCase,
  ) {
    this.name = uniqueQueue(c.queue);
    const options: RedisQueueOptions = {
      layout: c.layout,
      clock: () => this.now,
      onWarn: (message) => this.warnings.push(message),
    };
    if (c.retry_after !== undefined) options.retryAfter = c.retry_after;
    if (c.migration_batch_size !== undefined) options.migrationBatchSize = c.migration_batch_size;
    this.queue = new RedisQueue(asClient(client), this.name, options);
  }

  private get keys() {
    return zsetKeys(this.name);
  }

  async seed(): Promise<void> {
    const state = this.c.state;
    const readyKey = this.c.layout === "zset" ? this.keys.ready : this.name;
    const ready = [...(state.ready ?? []), ...(state.ready_generate ? expand(state.ready_generate) : [])];
    if (ready.length > 0) await this.client.rpush(readyKey, ...ready);
    if (this.c.layout === "zset") {
      if ((state.notify ?? 0) > 0) await this.client.rpush(this.keys.notify, ...Array(state.notify).fill("1"));
      await this.seedZset(this.keys.delayed, state.delayed, state.delayed_generate);
      await this.seedZset(this.keys.reserved, state.reserved);
    } else {
      if (state.processing && state.processing.length > 0) await this.client.rpush(`${this.name}:processing`, ...state.processing);
      for (const entry of state.deadlines ?? []) {
        const member = entry.member ?? `${entry.token}:${entry.raw}`;
        await this.client.zadd(`${this.name}:processing:deadlines`, entry.score as number | "+inf", member);
      }
    }
  }

  private async seedZset(key: string, entries?: ZsetEntry[], generate?: Generate): Promise<void> {
    for (const entry of entries ?? []) {
      await this.client.zadd(key, entry.score as number | "+inf", entry.member ?? JSON.stringify(entry.member_json ?? entry.body_json));
    }
    if (generate) {
      for (const member of expand(generate)) await this.client.zadd(key, (generate.score ?? 0) as number | "+inf", member);
    }
  }

  reservationOf(ref: ReservationRef): Reservation {
    if (typeof ref !== "string") return { layout: "legacy", raw: ref.raw, token: ref.token };
    const found = this.reservations.get(ref);
    assert.ok(found, `reservation "${ref}" was never stored by a reserve step`);
    return found;
  }

  async step(step: Step): Promise<void> {
    if (step.now !== undefined) this.now = step.now;
    switch (step.op) {
      case "push_delayed":
        await this.queue.pushDelayed(step.body as string, step.delay as number);
        break;
      case "migrate":
        await this.queue.migrate();
        break;
      case "sweep":
        await this.queue.sweep();
        break;
      case "reserve": {
        const reserved = await this.queue.reserve();
        if (step.as !== undefined && reserved.reservation !== undefined) this.reservations.set(step.as, reserved.reservation);
        const expect = step.expect ?? {};
        if (expect.outcome !== undefined) assert.equal(reserved.outcome, expect.outcome, "reserve outcome");
        if (expect.handler_body !== undefined) assert.equal(reserved.raw, expect.handler_body, "handler body (byte for byte)");
        if (expect.reported_attempts !== undefined) assert.equal(reserved.attempts, expect.reported_attempts, "reported attempts");
        break;
      }
      case "release":
        await this.queue.release(this.reservationOf(step.reservation as ReservationRef), step.delay ?? 0);
        break;
      case "ack":
        await this.queue.ack(this.reservationOf(step.reservation as ReservationRef));
        break;
      case "dead_letter":
        await this.queue.deadLetter(this.reservationOf(step.reservation as ReservationRef));
        break;
    }
    if (step.expect !== undefined) await this.verify(step.expect);
  }

  async verify(expect: Expect): Promise<void> {
    const layout = this.c.layout;
    const readyKey = layout === "zset" ? this.keys.ready : this.name;
    if (expect.ready !== undefined || expect.ready_generate !== undefined) {
      const wanted = [...(expect.ready ?? []), ...(expect.ready_generate ? expand(expect.ready_generate) : [])];
      assert.deepEqual(await this.client.lrange(readyKey, 0, -1), wanted, "ready list");
    }
    if (expect.ready_count !== undefined) assert.equal(await this.client.llen(readyKey), expect.ready_count, "ready_count");
    if (expect.dlq_count !== undefined) assert.equal(await this.client.llen(`${this.name}.dlq`), expect.dlq_count, "dlq_count");
    if (expect.notify !== undefined) assert.equal(await this.client.llen(this.keys.notify), expect.notify, "notify tokens");
    if (expect.processing !== undefined) {
      assert.deepEqual(await this.client.lrange(`${this.name}:processing`, 0, -1), expect.processing, "processing list");
    }
    if (expect.delayed !== undefined) matchZset(await zrange(this.client, this.keys.delayed), expect.delayed, "delayed");
    if (expect.reserved !== undefined) matchZset(await zrange(this.client, this.keys.reserved), expect.reserved, "reserved");
    if (expect.deadlines !== undefined) {
      matchDeadlines(await zrange(this.client, `${this.name}:processing:deadlines`), expect.deadlines, expect.tokens_distinct === true);
    }
  }
}

function takeMatch<T>(actual: T[], predicate: (candidate: T) => boolean, label: string): void {
  const index = actual.findIndex(predicate);
  assert.notEqual(index, -1, `${label}: no remaining member matches the expectation`);
  actual.splice(index, 1);
}

function matchZset(actual: Array<{ member: string; score: number }>, expected: ZsetEntry[], label: string): void {
  assert.equal(actual.length, expected.length, `${label}: member count`);
  const remaining = [...actual];
  for (const entry of expected) {
    takeMatch(
      remaining,
      (candidate) => {
        if (candidate.score !== toScore(entry.score)) return false;
        if (entry.member !== undefined) return candidate.member === entry.member;
        try {
          return isDeepStrictEqual(JSON.parse(candidate.member), entry.member_json ?? entry.body_json);
        } catch {
          return false;
        }
      },
      label,
    );
  }
}

function matchDeadlines(actual: Array<{ member: string; score: number }>, expected: DeadlineEntry[], distinct: boolean): void {
  assert.equal(actual.length, expected.length, "deadlines: member count");
  const remaining = [...actual];
  const tokens = new Set<string>();
  for (const entry of expected) {
    takeMatch(
      remaining,
      (candidate) => {
        if (candidate.score !== toScore(entry.score)) return false;
        if (entry.member !== undefined) return candidate.member === entry.member;
        const raw = entry.raw as string;
        if (!candidate.member.endsWith(`:${raw}`)) return false;
        const token = candidate.member.slice(0, candidate.member.length - raw.length - 1);
        if (entry.token === "<any>") {
          if (!/^[0-9a-f]{32}$/.test(token)) return false;
          tokens.add(token);
          return true;
        }
        return token === entry.token;
      },
      "deadlines",
    );
  }
  if (distinct) assert.equal(tokens.size, expected.length, "deadline tokens must be pairwise distinct");
}


for (const [block, cases] of behaviourBlocks) {
  cases.forEach((c, index) => {
    test(`redis_layout ${block}: ${c.name}`, async (t) => {
      // A sub-key with its own `cases` array yields `<sub-key>/<name>` (conformance README, case ids).
      const id = `${block}/${typeof c.name === "string" ? c.name : index}`;
      if (redis === null) {
        results.record("redis_layout", id, { status: "skip", reason: "live Redis unreachable and BQ_REQUIRE_REDIS unset" });
        t.skip("live Redis unreachable");
        return;
      }
      const client = redis;
      const harness = new Harness(client, c);
      try {
        await results.run("redis_layout", id, async () => {
          await harness.seed();
          for (const step of c.steps) await harness.step(step);
        });
      } finally {
        await cleanup(client, harness.name);
      }
    });
  });
}
