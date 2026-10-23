import { LUA, type RedisClient } from "../src/index.js";

/** One recorded `eval`: which transport script ran, with which keys and arguments. */
export interface EvalCall {
  name: keyof typeof LUA;
  keys: string[];
  args: string[];
}

const NAMES = Object.keys(LUA) as Array<keyof typeof LUA>;

/**
 * A scripted ioredis stand-in for the non-Lua logic (routing, retry policy, option handling).
 * The Lua scripts themselves are exercised against a real Redis (`redis-live.test.ts`,
 * `redis-layout.conformance.test.ts`), never against this double.
 */
export class FakeRedis implements RedisClient {
  pushed: { key: string; value: string }[] = [];
  zadded: { key: string; score: number | string; member: string }[] = [];
  calls: EvalCall[] = [];
  blpops: { key: string; timeout: number }[] = [];
  /** Raw bodies the legacy reserve script hands out, in order. */
  legacyReserves: string[] = [];
  /** `[job, reserved]` (or `[job, reserved, poisonFlag]`) tuples the zset reserve script hands out. */
  zsetReserves: Array<[string, string] | [string, string, string]> = [];
  /** What the gate scripts (ack / release / dead-letter) answer. */
  gateReply: number = 1;

  constructor(legacyReserves: string[] = []) {
    this.legacyReserves = [...legacyReserves];
  }

  async rpush(key: string, value: string): Promise<number> {
    this.pushed.push({ key, value });
    return this.pushed.length;
  }

  async zadd(key: string, score: number | string, member: string): Promise<number> {
    this.zadded.push({ key, score, member });
    return 1;
  }

  async blpop(key: string, timeout: number): Promise<[string, string] | null> {
    this.blpops.push({ key, timeout });
    return null;
  }

  async eval(script: string, numKeys: number, ...args: Array<string | number>): Promise<unknown> {
    const name = NAMES.find((n) => LUA[n] === script);
    if (!name) throw new Error("unknown script");
    const keys = args.slice(0, numKeys).map(String);
    const rest = args.slice(numKeys).map(String);
    this.calls.push({ name, keys, args: rest });
    switch (name) {
      case "legacyReserve":
        return this.legacyReserves.length ? this.legacyReserves.shift()! : false;
      case "zsetReserve":
        return this.zsetReserves.length ? this.zsetReserves.shift()! : false;
      case "legacySweep":
      case "zsetMigrate":
        return 0;
      default:
        return this.gateReply;
    }
  }

  called(name: keyof typeof LUA): EvalCall[] {
    return this.calls.filter((c) => c.name === name);
  }
}
