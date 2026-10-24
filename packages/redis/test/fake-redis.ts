import { LUA, type RedisClient } from "../src/index.js";

/** One recorded `eval`: which transport script ran, with which keys and arguments. */
export interface EvalCall {
  name: keyof typeof LUA;
  keys: string[];
  args: string[];
  /** The arguments exactly as sent (a `Buffer` stays a `Buffer`); `args` is their lossy text form. */
  rawArgs: Array<string | number | Buffer>;
  /** Whether the call went through `evalBuffer`. */
  binary: boolean;
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
  /** Raw bodies the legacy reserve script hands out, in order (a `Buffer` models bytes that are not UTF-8). */
  legacyReserves: Array<string | Buffer> = [];
  /** `[job, reserved]` (or `[job, reserved, poisonFlag]`) tuples the zset reserve script hands out. */
  zsetReserves: Array<
    [string | Buffer, string | Buffer] | [string | Buffer, string | Buffer, string]
  > = [];
  /** What the gate scripts (ack / release / dead-letter) answer. */
  gateReply: number = 1;

  constructor(legacyReserves: Array<string | Buffer> = []) {
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
    return this.answer(script, numKeys, args, false);
  }

  /** ioredis `evalBuffer`: string replies come back as `Buffer`s, `Buffer` arguments are kept as sent. */
  async evalBuffer(script: string, numKeys: number, ...args: Array<string | number | Buffer>): Promise<unknown> {
    return this.answer(script, numKeys, args, true);
  }

  private answer(script: string, numKeys: number, args: Array<string | number | Buffer>, binary: boolean): unknown {
    const name = NAMES.find((n) => LUA[n] === script);
    if (!name) throw new Error("unknown script");
    const text = (value: string | number | Buffer): string => (Buffer.isBuffer(value) ? value.toString("utf8") : String(value));
    const keys = args.slice(0, numKeys).map(text);
    const rest = args.slice(numKeys).map(text);
    this.calls.push({ name, keys, args: rest, rawArgs: args.slice(numKeys), binary });
    const out = (value: string | Buffer): string | Buffer => (binary && typeof value === "string" ? Buffer.from(value, "utf8") : value);
    switch (name) {
      case "legacyReserve":
        return this.legacyReserves.length ? out(this.legacyReserves.shift()!) : false;
      case "zsetReserve":
        return this.zsetReserves.length ? this.zsetReserves.shift()!.map(out) : false;
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
