import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

import { buildPoisonRecord } from "@babelqueue/core";
import type { Job, Queue } from "bullmq";

import { ConformanceResults } from "../../../test-support/results.js";
import { processor } from "../src/index.js";

// The vendored canonical suite (synced from conformance/). `@babelqueue/bullmq` runs the `poison_dlq` section
// (ADR-0035 record shape, required by ADR-0037): a poison job's record, as added to the `deadLetterQueue`.
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(packageRoot, "test", "conformance");
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as {
  poison_dlq: { original_queue: string; cases: PoisonDlqCase[] };
};
const results = new ConformanceResults(packageRoot, "bullmq", ["bullmq"]);

after(() => {
  results.write();
});

interface PoisonDlqCase {
  name: string;
  body?: string;
  body_base64?: string;
  expect: {
    shape: "splice" | "splice_or_wrapper" | "wrapper";
    record: Record<string, unknown>;
    splice_literals?: string[];
    splice_keys?: string[];
  };
}

/** Distinct top-level member names of a JSON object text (a repeated name counts once). */
function topLevelNames(text: string): string[] {
  const names = new Set<string>();
  let depth = 0;
  let expectKey = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (c === "{" || c === "[") {
      depth++;
      expectKey = c === "{" && depth === 1;
    } else if (c === "}" || c === "]") {
      depth--;
    } else if (c === ",") {
      expectKey = depth === 1;
    } else if (c === '"') {
      let j = i + 1;
      while (text[j] !== '"') j += text[j] === "\\" ? 2 : 1;
      if (depth === 1 && expectKey) {
        names.add(JSON.parse(text.slice(i, j + 1)) as string);
        expectKey = false;
      }
      i = j;
    }
  }
  return [...names];
}

/**
 * The "body" the adapter feeds to the ADR-0035 rule is the job data serialised as JSON, and BullMQ has already parsed
 * what it stored. A conformance body is therefore delivered through the processor only when it is exactly what
 * `JSON.stringify` would write for the job data it parses to (a body that is not JSON, or that carries formatting,
 * escapes or a number `JSON.parse` cannot carry, cannot reach the adapter as job data: a property of BullMQ, not of
 * the record rule). Every other body is checked through the same core builder the adapter calls.
 */
function viaProcessor(body: string): { data: unknown } | undefined {
  let data: unknown;
  try {
    data = JSON.parse(body);
  } catch {
    return undefined;
  }
  return (JSON.stringify(data) ?? "null") === body ? { data } : undefined;
}

test("bullmq conformance: poison_dlq records have the ADR-0035 shape", async () => {
  const section = manifest.poison_dlq;
  assert.ok(section.cases.length > 0);
  let driven = 0;
  for (const [index, c] of section.cases.entries()) {
    const id = ConformanceResults.caseId("poison_dlq", c, index);
    const delivered = c.body_base64 === undefined ? viaProcessor(c.body ?? "") : undefined;
    const execute = async (): Promise<void> => {
      const body = c.body_base64 !== undefined ? Buffer.from(c.body_base64, "base64") : Buffer.from(c.body ?? "", "utf8");

      let record: string;
      if (delivered !== undefined) {
        driven += 1;
        const added: Record<string, unknown>[] = [];
        const dlq = { async add(_name: string, data: Record<string, unknown>) { added.push(data); return {}; } };
        const job = { id: "1", name: "j", queueName: section.original_queue, attemptsMade: 0, data: delivered.data } as unknown as Job;
        await assert.rejects(() => processor({}, { deadLetterQueue: dlq as unknown as Queue })(job), { name: "UnrecoverableError" }, c.name);
        assert.equal(added.length, 1, c.name);
        record = JSON.stringify(added[0]);
      } else {
        const deadLetter = { reason: "poison", error: "e", exception: "BabelQueueError", failed_at: 1, original_queue: section.original_queue, attempts: 0, lang: "node" };
        record = buildPoisonRecord(body, JSON.stringify(deadLetter));
      }

      const parsed = JSON.parse(record) as Record<string, unknown>;
      const deadLetter = parsed["dead_letter"] as Record<string, unknown>;
      assert.equal(deadLetter["reason"], "poison", c.name);
      assert.equal(deadLetter["original_queue"], section.original_queue, c.name);
      const rest = { ...parsed };
      delete rest["dead_letter"];
      const { shape, record: expected } = c.expect;
      const wrapperMatches = ((): boolean => {
        try {
          assert.deepEqual(rest, expected);
          return true;
        } catch {
          return false;
        }
      })();
      if (shape === "wrapper" || (shape === "splice_or_wrapper" && wrapperMatches && "raw" in rest)) {
        assert.deepEqual(rest, expected, c.name);
        return;
      }
      // splice: the record is the original bytes plus the dead_letter member (through the processor the record
      // was re-parsed by JSON.parse, so the text comparison below is only made for the builder path)
      const text = new TextDecoder("utf-8", { fatal: true }).decode(body);
      assert.equal("raw" in parsed || "raw_encoding" in parsed, false, c.name);
      if (shape === "splice") assert.deepEqual(rest, expected, c.name);
      if (delivered === undefined) {
        for (const literal of c.expect.splice_literals ?? []) {
          assert.ok(record.includes(literal), `${c.name}: missing literal ${literal}`);
        }
        if (c.expect.splice_keys !== undefined) {
          assert.deepEqual(topLevelNames(record).filter((k) => k !== "dead_letter").sort(), [...c.expect.splice_keys].sort(), c.name);
        }
        assert.ok(record.startsWith(text.trim().slice(0, -1).trimEnd()), c.name);
      }
    };

    if (delivered !== undefined) {
      await results.run("poison_dlq", id, execute);
    } else {
      // Not a consumer result: this body cannot reach the adapter as BullMQ job data, so the same core builder the adapter
      // calls is checked for it (a failure still fails the test), but the case is reported as skipped, never as passed.
      await execute();
      results.record("poison_dlq", id, { status: "skip", reason: "ADR-0037 decision 3: not representable as BullMQ job data" });
    }
  }
  assert.ok(driven > 0, "at least one case must run through the processor");
});
