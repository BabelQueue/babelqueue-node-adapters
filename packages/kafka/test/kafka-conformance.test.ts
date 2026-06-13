import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";

import {
  KafkaConsumer,
  kafkaHeaders,
  type BabelHandlers,
  type EachMessagePayload,
  type IncomingHeaders,
  type KafkaConsumerClient,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `kafka` block locks the §6
// header projection + the attempts = bq-attempts-header-authoritative-else-body reconciliation.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const kafka = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).kafka as KafkaBlock;

interface KafkaBlock {
  property_projection: { envelope_file: string; headers: Record<string, string> };
  attempts_reconciliation: {
    cases: { name: string; body_attempts: number; header_attempts: number | null; expected_attempts: number }[];
  };
}

test("kafka conformance: header projection matches the golden", () => {
  const proj = kafka.property_projection;
  const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(env)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }
  assert.deepEqual(kafkaHeaders(env), proj.headers);
});

/** A consumer that only records the manual commits. */
class CommitCapturingConsumer implements KafkaConsumerClient {
  committed: { topic: string; partition: number; offset: string }[] = [];
  async run(): Promise<void> {}
  async commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void> {
    this.committed.push(...offsets);
  }
}

test("kafka conformance: attempts reconciliation matches the golden", async () => {
  const urn = "urn:babel:orders:created";
  for (const c of kafka.attempts_reconciliation.cases) {
    const base = EnvelopeCodec.make(urn, { x: 1 });
    const body = EnvelopeCodec.encode({ ...base, attempts: c.body_attempts });
    const headers: IncomingHeaders = {};
    if (c.header_attempts !== null) {
      headers["bq-attempts"] = Buffer.from(String(c.header_attempts), "utf8");
    }
    const payload: EachMessagePayload = {
      topic: "orders",
      partition: 0,
      message: { value: Buffer.from(body, "utf8"), headers, offset: "0" },
    };

    let seen = -1;
    const handlers: BabelHandlers = { [urn]: (env) => void (seen = env.attempts ?? -1) };
    await new KafkaConsumer(new CommitCapturingConsumer(), handlers).handle(payload);

    assert.equal(seen, c.expected_attempts, c.name);
  }
});
