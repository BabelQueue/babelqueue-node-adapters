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
  type KafkaProducerClient,
  type KafkaProducerMessage,
  RetryTopics,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `kafka` block locks the §6
// header projection + the attempts = bq-attempts-header-authoritative-else-body reconciliation.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const kafka = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).kafka as KafkaBlock;

interface GateCase {
  value?: string;
  absent?: boolean;
  expect: "decode" | "reject";
}

interface KafkaBlock {
  schema_version_gate: { property: string; fixture: string; cases: GateCase[] };
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

test("kafka conformance: schema_version_gate decides decode vs reject before the body is decoded", async () => {
  const gate = kafka.schema_version_gate;
  assert.equal(gate.property, "bq-schema-version");
  const body = Buffer.from(readFileSync(join(dir, gate.fixture), "utf8"), "utf8");
  const urn = EnvelopeCodec.urn(EnvelopeCodec.decode(body.toString("utf8")));
  assert.ok(gate.cases.length > 0);

  for (const c of gate.cases) {
    const label = c.absent ? "absent" : JSON.stringify(c.value);
    const headers: IncomingHeaders = {};
    if (!c.absent) headers[gate.property] = Buffer.from(c.value as string, "utf8");
    const payload: EachMessagePayload = { topic: "orders", partition: 0, message: { value: body, headers, offset: "0" } };

    const sent: { topic: string; messages: KafkaProducerMessage[] }[] = [];
    const producer: KafkaProducerClient = {
      async send(record) {
        sent.push(record);
        return {};
      },
    };
    const consumer = new CommitCapturingConsumer();
    let calls = 0;
    let errors = 0;
    await new KafkaConsumer(
      consumer,
      { [urn]: () => void (calls += 1) },
      { producer, retryTopics: new RetryTopics("orders", [5000]), onError: () => void (errors += 1) },
    ).handle(payload);

    assert.equal(consumer.committed.length, 1, label);
    if (c.expect === "decode") {
      assert.equal(calls, 1, label);
      assert.equal(sent.length, 0, label);
    } else {
      assert.equal(calls, 0, label);
      assert.equal(errors, 1, label);
      assert.equal(sent.length, 1, label);
      assert.equal(sent[0]!.topic, "orders.dlq", label);
    }
  }
});
