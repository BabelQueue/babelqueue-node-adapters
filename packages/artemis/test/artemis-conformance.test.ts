import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import { ConformanceResults } from "../../../test-support/results.js";

import {
  ArtemisConsumer,
  artemisMessage,
  JMS_TYPE_KEY,
  type AmqpMessage,
  type AmqpSender,
  type BabelHandlers,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `artemis` block locks the §7
// AMQP projection (the x-opt-jms-type annotation / correlation-id + the bq_ application
// properties) + the attempts = max(body, delivery-count) reconciliation (0-based, no −1).
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as {
  artemis: ArtemisBlock;
  poison_dlq: { original_queue: string; cases: PoisonDlqCase[] };
  version_gate_artemis: { cases: VersionGateCase[]; incoming_bodies: { name: string; body: string }[] };
};
const artemis = manifest.artemis;
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const results = new ConformanceResults(packageRoot, "artemis", ["artemis"]);

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

interface VersionGateCase {
  name: string;
  on_unknown_urn?: string;
  dead_letter: "on" | "off";
  dlq_write?: "ok" | "fails";
  bq_attempts?: string;
  expect: { outcome: "ack" | "dlq_raw" | "unsettled" | "native"; record?: string; outcome_no_writer?: string };
}

interface GateCase {
  value?: string | null;
  absent?: boolean;
  expect: "decode" | "reject";
}

interface ArtemisBlock {
  schema_version_gate: { property: string; fixture: string; cases: GateCase[] };
  property_projection: {
    envelope_file: string;
    jms_type: string;
    correlation_id: string;
    properties: Record<string, string>;
  };
  attempts_reconciliation: {
    cases: { name: string; body_attempts: number; delivery_count: number; expected_attempts: number }[];
  };
}

test("artemis conformance: projection matches the golden", () => {
  const proj = artemis.property_projection;
  const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(env)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }
  const message = artemisMessage(env as Envelope);
  assert.equal(message.message_annotations?.[JMS_TYPE_KEY], proj.jms_type);
  assert.equal(message.correlation_id, proj.correlation_id);
  assert.deepEqual(message.application_properties, proj.properties);
});

test("artemis conformance: attempts reconciliation matches the golden", async () => {
  const urn = "urn:babel:orders:created";
  for (const c of artemis.attempts_reconciliation.cases) {
    const base = EnvelopeCodec.make(urn, { x: 1 });
    const body = EnvelopeCodec.encode({ ...base, attempts: c.body_attempts });
    const message: AmqpMessage = { body, message_annotations: { [JMS_TYPE_KEY]: urn } };
    if (c.delivery_count) message.delivery_count = c.delivery_count;

    let seen = -1;
    const handlers: BabelHandlers = { [urn]: (env) => void (seen = env.attempts ?? -1) };
    await new ArtemisConsumer(handlers, { maxTries: 99 }).handle({
      message,
      delivery: { accept() {}, release() {} },
    });

    assert.equal(seen, c.expected_attempts, c.name);
  }
});

test("artemis conformance: schema_version_gate decides decode vs reject before the body is decoded", async () => {
  const gate = artemis.schema_version_gate;
  assert.equal(gate.property, "bq_schema_version");
  const body = readFileSync(join(dir, gate.fixture), "utf8");
  const urn = EnvelopeCodec.urn(EnvelopeCodec.decode(body));
  assert.ok(gate.cases.length > 0);

  for (const c of gate.cases) {
    const label = c.absent ? "absent" : JSON.stringify(c.value);
    const message: AmqpMessage = { body, message_annotations: { [JMS_TYPE_KEY]: urn } };
    if (!c.absent) message.application_properties = { [gate.property]: c.value };

    const dead: AmqpMessage[] = [];
    const dlq: AmqpSender = { send: (m) => void dead.push(m) };
    let accepted = 0;
    let released = 0;
    let calls = 0;
    let errors = 0;
    await new ArtemisConsumer(
      { [urn]: () => void (calls += 1) },
      { deadLetterSender: dlq, onError: () => void (errors += 1) },
    ).handle({
      message,
      delivery: { accept: () => void (accepted += 1), release: () => void (released += 1) },
    });

    assert.equal(accepted, 1, label);
    assert.equal(released, 0, label);
    if (c.expect === "decode") {
      assert.equal(calls, 1, label);
      assert.equal(dead.length, 0, label);
    } else {
      assert.equal(calls, 0, label);
      assert.equal(errors, 1, label);
      assert.equal(dead.length, 1, label);
      assert.equal(dead[0]!.body, body, label);
    }
  }
});

// --- poison_dlq: the DLQ record of a poison body, written by the consumer (ADR-0035, ADR-0037 decision 2) -------

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

test("artemis conformance: poison_dlq records written by the consumer have the ADR-0035 shape", async () => {
  const section = manifest.poison_dlq;
  assert.ok(section.cases.length > 0);
  for (const [index, c] of section.cases.entries()) {
    await results.run("poison_dlq", ConformanceResults.caseId("poison_dlq", c, index), async () => {
      const body = c.body_base64 !== undefined ? Buffer.from(c.body_base64, "base64") : Buffer.from(c.body ?? "", "utf8");
      const sent: AmqpMessage[] = [];
      const dlq: AmqpSender = { send: (m) => void sent.push(m) };
      let accepted = 0;
      let released = 0;
      await new ArtemisConsumer({}, { deadLetterSender: dlq, queue: section.original_queue, onError: () => undefined }).handle({
        message: { body },
        delivery: { accept: () => void (accepted += 1), release: () => void (released += 1) },
      });
      // A poison body is never handed to a handler and never retried: written once, accepted after the write.
      assert.equal(sent.length, 1, c.name);
      assert.equal(accepted, 1, c.name);
      assert.equal(released, 0, c.name);

      const record = sent[0]!.body as string;
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
      // splice: the record is the original bytes plus the dead_letter member
      const text = new TextDecoder("utf-8", { fatal: true }).decode(body);
      assert.equal("raw" in parsed || "raw_encoding" in parsed, false, c.name);
      if (shape === "splice") assert.deepEqual(rest, expected, c.name);
      for (const literal of c.expect.splice_literals ?? []) {
        assert.ok(record.includes(literal), `${c.name}: missing literal ${literal}`);
      }
      if (c.expect.splice_keys !== undefined) {
        assert.deepEqual(topLevelNames(record).filter((k) => k !== "dead_letter").sort(), [...c.expect.splice_keys].sort(), c.name);
      }
      assert.ok(record.startsWith(text.trim().slice(0, -1).trimEnd()), c.name);
    });
  }
});

// --- version_gate_artemis (ADR-0037 decision 1, broker-bindings §7.7) -------------------------------------------

test("artemis conformance: version_gate_artemis settles on the first delivery, undecoded", async () => {
  const section = manifest.version_gate_artemis;
  assert.ok(section.cases.length > 0);
  assert.ok(section.incoming_bodies.length > 0);

  for (const [index, c] of section.cases.entries()) {
    await results.run("version_gate_artemis", ConformanceResults.caseId("version_gate_artemis", c, index), async () => {
      // `dead_letter: off` means no DLQ sender exists in this consumer, so the no-writer outcome applies.
      const outcome = c.dead_letter === "off" ? (c.expect.outcome_no_writer ?? c.expect.outcome) : c.expect.outcome;

      for (const incoming of section.incoming_bodies) {
        const message: AmqpMessage = {
          body: incoming.body,
          application_properties: { bq_schema_version: "2", ...(c.bq_attempts === undefined ? {} : { bq_attempts: c.bq_attempts }) },
          delivery_count: 5,
        };
        const sent: AmqpMessage[] = [];
        const dlq: AmqpSender = {
          send: (m) => {
            if (c.dlq_write === "fails") throw new Error("dlq write failed");
            sent.push(m);
            return {};
          },
        };
        let accepted = 0;
        const released: unknown[] = [];
        let handled = 0;
        await new ArtemisConsumer(
          { "urn:babelqueue:order.created": () => void (handled += 1) },
          {
            ...(c.dead_letter === "on" ? { deadLetterSender: dlq, deadLetterRetryDelayMs: 0 } : {}),
            ...(c.on_unknown_urn === undefined ? {} : { unknownUrn: c.on_unknown_urn }),
            onError: () => undefined,
          },
        ).handle({
          message,
          delivery: { accept: () => void (accepted += 1), release: (p) => void released.push(p ?? {}) },
        });

        const label = `${c.name} / ${incoming.name}`;
        assert.equal(handled, 0, `${label}: never decoded, no handler`);
        switch (outcome) {
          case "ack":
            assert.equal(accepted, 1, label);
            assert.equal(released.length, 0, label);
            assert.equal(sent.length, 0, label);
            break;
          case "dlq_raw":
            assert.equal(accepted, 1, `${label}: accepted after the write`);
            assert.equal(released.length, 0, label);
            assert.equal(sent.length, 1, label);
            assert.equal(sent[0]!.body, incoming.body, `${label}: the record is the incoming body, byte for byte`);
            break;
          case "unsettled":
            assert.equal(accepted, 0, `${label}: never accepted`);
            assert.deepEqual(released, [{}], `${label}: plain release (recover), not a delivery failure`);
            assert.equal(sent.length, 0, label);
            break;
          default:
            assert.fail(`${label}: unexpected outcome ${outcome}`);
        }
      }
    });
  }
});
