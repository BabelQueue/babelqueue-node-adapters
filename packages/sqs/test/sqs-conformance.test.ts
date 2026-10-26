import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec, buildPoisonRecord } from "@babelqueue/core";

import { ConformanceResults } from "../../../test-support/results.js";
import { SqsConsumer, toMessageAttributes, type SendMessageInput, type SqsApi, type SqsMessage } from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `sqs` block locks the
// §3 attribute projection + the attempts = ApproximateReceiveCount − 1 reconciliation.
// Resolve via import.meta.url (not import.meta.dirname, which is Node 20.11+) so the
// suite stays on the supported Node 18+ floor.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as {
  sqs: SqsBlock;
  poison_dlq: { original_queue: string; cases: PoisonDlqCase[] };
  version_gate_sqs: { cases: VersionGateCase[]; incoming_bodies: { name: string; body: string }[] };
};
const sqs = manifest.sqs;
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const results = new ConformanceResults(packageRoot, "sqs", ["sqs"]);

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

interface GoldenAttr {
  DataType: string;
  StringValue: string;
}
interface GateCase {
  value?: string;
  absent?: boolean;
  expect: "decode" | "reject";
}
interface SqsBlock {
  schema_version_gate: { property: string; fixture: string; cases: GateCase[] };
  attribute_projection: { envelope_file: string; message_attributes: Record<string, GoldenAttr> };
  attempts_reconciliation: {
    cases: { name: string; body_attempts: number; approximate_receive_count: string | null; expected_attempts: number }[];
  };
}

test("sqs conformance: attribute projection matches the golden", () => {
  const proj = sqs.attribute_projection;
  const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(env)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }
  const got = toMessageAttributes(env);

  assert.deepEqual(Object.keys(got).sort(), Object.keys(proj.message_attributes).sort());
  for (const [key, want] of Object.entries(proj.message_attributes)) {
    assert.equal(got[key]?.DataType, want.DataType, key);
    assert.equal(got[key]?.StringValue, want.StringValue, key);
  }
});

test("sqs conformance: attempts reconciliation matches the golden", async () => {
  const url = "https://sqs.x/123/orders";
  for (const c of sqs.attempts_reconciliation.cases) {
    const base = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 });
    const body = EnvelopeCodec.encode({ ...base, attempts: c.body_attempts });

    const message: SqsMessage = {
      Body: body,
      ReceiptHandle: "rh",
      Attributes: c.approximate_receive_count === null
        ? undefined
        : { ApproximateReceiveCount: c.approximate_receive_count },
    };
    const api: SqsApi = {
      async sendMessage() { return {}; },
      async receiveMessage() { return { Messages: [message] }; },
      async deleteMessage() { return {}; },
    };

    let seen = -1;
    await new SqsConsumer(api, url, {
      "urn:babel:orders:created": (env) => { seen = env.attempts; },
    }).poll();

    assert.equal(seen, c.expected_attempts, c.name);
  }
});

test("sqs conformance: schema_version_gate decides decode vs reject before the body is decoded", async () => {
  const gate = sqs.schema_version_gate;
  assert.equal(gate.property, "bq-schema-version");
  const body = readFileSync(join(dir, gate.fixture), "utf8");
  assert.ok(gate.cases.length > 0);

  for (const c of gate.cases) {
    const label = c.absent ? "absent" : JSON.stringify(c.value);
    const message: SqsMessage = {
      Body: body,
      ReceiptHandle: "rh",
      MessageAttributes: c.absent ? undefined : { [gate.property]: { DataType: "String", StringValue: c.value } },
      Attributes: { ApproximateReceiveCount: "1" },
    };
    const deleted: string[] = [];
    const api: SqsApi = {
      async sendMessage() { return {}; },
      async receiveMessage() { return { Messages: [message] }; },
      async deleteMessage(input) { deleted.push(input.ReceiptHandle); return {}; },
    };

    let calls = 0;
    let errors = 0;
    await new SqsConsumer(
      api,
      "https://sqs.x/123/orders",
      { "urn:babel:orders:created": () => { calls += 1; } },
      { onError: () => { errors += 1; } },
    ).poll();

    if (c.expect === "decode") {
      assert.equal(calls, 1, label);
      assert.equal(deleted.length, 1, label);
    } else {
      assert.equal(calls, 0, label);
      assert.equal(errors, 1, label);
      assert.equal(deleted.length, 0, label);
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

test("sqs conformance: poison_dlq records written by the consumer have the ADR-0035 shape", async () => {
  const section = manifest.poison_dlq;
  assert.ok(section.cases.length > 0);
  const dlqUrl = `https://sqs.x/123/${section.original_queue}.dlq`;
  for (const [index, c] of section.cases.entries()) {
    const id = ConformanceResults.caseId("poison_dlq", c, index);
    const execute = async (): Promise<void> => {
      const body = c.body_base64 !== undefined ? Buffer.from(c.body_base64, "base64") : Buffer.from(c.body ?? "", "utf8");

      let record: string;
      if (c.body_base64 !== undefined) {
        // An SQS message body is always text (the service rejects bytes that are not valid UTF-8), so a body that
        // is not valid UTF-8 cannot reach this consumer. The record is built by the same core helper the consumer calls.
        const deadLetter = { reason: "poison", error: "e", exception: "BabelQueueError", failed_at: 1, original_queue: section.original_queue, attempts: 0, lang: "node" };
        record = buildPoisonRecord(body, JSON.stringify(deadLetter));
      } else {
        const sent: SendMessageInput[] = [];
        const deleted: string[] = [];
        const message: SqsMessage = { Body: c.body ?? "", ReceiptHandle: "rh", Attributes: { ApproximateReceiveCount: "1" } };
        const api: SqsApi = {
          async sendMessage(input) { sent.push(input); return {}; },
          async receiveMessage() { return { Messages: [message] }; },
          async deleteMessage(input) { deleted.push(input.ReceiptHandle); return {}; },
        };
        await new SqsConsumer(api, `https://sqs.x/123/${section.original_queue}`, {}, { deadLetterQueueUrl: dlqUrl, onError: () => undefined }).poll();
        // Never handed to a handler, never retried: written once, deleted after the write.
        assert.equal(sent.length, 1, c.name);
        assert.equal(sent[0]!.QueueUrl, dlqUrl, c.name);
        assert.equal(deleted.length, 1, c.name);
        record = sent[0]!.MessageBody;
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
    };

    if (c.body_base64 === undefined) {
      await results.run("poison_dlq", id, execute);
    } else {
      // Not a consumer result: an SQS body is always text, so a body that is not valid UTF-8 cannot reach this consumer.
      // The core builder the consumer calls is still checked (a failure fails the test), but the case is reported as skipped.
      await execute();
      results.record("poison_dlq", id, { status: "skip", reason: "SQS message bodies are text; a non-UTF-8 body cannot reach this consumer" });
    }
  }
});

// --- version_gate_sqs (ADR-0037 decision 1, broker-bindings §3.7) ------------------------------------------------

test("sqs conformance: version_gate_sqs settles on the first delivery, undecoded", async () => {
  const section = manifest.version_gate_sqs;
  assert.ok(section.cases.length > 0);
  assert.ok(section.incoming_bodies.length > 0);
  const queueUrl = "https://sqs.x/123/orders";

  for (const [index, c] of section.cases.entries()) {
    await results.run("version_gate_sqs", ConformanceResults.caseId("version_gate_sqs", c, index), async () => {
      // `dead_letter: off` means no DLQ queue URL exists in this consumer, so the no-writer outcome applies.
      const outcome = c.dead_letter === "off" ? (c.expect.outcome_no_writer ?? c.expect.outcome) : c.expect.outcome;

      for (const incoming of section.incoming_bodies) {
        const attributes: Record<string, { DataType: string; StringValue: string }> = {
          "bq-schema-version": { DataType: "String", StringValue: "2" },
        };
        if (c.bq_attempts !== undefined) attributes["bq-attempts"] = { DataType: "String", StringValue: c.bq_attempts };
        const message: SqsMessage = {
          Body: incoming.body,
          ReceiptHandle: "rh",
          MessageAttributes: attributes,
          Attributes: { ApproximateReceiveCount: "8" },
        };
        const events: string[] = [];
        const sent: SendMessageInput[] = [];
        const deleted: string[] = [];
        const visibility: unknown[] = [];
        const api: SqsApi = {
          async sendMessage(input) {
            if (c.dlq_write === "fails") throw new Error("dlq write failed");
            events.push("send");
            sent.push(input);
            return {};
          },
          async receiveMessage() { return { Messages: [message] }; },
          async deleteMessage(input) { events.push("delete"); deleted.push(input.ReceiptHandle); return {}; },
          async changeMessageVisibility(input) { visibility.push(input); return {}; },
        };

        let handled = 0;
        await new SqsConsumer(
          api,
          queueUrl,
          { "urn:babelqueue:order.created": () => { handled += 1; } },
          {
            onUnknownUrn: () => { handled += 1; },
            onError: () => undefined,
            ...(c.dead_letter === "on" ? { deadLetterQueueUrl: `${queueUrl}.dlq` } : {}),
            ...(c.on_unknown_urn === undefined ? {} : { unknownUrn: c.on_unknown_urn }),
          },
        ).poll();

        const label = `${c.name} / ${incoming.name}`;
        assert.equal(handled, 0, `${label}: never decoded, no handler and no onUnknownUrn call`);
        assert.equal(visibility.length, 0, `${label}: the SDK never releases or retries it`);
        switch (outcome) {
          case "ack": // SQS: DeleteMessage, no record
            assert.deepEqual(events, ["delete"], label);
            break;
          case "dlq_raw":
            assert.deepEqual(events, ["send", "delete"], `${label}: deleted only after the write`);
            assert.equal(sent[0]!.MessageBody, incoming.body, `${label}: the record is the incoming body, byte for byte`);
            break;
          case "unsettled": // the write failed: no DeleteMessage, visibility expiry redelivers it
            assert.deepEqual(events, [], label);
            break;
          case "native": // the SDK does not settle it: no DeleteMessage, RedrivePolicy / retention applies
            assert.deepEqual(events, [], label);
            assert.equal(deleted.length, 0, label);
            break;
          default:
            assert.fail(`${label}: unexpected outcome ${outcome}`);
        }
      }
    });
  }
});
