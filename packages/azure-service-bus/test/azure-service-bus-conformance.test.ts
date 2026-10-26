import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";
import { ConformanceResults } from "../../../test-support/results.js";
import {
  AsbConsumer,
  toServiceBusMessage,
  type AsbMessage,
  type AsbReceivedMessage,
  type AsbReceiver,
  type AsbSender,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `asb` block locks the §4
// native projection + the attempts = max(body, deliveryCount − 1) reconciliation.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as {
  asb: AsbBlock;
  poison_dlq: { original_queue: string; cases: PoisonDlqCase[] };
  version_gate_asb: { cases: VersionGateCase[]; incoming_bodies: { name: string; body: string }[] };
};
const asb = manifest.asb;
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const results = new ConformanceResults(packageRoot, "azure-service-bus", ["asb"]);

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
  value?: string | number | null;
  value_type?: "float";
  absent?: boolean;
  expect: "decode" | "reject";
}
interface AsbBlock {
  schema_version_gate: { property: string; fixture: string; cases: GateCase[] };
  property_projection: {
    envelope_file: string;
    message: { subject: string; correlation_id: string; message_id: string; content_type: string };
    application_properties: Record<string, number | string>;
  };
  attempts_reconciliation: {
    cases: { name: string; body_attempts: number; delivery_count: number; expected_attempts: number }[];
  };
}

test("asb conformance: native projection matches the golden", () => {
  const proj = asb.property_projection;
  const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(env)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }
  const msg = toServiceBusMessage(env);

  assert.equal(msg.subject, proj.message.subject);
  assert.equal(msg.correlationId, proj.message.correlation_id);
  assert.equal(msg.messageId, proj.message.message_id);
  assert.equal(msg.contentType, proj.message.content_type);
  assert.deepEqual(msg.applicationProperties, proj.application_properties);
});

/** A receiver that hands back one message, then nothing. */
class OneShotReceiver implements AsbReceiver {
  completed: AsbReceivedMessage[] = [];
  abandoned = 0;
  deadLettered = 0;

  constructor(private readonly message: AsbReceivedMessage) {}

  async receiveMessages(): Promise<AsbReceivedMessage[]> {
    return [this.message];
  }

  async completeMessage(message: AsbReceivedMessage): Promise<void> {
    this.completed.push(message);
  }

  async abandonMessage(): Promise<void> {
    this.abandoned += 1;
  }

  async deadLetterMessage(): Promise<void> {
    this.deadLettered += 1;
  }
}

test("asb conformance: attempts reconciliation matches the golden", async () => {
  for (const c of asb.attempts_reconciliation.cases) {
    const base = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 });
    const body = EnvelopeCodec.encode({ ...base, attempts: c.body_attempts });
    const receiver = new OneShotReceiver({ body, deliveryCount: c.delivery_count });

    let seen = -1;
    await new AsbConsumer(receiver, {
      "urn:babel:orders:created": (env) => {
        seen = env.attempts;
      },
    }).poll();

    assert.equal(seen, c.expected_attempts, c.name);
  }
});

test("asb conformance: schema_version_gate decides decode vs reject before the body is decoded", async () => {
  const gate = asb.schema_version_gate;
  assert.equal(gate.property, "bq-schema-version");
  const body = readFileSync(join(dir, gate.fixture), "utf8");
  assert.ok(gate.cases.length > 0);

  const skippedFloatValues: unknown[] = [];
  for (const c of gate.cases) {
    // value_type "float" (e.g. 1.0) is a native AMQP float. JavaScript has a single number type, so
    // a float 1.0 is the very same value as integral 1 and reaches the gate as `1` -- the case is
    // not representable here (the SDK cannot tell them apart) and is skipped, never mis-run as `1`.
    if (c.value_type === "float") {
      skippedFloatValues.push(c.value);
      continue;
    }
    assert.equal(c.value_type, undefined, `unknown value_type ${String(c.value_type)}`);
    // A JSON number stands for a native AMQP integral: exercise it at both int32 (number) and
    // int64 (bigint) width when it is an integer; a non-integral number only exists as a number.
    const variants: unknown[] =
      typeof c.value === "number" && Number.isInteger(c.value) ? [c.value, BigInt(c.value)] : [c.value];
    for (const value of variants) {
      const label = c.absent ? "absent" : `${typeof value}:${String(value)}`;
      const message: AsbReceivedMessage = {
        body,
        deliveryCount: 1,
        applicationProperties: c.absent ? undefined : { [gate.property]: value },
      };
      const receiver = new OneShotReceiver(message);
      let calls = 0;
      await new AsbConsumer(receiver, { "urn:babel:orders:created": () => { calls += 1; } }).poll();

      if (c.expect === "decode") {
        assert.equal(calls, 1, label);
        assert.equal(receiver.completed.length, 1, label);
        assert.equal(receiver.deadLettered, 0, label);
      } else {
        assert.equal(calls, 0, label);
        assert.equal(receiver.deadLettered, 1, label);
        assert.equal(receiver.abandoned, 0, label);
        assert.equal(receiver.completed.length, 0, label);
      }
    }
  }
  // Exactly the one float 1.0 case may be skipped -- a vanished or renamed marker must fail loudly.
  assert.deepEqual(skippedFloatValues, [1]);
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

/** A receiver that hands back one message and records how it was settled. */
class SettlingReceiver implements AsbReceiver {
  events: string[] = [];
  constructor(private readonly message: AsbReceivedMessage) {}
  async receiveMessages(): Promise<AsbReceivedMessage[]> {
    return [this.message];
  }
  async completeMessage(): Promise<void> {
    this.events.push("complete");
  }
  async abandonMessage(): Promise<void> {
    this.events.push("abandon");
  }
  async deadLetterMessage(): Promise<void> {
    this.events.push("deadletter");
  }
}

/** A sender on `<queue>.dlq` that records its sends in `events` (shared with the receiver) or rejects. */
function dlqSender(events: string[], sent: AsbMessage[], fails = false): AsbSender {
  return {
    entityPath: "orders.dlq",
    async sendMessages(message) {
      if (fails) throw new Error("dlq write failed");
      events.push("send");
      sent.push(message);
    },
    async scheduleMessages() {
      return [];
    },
  };
}

test("asb conformance: poison_dlq records written by the consumer have the ADR-0035 shape", async () => {
  const section = manifest.poison_dlq;
  assert.ok(section.cases.length > 0);
  for (const [index, c] of section.cases.entries()) {
    await results.run("poison_dlq", ConformanceResults.caseId("poison_dlq", c, index), async () => {
      const body = c.body_base64 !== undefined ? Buffer.from(c.body_base64, "base64") : Buffer.from(c.body ?? "", "utf8");
      const receiver = new SettlingReceiver({ body, deliveryCount: 1 });
      const sent: AsbMessage[] = [];
      await new AsbConsumer(receiver, {}, {
        deadLetterSender: dlqSender(receiver.events, sent),
        queue: section.original_queue,
        onError: () => undefined,
      }).poll();
      // Never handed to a handler: written once, completed after the write (not abandoned, not natively dead-lettered).
      assert.deepEqual(receiver.events, ["send", "complete"], c.name);
      const record = Buffer.from(sent[0]!.body as Uint8Array).toString("utf8");

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

// --- version_gate_asb (ADR-0037 decision 1, broker-bindings §4.7) -----------------------------------------------

test("asb conformance: version_gate_asb settles on the first delivery, undecoded", async () => {
  const section = manifest.version_gate_asb;
  assert.ok(section.cases.length > 0);
  assert.ok(section.incoming_bodies.length > 0);

  for (const [index, c] of section.cases.entries()) {
    const id = ConformanceResults.caseId("version_gate_asb", c, index);

    // The Node Azure Service Bus consumer has only an `onUnknownUrn` callback and no strategy option (it behaves as
    // `fail`): the rows that set another strategy belong to the Go and Python runtimes, and are skipped, never passed.
    if (c.on_unknown_urn !== undefined && c.on_unknown_urn !== "fail") {
      results.record("version_gate_asb", id, { status: "skip", reason: `ADR-0037: no on_unknown_urn option (${c.on_unknown_urn})` });
      continue;
    }

    await results.run("version_gate_asb", id, async () => {
      // `dead_letter: off` means no DLQ sender exists in this consumer, so the no-writer outcome applies.
      const outcome = c.dead_letter === "off" ? (c.expect.outcome_no_writer ?? c.expect.outcome) : c.expect.outcome;

      for (const incoming of section.incoming_bodies) {
        const message: AsbReceivedMessage = {
          body: incoming.body,
          deliveryCount: 9,
          applicationProperties: { "bq-schema-version": 2, ...(c.bq_attempts === undefined ? {} : { "bq-attempts": c.bq_attempts }) },
        };
        const receiver = new SettlingReceiver(message);
        const sent: AsbMessage[] = [];
        let handled = 0;
        await new AsbConsumer(
          receiver,
          { "urn:babelqueue:order.created": () => { handled += 1; } },
          {
            onUnknownUrn: () => { handled += 1; },
            onError: () => undefined,
            ...(c.dead_letter === "on" ? { deadLetterSender: dlqSender(receiver.events, sent, c.dlq_write === "fails") } : {}),
          },
        ).poll();

        const label = `${c.name} / ${incoming.name}`;
        assert.equal(handled, 0, `${label}: never decoded, no handler and no onUnknownUrn call`);
        switch (outcome) {
          case "ack": // Complete, no record
            assert.deepEqual(receiver.events, ["complete"], label);
            break;
          case "dlq_raw":
            assert.deepEqual(receiver.events, ["send", "complete"], `${label}: completed only after the write`);
            assert.ok(sent[0]!.body instanceof Uint8Array, `${label}: bytes, not a string the SDK would quote`);
            assert.equal(Buffer.from(sent[0]!.body as Uint8Array).toString("utf8"), incoming.body, `${label}: the record is the incoming body, byte for byte`);
            break;
          case "unsettled": // the write failed: Abandon (never Complete)
            assert.deepEqual(receiver.events, ["abandon"], label);
            break;
          case "native": // an explicit native DeadLetter(...) or Abandon until MaxDeliveryCount: both are conformant
            assert.ok(
              receiver.events.length === 1 && (receiver.events[0] === "deadletter" || receiver.events[0] === "abandon"),
              `${label}: ${receiver.events.join(",")}`,
            );
            break;
          default:
            assert.fail(`${label}: unexpected outcome ${outcome}`);
        }
      }
    });
  }
});

// --- publisher bytes vs the golden envelope (GR-5) -----------------------------------------------------------

test("asb conformance: the publisher's wire body is bytes holding the golden envelope JSON, not a quoted JSON string", async () => {
  const goldenText = readFileSync(join(dir, asb.property_projection.envelope_file), "utf8");
  const golden = EnvelopeCodec.decode(goldenText) as Envelope;
  const message = toServiceBusMessage(golden);
  // @azure/service-bus: a Buffer / Uint8Array is the data section as is; anything else is JSON.stringify'ed.
  assert.ok(message.body instanceof Uint8Array);
  const wire = Buffer.from(message.body).toString("utf8");
  // This does not prove byte equality with the other SDKs (the core codec conformance does); it proves the adapter hands
  // the codec's output to the SDK as bytes, so the data section is the document itself and not a quoted literal.
  assert.equal(wire, EnvelopeCodec.encode(golden), "the codec's output, as bytes");
  assert.equal(wire.charAt(0), "{");
  assert.deepEqual(JSON.parse(wire), JSON.parse(goldenText), "the same JSON document as the golden file");
  const parsed = JSON.parse(wire) as Envelope;
  assert.equal(parsed.job, golden.job);
  assert.equal(parsed.trace_id, golden.trace_id);
  assert.deepEqual(parsed.data, golden.data);
  assert.deepEqual(parsed.meta, golden.meta);
});
