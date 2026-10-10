import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import {
  AsbConsumer,
  toServiceBusMessage,
  type AsbReceivedMessage,
  type AsbReceiver,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `asb` block locks the §4
// native projection + the attempts = max(body, deliveryCount − 1) reconciliation.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const asb = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).asb as AsbBlock;

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
