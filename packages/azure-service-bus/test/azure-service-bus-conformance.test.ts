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

interface AsbBlock {
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

  constructor(private readonly message: AsbReceivedMessage) {}

  async receiveMessages(): Promise<AsbReceivedMessage[]> {
    return [this.message];
  }

  async completeMessage(message: AsbReceivedMessage): Promise<void> {
    this.completed.push(message);
  }

  async abandonMessage(): Promise<void> {}
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
