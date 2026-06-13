import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  ArtemisConsumer,
  artemisMessage,
  JMS_TYPE_KEY,
  type AmqpMessage,
  type BabelHandlers,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `artemis` block locks the §7
// AMQP projection (the x-opt-jms-type annotation / correlation-id + the bq- application
// properties) + the attempts = max(body, delivery-count) reconciliation (0-based, no −1).
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const artemis = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).artemis as ArtemisBlock;

interface ArtemisBlock {
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
