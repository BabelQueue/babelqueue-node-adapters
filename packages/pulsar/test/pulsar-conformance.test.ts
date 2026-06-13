import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import {
  PulsarConsumer,
  pulsarProperties,
  type PulsarConsumerClient,
  type PulsarReceivedMessage,
} from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `pulsar` block locks the §5
// property projection (bq-* string→string) + the attempts = max(body, redeliveryCount)
// reconciliation (no −1; the redelivery count is 0-based).
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const pulsar = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).pulsar as PulsarBlock;

interface PulsarBlock {
  property_projection: { envelope_file: string; properties: Record<string, string> };
  attempts_reconciliation: {
    cases: { name: string; body_attempts: number; redelivery_count: number; expected_attempts: number }[];
  };
}

test("pulsar conformance: property projection matches the golden", () => {
  const proj = pulsar.property_projection;
  const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(env)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }
  assert.deepEqual(pulsarProperties(env), proj.properties);
});

/** A consumer that hands back one message, then records the settle calls. */
class OneShotConsumer implements PulsarConsumerClient {
  acked: PulsarReceivedMessage[] = [];
  nacked: PulsarReceivedMessage[] = [];

  constructor(private readonly message: PulsarReceivedMessage) {}

  async receive(): Promise<PulsarReceivedMessage> {
    return this.message;
  }

  acknowledge(message: PulsarReceivedMessage): void {
    this.acked.push(message);
  }

  negativeAcknowledge(message: PulsarReceivedMessage): void {
    this.nacked.push(message);
  }
}

test("pulsar conformance: attempts reconciliation matches the golden", async () => {
  for (const c of pulsar.attempts_reconciliation.cases) {
    const base = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 });
    const body = EnvelopeCodec.encode({ ...base, attempts: c.body_attempts });
    const message: PulsarReceivedMessage = {
      getData: () => body,
      getProperties: () => ({}),
      getRedeliveryCount: () => c.redelivery_count,
    };

    let seen = -1;
    await new PulsarConsumer(new OneShotConsumer(message), {
      "urn:babel:orders:created": (env) => {
        seen = env.attempts ?? -1;
      },
    }).poll();

    assert.equal(seen, c.expected_attempts, c.name);
  }
});
