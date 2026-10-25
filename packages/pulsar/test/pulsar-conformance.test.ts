import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";

import { ConformanceResults } from "../../../test-support/results.js";
import {
  pulsarProperties,
  type BabelHandlers,
  type PulsarConsumerClient,
  type PulsarProducer,
  type PulsarProducerMessage,
  type PulsarReceivedMessage,
} from "../src/index.js";
import { sharedConsumer } from "./support.js";

// The vendored canonical suite (synced from conformance/). The `pulsar` block locks the §5
// property projection (bq-* string→string) + the attempts = max(body, redeliveryCount)
// reconciliation (no −1; the redelivery count is 0-based).
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(packageRoot, "test", "conformance");
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as {
  pulsar: PulsarBlock;
  pulsar_exhaustion: { cases: ExhaustionCase[] };
};
const pulsar = manifest.pulsar;
const results = new ConformanceResults(packageRoot, "pulsar", ["pulsar"]);

after(() => {
  results.write();
});

interface ExhaustionCase {
  name: string;
  kind: "failed" | "unknown_urn" | "poison";
  body_attempts?: number;
  bq_attempts?: string;
  redelivery_count: number;
  max_attempts?: number;
  dead_letter: boolean;
  on_unknown_urn?: string;
  expect: { outcome: "retry" | "dlq" | "drop"; reason?: string; dead_letter_attempts?: number; outcome_no_writer?: string };
}

interface GateCase {
  value?: string;
  absent?: boolean;
  expect: "decode" | "reject";
}

interface PulsarBlock {
  schema_version_gate: { property: string; fixture: string; cases: GateCase[] };
  property_projection: { envelope_file: string; properties: Record<string, string> };
  attempts_reconciliation: {
    cases: { name: string; body_attempts: number; redelivery_count: number; expected_attempts: number }[];
  };
}

test("pulsar conformance: property projection matches the golden", async () => {
  await results.run("pulsar", "property_projection", () => {
    const proj = pulsar.property_projection;
    const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
    if (!EnvelopeCodec.accepts(env)) {
      throw new Error("conformance fixture is not a conformant envelope");
    }
    assert.deepEqual(pulsarProperties(env), proj.properties);
  });
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
    await results.run("pulsar_attempts_reconciliation", c.name, async () => {
      const base = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 });
      const body = EnvelopeCodec.encode({ ...base, attempts: c.body_attempts });
      const message: PulsarReceivedMessage = {
        getData: () => body,
        getProperties: () => ({}),
        getRedeliveryCount: () => c.redelivery_count,
      };

      let seen = -1;
      await sharedConsumer(new OneShotConsumer(message), {
        "urn:babel:orders:created": (env) => {
          seen = env.attempts ?? -1;
        },
      }).poll();

      assert.equal(seen, c.expected_attempts, c.name);
    });
  }
});

test("pulsar conformance: schema_version_gate decides decode vs reject before the body is decoded", async () => {
  const gate = pulsar.schema_version_gate;
  assert.equal(gate.property, "bq-schema-version");
  const body = readFileSync(join(dir, gate.fixture), "utf8");
  const urn = EnvelopeCodec.urn(EnvelopeCodec.decode(body));
  assert.ok(gate.cases.length > 0);

  for (const [index, c] of gate.cases.entries()) {
    const label = c.absent ? "absent" : JSON.stringify(c.value);
    await results.run("pulsar_schema_version_gate", `${index}:${label}`, async () => {
      const properties: Record<string, string> = c.absent ? {} : { [gate.property]: c.value as string };
      const message: PulsarReceivedMessage = {
        getData: () => body,
        getProperties: () => properties,
        getRedeliveryCount: () => 0,
      };

      const client = new OneShotConsumer(message);
      let calls = 0;
      let errors = 0;
      await sharedConsumer(
        client,
        { [urn]: () => void (calls += 1) },
        { onError: () => void (errors += 1) },
      ).poll();

      if (c.expect === "decode") {
        assert.equal(calls, 1, label);
        assert.equal(client.acked.length, 1, label);
        assert.equal(client.nacked.length, 0, label);
      } else {
        assert.equal(calls, 0, label);
        assert.equal(errors, 1, label);
        assert.equal(client.acked.length, 0, label);
        assert.equal(client.nacked.length, 1, label);
      }
    });
  }
});

// --- pulsar_exhaustion (ADR-0036 decisions 1 and 2) ---------------------------------

/** A producer that records the DLQ records it is asked to send. */
class RecordingProducer implements PulsarProducer {
  sent: PulsarProducerMessage[] = [];
  getTopic(): string {
    return "persistent://public/default/orders.dlq";
  }
  async send(message: PulsarProducerMessage): Promise<unknown> {
    this.sent.push(message);
    return {};
  }
}

test("pulsar conformance: pulsar_exhaustion settle decision matches the golden", async () => {
  const cases = manifest.pulsar_exhaustion.cases;
  assert.ok(cases.length > 0);

  // Every row runs twice: retry as a negative acknowledgement (Shared) and as a re-publish through a
  // retryProducer (the Go model, bounded on Exclusive/Failover too). The result is recorded per row, passing
  // only when both modes agree.
  for (const [index, c] of cases.entries()) {
    const id = ConformanceResults.caseId("pulsar_exhaustion", c, index);

    // Node has no unknown-URN strategy option: the rows that set one belong to the Go and Python
    // runtimes and the PHP Dispatcher, and are reported as skipped, never as passed.
    if (c.on_unknown_urn !== undefined && c.on_unknown_urn !== "fail") {
      results.record("pulsar_exhaustion", id, { status: "skip", reason: `no on_unknown_urn option (${c.on_unknown_urn})` });
      continue;
    }

    await results.run("pulsar_exhaustion", id, async () => {
      for (const withRetryProducer of [false, true]) {
        const retryProducer = new RecordingProducer();
        const urn = "urn:babel:orders:created";
        const raw =
          c.kind === "poison"
            ? "not-json"
            : EnvelopeCodec.encode({ ...EnvelopeCodec.make(urn, { x: 1 }, { queue: "orders" }), attempts: c.body_attempts ?? 0 });
        const properties: Record<string, string> = c.bq_attempts === undefined ? {} : { "bq-attempts": c.bq_attempts };
        const message: PulsarReceivedMessage = {
          getData: () => raw,
          getProperties: () => properties,
          getRedeliveryCount: () => c.redelivery_count,
        };

        const client = new OneShotConsumer(message);
        const producer = new RecordingProducer();
        const handlers: BabelHandlers =
          c.kind === "failed"
            ? {
                [urn]: () => {
                  throw new Error("boom");
                },
              }
            : {};
        await sharedConsumer(client, handlers, {
          ...(c.max_attempts === undefined ? {} : { maxAttempts: c.max_attempts }),
          ...(c.dead_letter ? { deadLetterProducer: producer, queue: "orders" } : {}),
          ...(withRetryProducer ? { retryProducer } : {}),
          onError: () => undefined,
          onWarn: () => undefined,
        }).poll();

        switch (c.expect.outcome) {
          case "retry":
            if (withRetryProducer) {
              assert.equal(retryProducer.sent.length, 1, "retry = re-publish with bq-attempts + 1");
              assert.equal(client.acked.length, 1);
              assert.equal(client.nacked.length, 0);
            } else {
              assert.equal(client.nacked.length, 1, "retry = negativeAcknowledge");
              assert.equal(client.acked.length, 0);
            }
            assert.equal(producer.sent.length, 0);
            break;
          case "drop":
            assert.equal(retryProducer.sent.length, 0);
            assert.equal(client.acked.length, 1, "drop = acknowledge");
            assert.equal(client.nacked.length, 0);
            assert.equal(producer.sent.length, 0);
            break;
          case "dlq": {
            assert.equal(retryProducer.sent.length, 0);
            assert.equal(client.acked.length, 1, "dlq = acknowledge after the write");
            assert.equal(client.nacked.length, 0);
            assert.equal(producer.sent.length, 1);
            const record = JSON.parse((producer.sent[0] as PulsarProducerMessage).data.toString("utf8")) as {
              dead_letter: { reason: string; attempts: number };
            };
            assert.equal(record.dead_letter.reason, c.expect.reason);
            if (c.expect.dead_letter_attempts !== undefined) {
              assert.equal(record.dead_letter.attempts, c.expect.dead_letter_attempts);
            }
            break;
          }
        }
      }
    });
  }
});
