import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

import { BabelQueueError, EnvelopeCodec, UnknownUrnStrategy } from "@babelqueue/core";

import { ConformanceResults } from "../../../test-support/results.js";
import { envelopeViolation, resolveIdentity } from "../src/envelope-shape.js";
import {
  DEFAULT_RETRY_TIERS_MS,
  KafkaConsumer,
  KafkaPublisher,
  kafkaHeaders,
  type BabelHandler,
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
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const dir = join(packageRoot, "test", "conformance");
const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")) as {
  kafka: KafkaBlock;
  backoff: BackoffBlock;
  kafka_poison: { cases: PoisonCase[] };
};
const kafka = manifest.kafka;
const results = new ConformanceResults(packageRoot, "kafka", ["kafka"]);

after(() => {
  results.write();
});

interface GateCase {
  value?: string | null;
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

interface TierExpect {
  n?: number;
  topic?: string;
  tier_ms: number;
  body_attempts?: number;
  headers: Record<string, string>;
  key?: string;
}

interface BackoffBlock {
  cases: { name: string; work_topic: string; retry_tiers_ms: number[]; delay_ms: number; expect: TierExpect | "reject" }[];
  retry_republish: {
    cases: { name: string; work_topic: string; retry_tiers_ms: number[]; attempts: number; expect: TierExpect | "reject" }[];
  };
  topic_names: { cases: { name: string; work_topic: string; n: number; expect_topic: string }[] };
  retry_attempt_tier: {
    cases: { name: string; work_topic: string; retry_tiers_ms: number[]; attempts_before: number; key?: string; expect: TierExpect }[];
  };
  default_tiers: { cases: { name: string; constant: string; expect: number[] }[] };
}

interface PoisonCase {
  name: string;
  body: string;
  expect: "ok" | "no_identity" | "poison";
}

const URN = "urn:babel:orders:created";

test("kafka conformance: header projection matches the golden", async () => {
  await results.run("kafka", "property_projection", () => {
    const proj = kafka.property_projection;
    const env = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
    if (!EnvelopeCodec.accepts(env)) {
      throw new Error("conformance fixture is not a conformant envelope");
    }
    assert.deepEqual(kafkaHeaders(env), proj.headers);
  });
});

/** A consumer that only records the manual commits. */
class CommitCapturingConsumer implements KafkaConsumerClient {
  committed: { topic: string; partition: number; offset: string }[] = [];
  async run(): Promise<void> {}
  async commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void> {
    this.committed.push(...offsets);
  }
}

/** A producer that only records what is sent. */
class CapturingProducer implements KafkaProducerClient {
  sent: { topic: string; messages: KafkaProducerMessage[] }[] = [];
  async send(record: { topic: string; messages: KafkaProducerMessage[] }): Promise<unknown> {
    this.sent.push(record);
    return {};
  }
}

test("kafka conformance: attempts reconciliation matches the golden", async () => {
  for (const c of kafka.attempts_reconciliation.cases) {
    await results.run("kafka_attempts_reconciliation", c.name, async () => {
      const base = EnvelopeCodec.make(URN, { x: 1 });
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
      const handlers: BabelHandlers = { [URN]: (env) => void (seen = env.attempts ?? -1) };
      await new KafkaConsumer(new CommitCapturingConsumer(), handlers).handle(payload);

      assert.equal(seen, c.expected_attempts, c.name);
    });
  }
});

test("kafka conformance: schema_version_gate decides decode vs reject before the body is decoded", async () => {
  const gate = kafka.schema_version_gate;
  assert.equal(gate.property, "bq-schema-version");
  const body = Buffer.from(readFileSync(join(dir, gate.fixture), "utf8"), "utf8");
  const urn = EnvelopeCodec.urn(EnvelopeCodec.decode(body.toString("utf8")));
  assert.ok(gate.cases.length > 0);

  for (const [index, c] of gate.cases.entries()) {
    const label = c.absent ? "absent" : JSON.stringify(c.value);
    await results.run("kafka_schema_version_gate", `${index}:${label}`, async () => {
      const headers: IncomingHeaders = {};
      // JSON null = the header key is present with a null value (must be treated as absent, never "null").
      if (!c.absent) headers[gate.property] = c.value === null ? null : Buffer.from(c.value as string, "utf8");
      const payload: EachMessagePayload = { topic: "orders", partition: 0, message: { value: body, headers, offset: "0" } };

      const producer = new CapturingProducer();
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
        assert.equal(producer.sent.length, 0, label);
      } else {
        assert.equal(calls, 0, label);
        assert.equal(errors, 1, label);
        assert.equal(producer.sent.length, 1, label);
        assert.equal(producer.sent[0]!.topic, "orders.dlq", label);
      }
    });
  }
});

// --- backoff: retry-topic tiers (ADR-0034 sections 5-7; broker-bindings.md sections 6.4 / 6.5) -----------------

const text = (value: Buffer | string | undefined): string | undefined =>
  value === undefined ? undefined : typeof value === "string" ? value : value.toString("utf8");

test("kafka conformance: backoff requested-delay rule picks the smallest tier >= the delay", async () => {
  for (const c of manifest.backoff.cases) {
    await results.run("backoff", c.name, async () => {
      const topics = new RetryTopics(c.work_topic, c.retry_tiers_ms);
      const producer = new CapturingProducer();
      const publisher = KafkaPublisher.withRetryTopics(producer, topics);
      if (c.expect === "reject") {
        await assert.rejects(publisher.publish(URN, { x: 1 }, { delayMs: c.delay_ms }), BabelQueueError);
        assert.equal(producer.sent.length, 0);
        return;
      }
      const expect = c.expect;
      await publisher.publish(URN, { x: 1 }, { delayMs: c.delay_ms });
      assert.equal(producer.sent.length, 1);
      const sent = producer.sent[0]!;
      const tier = topics.tiers.find((t) => t.delayMs === expect.tier_ms);
      assert.ok(tier, "the expected tier is configured");
      assert.equal(sent.topic, tier.topic);
      const headers = sent.messages[0]!.headers!;
      for (const [name, value] of Object.entries(expect.headers)) assert.equal(headers[name], value, name);
    });
  }
});

test("kafka conformance: backoff topic_names are <work_topic>.retry.<n>", async () => {
  for (const c of manifest.backoff.topic_names.cases) {
    await results.run("backoff_topic_names", c.name, () => {
      const topics = new RetryTopics(
        c.work_topic,
        Array.from({ length: c.n }, (_, i) => (i + 1) * 1000),
      );
      assert.equal(topics.tiers[c.n - 1]!.topic, c.expect_topic);
    });
  }
});

/** Settle a failed record at `attempts` through the SDK consumer, returning the single record it republished. */
async function republishedAfterFailure(
  workTopic: string,
  tiersMs: number[],
  attempts: number,
  key: string | undefined,
  via: "handler" | "release",
): Promise<KafkaProducerMessage & { topic: string }> {
  const producer = new CapturingProducer();
  const retryTopics = new RetryTopics(workTopic, tiersMs);
  const base = EnvelopeCodec.make(URN, { x: 1 }, { queue: workTopic });
  const body = EnvelopeCodec.encode({ ...base, attempts });
  const payload: EachMessagePayload = {
    topic: workTopic,
    partition: 0,
    message: { value: Buffer.from(body, "utf8"), headers: {}, offset: "3", ...(key === undefined ? {} : { key: Buffer.from(key, "utf8") }) },
  };
  const consumer =
    via === "handler"
      ? new KafkaConsumer(
          new CommitCapturingConsumer(),
          {
            [URN]: () => {
              throw new Error("boom");
            },
          },
          { producer, retryTopics, maxTries: 1_000 },
        )
      : new KafkaConsumer(new CommitCapturingConsumer(), {}, { producer, retryTopics, unknownUrn: UnknownUrnStrategy.RELEASE });
  await consumer.handle(payload);
  assert.equal(producer.sent.length, 1);
  const sent = producer.sent[0]!;
  assert.equal(sent.messages.length, 1);
  return { topic: sent.topic, ...sent.messages[0]! };
}

function assertTierRecord(record: KafkaProducerMessage & { topic: string }, expect: TierExpect, key?: string): void {
  assert.equal(record.topic, expect.topic);
  const headers = record.headers!;
  for (const [name, value] of Object.entries(expect.headers)) assert.equal(headers[name], value, name);
  assert.equal(EnvelopeCodec.decode(String(record.value)).attempts, expect.body_attempts);
  if (key !== undefined) assert.equal(text(record.key as Buffer | string), key, "the record key is kept byte for byte");
}

test("kafka conformance: backoff retry_attempt_tier follows n = min(attempts_before, N-1) + 1", async () => {
  for (const c of manifest.backoff.retry_attempt_tier.cases) {
    await results.run("backoff_retry_attempt_tier", c.name, async () => {
      const record = await republishedAfterFailure(c.work_topic, c.retry_tiers_ms, c.attempts_before, c.key, "handler");
      assertTierRecord(record, c.expect, c.expect.key);
    });
  }
});

test("kafka conformance: backoff retry_republish through the release entry point", async () => {
  for (const c of manifest.backoff.retry_republish.cases) {
    await results.run("backoff_retry_republish", c.name, async () => {
      if (c.expect === "reject") {
        const producer = new CapturingProducer();
        const base = EnvelopeCodec.make(URN, { x: 1 }, { queue: c.work_topic });
        const payload: EachMessagePayload = {
          topic: c.work_topic,
          partition: 0,
          message: { value: Buffer.from(EnvelopeCodec.encode({ ...base, attempts: c.attempts }), "utf8"), headers: {}, offset: "3" },
        };
        await assert.rejects(
          new KafkaConsumer(
            new CommitCapturingConsumer(),
            {},
            { producer, retryTopics: new RetryTopics(c.work_topic, c.retry_tiers_ms), unknownUrn: UnknownUrnStrategy.RELEASE },
          ).handle(payload),
          BabelQueueError,
        );
        assert.equal(producer.sent.length, 0);
        return;
      }
      const record = await republishedAfterFailure(c.work_topic, c.retry_tiers_ms, c.attempts, undefined, "release");
      assertTierRecord(record, c.expect);
    });
  }
});

test("kafka conformance: backoff default_tiers exposes DEFAULT_RETRY_TIERS_MS", async () => {
  for (const c of manifest.backoff.default_tiers.cases) {
    await results.run("backoff_default_tiers", c.name, () => {
      assert.equal(c.constant, "DEFAULT_RETRY_TIERS_MS");
      assert.deepEqual([...DEFAULT_RETRY_TIERS_MS], c.expect);
    });
  }
});

// --- kafka_poison: consumer-side body classification (ADR-0034 section 10, ADR-0032 section 6) -------------------
//   ok          - decodes into the envelope and carries a valid identity
//   no_identity - decodes, but neither `job` nor `urn` is a valid identity
//   poison      - cannot be decoded into the envelope
// On Kafka `no_identity` and `poison` share one fate: `<topic>.dlq` with reason `poison`, handler never called.

type Verdict = PoisonCase["expect"];

function verdictOf(body: string): Verdict {
  if (envelopeViolation(body) !== null) return "poison";
  return resolveIdentity(body) === "" ? "no_identity" : "ok";
}

test("kafka conformance: kafka_poison is present and covers all three verdicts", () => {
  const cases = manifest.kafka_poison.cases;
  assert.ok(cases.length >= 92, `expected at least 92 cases, got ${cases.length}`);
  assert.deepEqual([...new Set(cases.map((c) => c.expect))].sort(), ["no_identity", "ok", "poison"]);
});

test("kafka conformance: kafka_poison cases classify and settle with the verdict's fate", async () => {
  for (const [index, c] of manifest.kafka_poison.cases.entries()) {
    await results.run("kafka_poison", ConformanceResults.caseId("kafka_poison", c, index), async () => {
      assert.equal(verdictOf(c.body), c.expect, "classification");

      const producer = new CapturingProducer();
      const consumer = new CommitCapturingConsumer();
      let handled = 0;
      // A handler for every identity, so only the verdict (never a missing handler) decides the fate.
      const handler: BabelHandler = () => void (handled += 1);
      const handlers = new Proxy({} as BabelHandlers, { get: (_target, name) => (typeof name === "string" ? handler : undefined) });
      const payload: EachMessagePayload = {
        topic: "orders",
        partition: 0,
        message: { value: Buffer.from(c.body, "utf8"), headers: {}, offset: "7" },
      };

      await new KafkaConsumer(consumer, handlers, { producer, retryTopics: new RetryTopics("orders", [5000]) }).handle(payload);

      assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "8" }], "offset committed once");
      if (c.expect === "ok") {
        assert.equal(handled, 1, "handler ran");
        assert.equal(producer.sent.length, 0, "nothing republished");
        return;
      }
      assert.equal(handled, 0, "handler never called");
      assert.equal(producer.sent.length, 1, "one record written");
      assert.equal(producer.sent[0]!.topic, "orders.dlq");
      const dead = JSON.parse(String(producer.sent[0]!.messages[0]!.value)) as { dead_letter?: { reason?: string } };
      assert.equal(dead.dead_letter?.reason, "poison");
    });
  }
});

test("kafka conformance: kafka_poison last duplicate top-level key wins", () => {
  assert.equal(verdictOf('{"job":5,"job":"urn:x"}'), "ok");
  assert.equal(verdictOf('{"job":"urn:x","job":5}'), "no_identity");
});
