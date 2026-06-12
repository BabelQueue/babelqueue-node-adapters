import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import { SqsConsumer, toMessageAttributes, type SqsApi, type SqsMessage } from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `sqs` block locks the
// §3 attribute projection + the attempts = ApproximateReceiveCount − 1 reconciliation.
// Resolve via import.meta.url (not import.meta.dirname, which is Node 20.11+) so the
// suite stays on the supported Node 18+ floor.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const sqs = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).sqs as SqsBlock;

interface GoldenAttr {
  DataType: string;
  StringValue: string;
}
interface SqsBlock {
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
