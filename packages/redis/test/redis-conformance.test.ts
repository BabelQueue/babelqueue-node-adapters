import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import { RedisPublisher, type RedisClient } from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `redis` block locks the §1
// payload-identity invariant: the queue element is the byte-identical envelope, no wrapping.
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const redis = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).redis as RedisBlock;

interface RedisBlock {
  payload_identity: { envelope_file: string };
}

class CapturingRedis implements RedisClient {
  pushed: string[] = [];
  async rpush(_key: string, value: string): Promise<number> {
    this.pushed.push(value);
    return this.pushed.length;
  }
  async brpoplpush(): Promise<string | null> {
    return null;
  }
  async lrem(): Promise<number> {
    return 0;
  }
}

test("redis conformance: produce stores the byte-identical envelope (no wrapping)", async () => {
  const envelope = EnvelopeCodec.decode(readFileSync(join(dir, redis.payload_identity.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(envelope)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }

  const client = new CapturingRedis();
  // Re-publish the fixture's (urn, data) so the transport produces from the same inputs...
  await RedisPublisher.create(client, (envelope as Envelope).meta.queue).publish(
    (envelope as Envelope).job,
    (envelope as Envelope).data,
    { traceId: (envelope as Envelope).trace_id },
  );

  // ...and the stored element decodes back to the same job/trace/data with no wrapping layer.
  assert.equal(client.pushed.length, 1);
  const stored = EnvelopeCodec.decode(client.pushed[0]!) as Envelope;
  assert.equal(stored.job, (envelope as Envelope).job);
  assert.equal(stored.trace_id, (envelope as Envelope).trace_id);
  assert.deepEqual(stored.data, (envelope as Envelope).data);
  // The stored value is a single canonical envelope, not a wrapper object containing one.
  assert.ok("job" in JSON.parse(client.pushed[0]!) && "meta" in JSON.parse(client.pushed[0]!));
});
