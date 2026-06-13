import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import { amqpProperties } from "../src/index.js";

// The vendored canonical suite (synced from conformance/). The `rabbitmq` block locks the §2
// AMQP 0-9-1 property projection (type/correlation_id/message_id/app_id + native-typed x- headers).
const dir = fileURLToPath(new URL("./conformance", import.meta.url));
const rabbitmq = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")).rabbitmq as RabbitMQBlock;

interface RabbitMQBlock {
  property_projection: {
    envelope_file: string;
    properties: Record<string, string>;
    headers: Record<string, string | number>;
  };
}

test("rabbitmq conformance: property projection matches the golden", () => {
  const proj = rabbitmq.property_projection;
  const envelope = EnvelopeCodec.decode(readFileSync(join(dir, proj.envelope_file), "utf8"));
  if (!EnvelopeCodec.accepts(envelope)) {
    throw new Error("conformance fixture is not a conformant envelope");
  }
  const props = amqpProperties(envelope as Envelope);

  assert.equal(props.type, proj.properties["type"]);
  assert.equal(props.correlationId, proj.properties["correlation_id"]);
  assert.equal(props.messageId, proj.properties["message_id"]);
  assert.equal(props.appId, proj.properties["app_id"]);
  assert.equal(props.contentType, proj.properties["content_type"]);

  // Headers are native-typed — integers stay integers.
  assert.equal(props.headers!["x-schema-version"], proj.headers["x-schema-version"]);
  assert.equal(props.headers!["x-source-lang"], proj.headers["x-source-lang"]);
  assert.equal(props.headers!["x-attempts"], proj.headers["x-attempts"]);
});
