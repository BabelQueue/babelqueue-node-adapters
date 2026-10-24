import assert from "node:assert/strict";
import { test } from "node:test";

import { buildPoisonRecord } from "../src/poison-record.js";

const body = Buffer.from('{"job":"urn:a","n":1}', "utf8");

test("buildPoisonRecord splices a single-object dead_letter block", () => {
  const record = buildPoisonRecord(body, '{"reason":"poison"}');
  assert.equal(record, '{"job":"urn:a","n":1,"dead_letter":{"reason":"poison"}}');
});

test("buildPoisonRecord rejects a dead_letter argument that is not exactly one JSON object", () => {
  for (const bad of [
    "",
    "not-json",
    "[]",
    "null",
    "1",
    '"x"',
    "true",
    '{"reason":"poison"',
    '{"reason":"poison"},"extra":1',
    '{"reason":"poison"}}',
    '{"a":1} {"b":2}',
  ]) {
    assert.throws(() => buildPoisonRecord(body, bad), TypeError, `should reject ${JSON.stringify(bad)}`);
  }
});

test("buildPoisonRecord wraps a non-object body only after validating the block", () => {
  assert.throws(() => buildPoisonRecord(Buffer.from("not-json"), "[]"), TypeError);
  const record = buildPoisonRecord(Buffer.from([0xff, 0xfe]), '{"reason":"poison"}');
  assert.deepEqual(JSON.parse(record), { raw: "//4=", raw_encoding: "base64", dead_letter: { reason: "poison" } });
});
