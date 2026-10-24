import assert from "node:assert/strict";

/** True when `body` is a strict JSON object without a top-level "dead_letter" member (ADR-0035 section 1). */
function isSpliceable(body: string): boolean {
  try {
    const parsed: unknown = JSON.parse(body);
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed) && !("dead_letter" in parsed);
  } catch {
    return false;
  }
}

/**
 * Asserts that `record` is the ADR-0035 poison DLQ record of `body`: a `dead_letter` block of reason `poison`
 * naming `queue`, and either the original bytes plus that member (splice) or the `{"raw", "dead_letter"}` wrapper.
 */
export function assertPoisonRecord(record: string, body: string, queue: string): void {
  const parsed = JSON.parse(record) as Record<string, unknown>;
  const block = parsed.dead_letter as Record<string, unknown>;
  assert.equal(block.reason, "poison");
  assert.equal(block.original_queue, queue);
  assert.equal(typeof block.error, "string");
  if (isSpliceable(body)) {
    assert.equal("raw" in parsed, false, "a strict object is spliced, not wrapped");
    assert.ok(record.startsWith(body.trim().slice(0, -1).trimEnd()), "the original bytes are kept verbatim");
    assert.ok(record.includes(',"dead_letter":') || record.startsWith('{"dead_letter":'));
  } else {
    assert.equal(parsed.raw, body);
    assert.equal("raw_encoding" in parsed, false);
    assert.deepEqual(Object.keys(parsed), ["raw", "dead_letter"]);
  }
}
