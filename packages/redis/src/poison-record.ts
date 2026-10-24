// KEEP IN SYNC with babelqueue-node/src/poisonRecord.ts (@babelqueue/core, buildPoisonRecord): this is a verbatim
// copy, because the installed core dependency of this package predates the export. Delete this file and import
// buildPoisonRecord from @babelqueue/core once the dependency range is bumped to the release that ships it.
// The DLQ record of a poison body (ADR-0035, error-handling.md section 6).
//
// A body that is a strict RFC 8259 JSON object, valid UTF-8 and without a top-level "dead_letter" member is
// spliced at byte level: every original byte is kept and only the dead_letter member is inserted, so no value is
// ever re-encoded (a 1e400 or a 64-bit integer stays as written, a repeated member name is not collapsed).
// Anything else is wrapped as {"raw": <string>, "raw_encoding"?: "base64", "dead_letter": ...}: "raw" is the body
// as plain text when it is valid UTF-8, otherwise the standard base64 of the exact bytes with
// "raw_encoding": "base64". U+FFFD substitution never happens.

const JSON_SPACE = /^[ \t\r\n]+|[ \t\r\n]+$/g;

/** Decodes `bytes` as strict UTF-8 (no replacement, BOM kept), or returns `null` when they are not valid UTF-8. */
function strictUtf8(bytes: Uint8Array): string | null {
  try {
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
  } catch {
    return null;
  }
}

/**
 * Rejects a `deadLetter` argument that is not the serialized form of exactly one JSON object: it is spliced into
 * the record verbatim, so anything else (a fragment, an array, trailing members, a bare value) would produce a
 * record that is not valid JSON or carries a second top-level member.
 */
function assertJsonObject(deadLetter: string): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(deadLetter);
  } catch {
    throw new TypeError("deadLetter must be the JSON text of a single object");
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TypeError("deadLetter must be the JSON text of a single object");
  }
}

/**
 * Builds the DLQ record for `body` (the exact record value bytes), with `deadLetter` (the JSON of the
 * dead_letter block, which must be a single JSON object, else a `TypeError` is thrown) as a top-level member.
 */
export function buildPoisonRecord(body: Uint8Array, deadLetter: string): string {
  assertJsonObject(deadLetter);
  const text = strictUtf8(body);
  if (text !== null && isSpliceableObject(text)) {
    const trimmed = text.replace(JSON_SPACE, "");
    const inner = trimmed.slice(1, -1).replace(JSON_SPACE, "");
    return inner === ""
      ? `{"dead_letter":${deadLetter}}`
      : `${trimmed.slice(0, -1)},"dead_letter":${deadLetter}}`;
  }
  const raw =
    text !== null
      ? `"raw":${JSON.stringify(text)}`
      : `"raw":${JSON.stringify(Buffer.from(body).toString("base64"))},"raw_encoding":"base64"`;
  return `{${raw},"dead_letter":${deadLetter}}`;
}

/**
 * True when `text` is a single strict RFC 8259 JSON object that carries no unpaired surrogate escape and no
 * top-level "dead_letter" member (ADR-0035 section 1).
 */
function isSpliceableObject(text: string): boolean {
  try {
    JSON.parse(text); // rejects NaN / Infinity / a BOM / trailing data / control characters / an empty body
  } catch {
    return false;
  }
  let i = 0;
  while (i < text.length && isJsonSpace(text.charCodeAt(i))) i++;
  if (text[i] !== "{") return false;

  let depth = 0;
  let expectKey = false;
  while (i < text.length) {
    const c = text[i]!;
    if (c === "{" || c === "[") {
      depth++;
      expectKey = c === "{" && depth === 1;
      i++;
    } else if (c === "}" || c === "]") {
      depth--;
      i++;
    } else if (c === ",") {
      expectKey = depth === 1;
      i++;
    } else if (c === '"') {
      const end = scanJsonString(text, i);
      if (end < 0) return false;
      if (depth === 1 && expectKey) {
        if ((JSON.parse(text.slice(i, end)) as string) === "dead_letter") return false;
        expectKey = false;
      }
      i = end;
    } else {
      i++;
    }
  }
  return true;
}

function isJsonSpace(c: number): boolean {
  return c === 0x20 || c === 0x09 || c === 0x0d || c === 0x0a;
}

/**
 * Scans the (already syntax-validated) string literal starting at `text[start]` and returns the index just past
 * its closing quote, or -1 when it holds an unpaired surrogate escape.
 */
function scanJsonString(text: string, start: number): number {
  let i = start + 1;
  while (i < text.length) {
    const c = text[i]!;
    if (c === '"') return i + 1;
    if (c !== "\\") {
      i++;
      continue;
    }
    if (text[i + 1] !== "u") {
      i += 2;
      continue;
    }
    const cp = hex4(text, i + 2);
    if (cp < 0) return -1;
    i += 6;
    if (cp >= 0xd800 && cp <= 0xdbff) {
      if (text[i] !== "\\" || text[i + 1] !== "u") return -1;
      const lo = hex4(text, i + 2);
      if (lo < 0xdc00 || lo > 0xdfff) return -1;
      i += 6;
    } else if (cp >= 0xdc00 && cp <= 0xdfff) {
      return -1;
    }
  }
  return -1;
}

function hex4(text: string, at: number): number {
  const digits = text.slice(at, at + 4);
  return /^[0-9a-fA-F]{4}$/.test(digits) ? parseInt(digits, 16) : -1;
}
