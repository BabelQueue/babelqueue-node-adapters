/**
 * The poison trigger set: a body is undecodable when a strict, typed decode into `Envelope` / `Meta` /
 * `DeadLetter` (the Go reference's `babelqueue.Decode`, except that member names are case-sensitive here) rejects it.
 *
 *  - the body must be exactly one JSON value (no BOM, trailing tokens or malformed numbers); a bare `null`
 *    decodes to an empty envelope, any other non-object is rejected;
 *  - `trace_id`, `meta.id|queue|lang` and `dead_letter.reason|error|exception|original_queue|lang` are
 *    string-or-null;
 *  - `meta.schema_version|created_at`, `attempts` and `dead_letter.failed_at|attempts` are integer-or-null: an
 *    integer literal (no fraction or exponent) that fits int64;
 *  - `data`, `meta` and `dead_letter` are object-or-null; numbers inside `data` must be finite doubles;
 *  - member names are matched exactly as spelled (case-sensitive, as the contract schema and the core decoder read
 *    them) and every occurrence of a duplicated key is checked (`JSON.parse` would keep only the last, so this is a
 *    dedicated scanner); any other member, including a differently-cased name, a `job` or `urn` of any JSON type, is
 *    only checked for syntax. `job` and `urn` are identity, not typed fields: a non-string, blank or missing one
 *    is resolved by {@link resolveIdentity} (ADR-0032 section 6), and a body with no identity is dead-lettered as
 *    poison by the consumer instead of being rejected here.
 *
 * A well-typed but non-conformant envelope is not poison (Go's consume path never calls `Accepts`).
 */

type Kind = "string" | "integer" | "object" | "data";

const MAX_DEPTH = 10_000;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

const META: Record<string, Kind> = {
  id: "string",
  queue: "string",
  lang: "string",
  schema_version: "integer",
  created_at: "integer",
};
const DEAD_LETTER: Record<string, Kind> = {
  reason: "string",
  error: "string",
  exception: "string",
  original_queue: "string",
  lang: "string",
  failed_at: "integer",
  attempts: "integer",
};
const TOP: Record<string, Kind> = {
  trace_id: "string",
  attempts: "integer",
  data: "data",
  meta: "object",
  dead_letter: "object",
};

class Reject extends Error {}

function reject(message: string): never {
  throw new Reject(message);
}

const isWs = (c: string | undefined): boolean => c === " " || c === "\t" || c === "\n" || c === "\r";
const isDigit = (c: string | undefined): boolean => c !== undefined && c >= "0" && c <= "9";

class Scanner {
  private i = 0;
  /** Containers currently open around the cursor (Go counts nesting over the whole document, not per subtree). */
  private depth = 0;
  constructor(private readonly s: string) {}

  /** Why the body is undecodable, or `null` when Go's typed decode accepts it. */
  check(): string | null {
    try {
      this.ws();
      if (this.s.startsWith("null", this.i)) {
        this.i += 4;
      } else if (this.s[this.i] === "{") {
        this.members(TOP, "");
      } else {
        reject("expected a JSON object.");
      }
      this.ws();
      if (this.i < this.s.length) reject("unexpected trailing data after the JSON value.");
      return null;
    } catch (error) {
      if (error instanceof Reject) return error.message;
      throw error;
    }
  }

  private ws(): void {
    while (isWs(this.s[this.i])) this.i++;
  }

  /** Reads an object whose members are checked against `kinds`; the cursor sits on its `{`. */
  private members(kinds: Record<string, Kind>, path: string): void {
    this.i++; // {
    this.depth++;
    this.ws();
    if (this.s[this.i] === "}") {
      this.i++;
      this.depth--;
      return;
    }
    for (;;) {
      this.ws();
      if (this.s[this.i] !== '"') reject("expected an object key.");
      const key = this.string();
      this.ws();
      if (this.s[this.i] !== ":") reject("expected ':' after an object key.");
      this.i++;
      this.ws();
      const kind = Object.prototype.hasOwnProperty.call(kinds, key) ? kinds[key] : undefined;
      if (kind === undefined) {
        this.skip(false);
      } else {
        this.typed(kind, path + key, key);
      }
      this.ws();
      const c = this.s[this.i++];
      if (c === "}") {
        this.depth--;
        return;
      }
      if (c !== ",") reject("expected ',' or '}' in an object.");
    }
  }

  private typed(kind: Kind, name: string, lower: string): void {
    const c = this.s[this.i];
    if (this.s.startsWith("null", this.i)) {
      this.i += 4;
      return;
    }
    switch (kind) {
      case "string":
        if (c !== '"') reject(`'${name}' must be a string.`);
        this.string();
        return;
      case "integer": {
        if (c !== "-" && !isDigit(c)) reject(`'${name}' must be an integer.`);
        const token = this.number();
        if (!/^-?[0-9]+$/.test(token)) reject(`'${name}' must be an integer.`);
        const n = BigInt(token);
        if (n < INT64_MIN || n > INT64_MAX) reject(`'${name}' does not fit an int64.`);
        return;
      }
      case "data":
        if (c !== "{") reject(`'${name}' must be a JSON object.`);
        this.skip(true);
        return;
      case "object":
        if (c !== "{") reject(`'${name}' must be a JSON object.`);
        this.members(lower === "meta" ? META : DEAD_LETTER, name + ".");
        return;
    }
  }

  /** Reads a string literal and returns its decoded value; the cursor sits on the opening quote. */
  private string(): string {
    const start = this.i++;
    for (;;) {
      const c = this.s[this.i];
      if (c === undefined) reject("unterminated string.");
      if (c === '"') break;
      if (c < " ") reject("control character in a string.");
      if (c === "\\") {
        const e = this.s[this.i + 1];
        if (e === "u") {
          if (!/^[0-9a-fA-F]{4}$/.test(this.s.slice(this.i + 2, this.i + 6))) reject("invalid \\u escape.");
          this.i += 6;
          continue;
        }
        if (e === undefined || !'"\\/bfnrt'.includes(e)) reject("invalid escape in a string.");
        this.i += 2;
        continue;
      }
      this.i++;
    }
    this.i++;
    return JSON.parse(this.s.slice(start, this.i)) as string;
  }

  /** Reads a number literal per the JSON grammar and returns its text. */
  private number(): string {
    const start = this.i;
    if (this.s[this.i] === "-") this.i++;
    if (this.s[this.i] === "0") {
      this.i++;
    } else if (isDigit(this.s[this.i])) {
      while (isDigit(this.s[this.i])) this.i++;
    } else {
      reject("invalid number.");
    }
    if (this.s[this.i] === ".") {
      this.i++;
      if (!isDigit(this.s[this.i])) reject("invalid number.");
      while (isDigit(this.s[this.i])) this.i++;
    }
    if (this.s[this.i] === "e" || this.s[this.i] === "E") {
      this.i++;
      if (this.s[this.i] === "+" || this.s[this.i] === "-") this.i++;
      if (!isDigit(this.s[this.i])) reject("invalid number.");
      while (isDigit(this.s[this.i])) this.i++;
    }
    return this.s.slice(start, this.i);
  }

  /** Skips one value iteratively (no recursion, so depth cannot overflow the stack); optionally requires finite numbers. */
  private skip(finite: boolean): void {
    const stack: string[] = [];
    let expectValue = true;
    for (;;) {
      this.ws();
      const c = this.s[this.i];
      if (expectValue) {
        if (c === "{" || c === "[") {
          if (this.depth + stack.length >= MAX_DEPTH) reject("exceeded the maximum nesting depth.");
          stack.push(c);
          this.i++;
          this.ws();
          if (this.s[this.i] === (c === "{" ? "}" : "]")) {
            this.i++;
            stack.pop();
            expectValue = false;
          } else if (c === "{") {
            this.key();
          }
          continue;
        }
        if (c === '"') {
          this.string();
        } else if (c === "-" || isDigit(c)) {
          const token = this.number();
          if (finite && !Number.isFinite(Number(token))) reject("a number in 'data' is not a finite double.");
        } else if (this.s.startsWith("true", this.i)) {
          this.i += 4;
        } else if (this.s.startsWith("false", this.i)) {
          this.i += 5;
        } else if (this.s.startsWith("null", this.i)) {
          this.i += 4;
        } else {
          reject("invalid JSON value.");
        }
        expectValue = false;
      }
      if (stack.length === 0) return;
      const top = stack[stack.length - 1]!;
      this.ws();
      const d = this.s[this.i++];
      if (d === ",") {
        if (top === "{") this.key();
        expectValue = true;
      } else if (d === (top === "{" ? "}" : "]")) {
        stack.pop();
        if (stack.length === 0) return;
      } else {
        reject("expected ',' or the end of a container.");
      }
    }
  }

  /** Reads `"key" :` inside a skipped object. */
  private key(): void {
    this.ws();
    if (this.s[this.i] !== '"') reject("expected an object key.");
    this.string();
    this.ws();
    if (this.s[this.i] !== ":") reject("expected ':' after an object key.");
    this.i++;
  }
}

/** Why `raw` is not a decodable BabelQueue body, or `null` when Go's typed decode accepts it. */
export function envelopeViolation(raw: string): string | null {
  return new Scanner(raw).check();
}

const ASCII_BLANK = /^[ \t\n\v\f\r]*$/;
const ASCII_EDGE = /^[ \t\n\v\f\r]+|[ \t\n\v\f\r]+$/g;

const validIdentity = (value: unknown): value is string => typeof value === "string" && !ASCII_BLANK.test(value);

/**
 * The routing identity of a (syntactically valid) body, per ADR-0032 section 6, or `""` when it has none: a
 * non-string `job` is ignored; a blank (empty or ASCII-whitespace-only) `job` falls back to `urn`; a `urn` that is
 * not a non-blank string is ignored. Surrounding ASCII whitespace (space, tab, LF, VT, FF, CR - never Unicode
 * spaces) is trimmed. When a member is duplicated the last occurrence wins, as `JSON.parse` does.
 */
export function resolveIdentity(raw: string): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return "";
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) return "";
  const record = parsed as Record<string, unknown>;
  const identity = validIdentity(record["job"]) ? record["job"] : validIdentity(record["urn"]) ? record["urn"] : "";
  return identity.replace(ASCII_EDGE, "");
}
