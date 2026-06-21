/**
 * Pure, dependency-free helpers for the out-of-band transport-header seam (ADR-0028) — the Node
 * adapter mirror of Go's `sanitizeHeaders` and PHP's `Support\Headers`. They fold an injected
 * {@link HeaderCarrier} (e.g. a W3C `traceparent`) onto a transport's metadata channel **without
 * clobbering** the contract headers it already carries.
 *
 * Headers ride beside the frozen wire envelope (GR-1), never inside it.
 */

import type { HeaderCarrier } from "@babelqueue/core";

/**
 * Copy `headers`, dropping blank keys and blank values. Returns an empty object when nothing
 * survives. A nullish input yields an empty object.
 */
export function sanitizeHeaders(headers: HeaderCarrier | null | undefined): HeaderCarrier {
  const out: HeaderCarrier = {};
  if (!headers) return out;
  for (const key of Object.keys(headers)) {
    const value = headers[key];
    if (key === "" || value == null || value === "") continue;
    out[key] = value;
  }
  return out;
}

/**
 * Merge out-of-band `extra` headers onto an already-built `base` map **without clobbering** the
 * contract headers in `base` (they win a key collision) and dropping blanks. Returns `base`,
 * mutated in place.
 */
export function mergeInto(base: { [key: string]: string }, extra: HeaderCarrier | null | undefined): { [key: string]: string } {
  for (const [key, value] of Object.entries(sanitizeHeaders(extra))) {
    if (key in base) continue; // the contract header already there wins
    base[key] = value;
  }
  return base;
}
