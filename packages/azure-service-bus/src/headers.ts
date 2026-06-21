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
