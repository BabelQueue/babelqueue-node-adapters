/**
 * Pure, dependency-free helpers for the out-of-band transport-header seam (ADR-0028) — the Node
 * adapter mirror of Go's `sanitizeHeaders` and PHP's `Support\Headers`. They carry an injected
 * {@link HeaderCarrier} (e.g. a W3C `traceparent`) beside the frozen envelope (GR-1), never inside it.
 *
 * BullMQ has no AMQP-style header table; its native context-propagation slot is
 * `JobsOptions.telemetry.metadata` (a string, stored compactly as `tm` in Redis), so the carrier is
 * serialized to JSON there — leaving `job.data` (the canonical envelope) byte-identical.
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
 * Serialize a header carrier for BullMQ's `telemetry.metadata` slot, or `undefined` when there is
 * nothing to carry (so a header-less publish leaves `telemetry` unset and the job byte-identical).
 */
export function encodeMetadata(headers: HeaderCarrier | null | undefined): string | undefined {
  const clean = sanitizeHeaders(headers);
  return Object.keys(clean).length === 0 ? undefined : JSON.stringify(clean);
}

/**
 * Read a header carrier back from a BullMQ `telemetry.metadata` string. Returns an empty object for
 * a missing or unparseable value, so a job produced without headers (or by a non-babelqueue
 * producer) consumes with no headers and no error.
 */
export function decodeMetadata(metadata: string | undefined | null): HeaderCarrier {
  if (!metadata) return {};
  try {
    const parsed = JSON.parse(metadata) as unknown;
    if (parsed == null || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return sanitizeHeaders(parsed as HeaderCarrier);
  } catch {
    return {};
  }
}
