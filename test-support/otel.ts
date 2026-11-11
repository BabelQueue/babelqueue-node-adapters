import type { Tracer } from "@opentelemetry/api";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

/** A tracer that records finished spans in memory, for asserting on `@babelqueue/core/otel` spans. */
export function recordingTracer(): { tracer: Tracer; exporter: InMemorySpanExporter } {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { tracer: provider.getTracer("adapter-test"), exporter };
}

/** The `messaging.message.id` of the single recorded producer span. */
export function producerMessageId(exporter: InMemorySpanExporter): unknown {
  const spans = exporter.getFinishedSpans();
  if (spans.length !== 1) throw new Error(`expected exactly one span, got ${spans.length}`);
  return spans[0]!.attributes["messaging.message.id"];
}
