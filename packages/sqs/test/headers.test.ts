import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import {
  SqsConsumer,
  SqsPublisher,
  headersOf,
  mergeAttributes,
  toMessageAttributes,
  type BabelHandlers,
  type SendMessageInput,
  type SqsApi,
  type SqsMessage,
} from "../src/index.js";

const URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";
const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(): Envelope {
  return EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" });
}

// A duck-typed SQS client that round-trips MessageAttributes from send to receive.
function fakeSqs() {
  const queue: SqsMessage[] = [];
  const sent: SendMessageInput[] = [];
  let n = 0;
  const api: SqsApi & { sent: SendMessageInput[] } = {
    sent,
    async sendMessage(input) {
      sent.push(input);
      n += 1;
      queue.push({
        Body: input.MessageBody,
        MessageAttributes: input.MessageAttributes,
        ReceiptHandle: `rh-${n}`,
        Attributes: { ApproximateReceiveCount: "1" },
      });
      return { MessageId: `rh-${n}` };
    },
    async receiveMessage(input) {
      const taken = queue.splice(0, input.MaxNumberOfMessages ?? 10);
      return { Messages: taken };
    },
    async deleteMessage() {
      return {};
    },
  };
  return api;
}

// --- mergeAttributes: contract wins + 10-attr cap (no broker) -----------------

test("mergeAttributes folds a traceparent beside the contract bq-* attributes", () => {
  const merged = mergeAttributes(toMessageAttributes(envelope()), { traceparent: TRACEPARENT, tracestate: "v=1" });
  assert.equal(merged["traceparent"].StringValue, TRACEPARENT);
  assert.equal(merged["traceparent"].DataType, "String");
  assert.equal(merged["tracestate"].StringValue, "v=1");
  assert.equal(merged["bq-job"].StringValue, URN); // contract attribute still present
});

test("a contract bq-* attribute always wins a key collision", () => {
  const merged = mergeAttributes(toMessageAttributes(envelope()), { "bq-trace-id": "HIJACK" } as HeaderCarrier);
  assert.equal(merged["bq-trace-id"].StringValue, "trace-1"); // not "HIJACK"
});

test("mergeAttributes never exceeds the 10-attribute SQS cap (contract attrs preserved)", () => {
  const base = toMessageAttributes(envelope()); // ~5 contract attrs
  const riders: HeaderCarrier = {};
  for (let i = 0; i < 20; i++) riders[`r-${i.toString().padStart(2, "0")}`] = `v${i}`;
  const merged = mergeAttributes(base, riders);
  assert.ok(Object.keys(merged).length <= 10);
  assert.equal(merged["bq-job"].StringValue, URN); // contract attrs were seeded first, so kept
});

test("a header-less publish carries only the contract attributes", () => {
  const merged = mergeAttributes(toMessageAttributes(envelope()), undefined);
  assert.ok(!("traceparent" in merged));
});

// --- headersOf: extract round-trip --------------------------------------------

test("headersOf reads MessageAttributes back into a Record<string,string>", () => {
  const message: SqsMessage = { MessageAttributes: mergeAttributes(toMessageAttributes(envelope()), { traceparent: TRACEPARENT }) };
  const headers = headersOf(message);
  assert.equal(headers["traceparent"], TRACEPARENT);
  assert.equal(headers["bq-job"], URN);
  assert.deepEqual(headersOf({}), {});
});

// --- end-to-end through the fake (send → receive) -----------------------------

test("a published traceparent arrives on the consumed message's headers", async () => {
  const api = fakeSqs();
  await new SqsPublisher(api, URL).publish(URN, { order_id: 7 }, { headers: { traceparent: TRACEPARENT } });
  assert.equal(api.sent[0]!.MessageAttributes!["traceparent"].StringValue, TRACEPARENT);

  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _m, headers) => { seen = headers; } };
  await new SqsConsumer(api, URL, handlers, { waitTimeSeconds: 0 }).poll();

  assert.equal(seen!["traceparent"], TRACEPARENT);
});

// --- broker-gated integration (skips cleanly without SQS/LocalStack) ----------

const SQS_ENDPOINT = process.env.BABELQUEUE_SQS_ENDPOINT;
const SQS_QUEUE_URL = process.env.BABELQUEUE_SQS_QUEUE_URL;

test("integration: a published traceparent arrives on consume", { skip: !(SQS_ENDPOINT && SQS_QUEUE_URL) }, async () => {
  // Wired only against a real SQS / LocalStack. @aws-sdk/client-sqs is imported lazily via a
  // non-literal specifier so the optional peer is needed neither to typecheck nor to run when absent.
  const specifier = "@aws-sdk/client-sqs";
  const mod = (await import(specifier)) as unknown as { SQS: new (cfg: unknown) => SqsApi };
  const api = new mod.SQS({ endpoint: SQS_ENDPOINT, region: "us-east-1" });
  await new SqsPublisher(api, SQS_QUEUE_URL!).publish(URN, { order_id: 7 }, { headers: { traceparent: TRACEPARENT } });
  let received: HeaderCarrier | null = null;
  await new SqsConsumer(api, SQS_QUEUE_URL!, { [URN]: (_e, _m, headers) => { received = headers; } }, {
    waitTimeSeconds: 2,
  }).poll();
  assert.equal(received!["traceparent"], TRACEPARENT);
});
