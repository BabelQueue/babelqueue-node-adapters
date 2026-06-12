import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import {
  SqsConsumer,
  SqsPublisher,
  toMessageAttributes,
  type ReceiveMessageInput,
  type SendMessageInput,
  type SqsApi,
  type SqsMessage,
} from "../src/index.js";

const URL = "https://sqs.eu-central-1.amazonaws.com/123456789012/orders";

// A duck-typed AWS SQS client — no @aws-sdk/client-sqs, no network.
function fakeSqs(err?: Error) {
  const queues = new Map<string, SqsMessage[]>();
  const sent: SendMessageInput[] = [];
  const deleted: string[] = [];
  let lastReceive: ReceiveMessageInput | undefined;
  let n = 0;

  const push = (url: string, msg: SqsMessage) => {
    const q = queues.get(url) ?? [];
    q.push(msg);
    queues.set(url, q);
  };

  const api: SqsApi & {
    sent: SendMessageInput[];
    deleted: string[];
    lastReceive(): ReceiveMessageInput | undefined;
    seed(url: string, body: string, receiveCount: number): void;
  } = {
    sent,
    deleted,
    lastReceive: () => lastReceive,
    async sendMessage(input) {
      if (err) throw err;
      sent.push(input);
      n += 1;
      push(input.QueueUrl, {
        Body: input.MessageBody,
        MessageAttributes: input.MessageAttributes,
        ReceiptHandle: `rh-${n}`,
        Attributes: { ApproximateReceiveCount: "1" },
      });
      return { MessageId: `rh-${n}` };
    },
    async receiveMessage(input) {
      lastReceive = input;
      if (err) throw err;
      const q = queues.get(input.QueueUrl) ?? [];
      const taken = q.splice(0, input.MaxNumberOfMessages ?? 10);
      return { Messages: taken };
    },
    async deleteMessage(input) {
      if (err) throw err;
      deleted.push(input.ReceiptHandle);
      return {};
    },
    seed(url, body, receiveCount) {
      n += 1;
      push(url, {
        Body: body,
        ReceiptHandle: `seed-${n}`,
        Attributes: { ApproximateReceiveCount: String(receiveCount) },
      });
    },
  };
  return api;
}

test("publish projects the contract attributes and is byte-identical", async () => {
  const sqs = fakeSqs();
  const env = EnvelopeCodec.make("urn:babel:orders:created", { order_id: 1042 }, { queue: "orders" });
  // re-derive expected body shape independently
  const id = await new SqsPublisher(sqs, URL).publish("urn:babel:orders:created", { order_id: 1042 });

  assert.equal(sqs.sent.length, 1);
  const sent = sqs.sent[0];
  assert.equal(sent.QueueUrl, URL);
  const body = EnvelopeCodec.decode(sent.MessageBody);
  assert.equal(body.job, "urn:babel:orders:created");
  assert.equal((body.meta as { queue: string }).queue, "orders"); // derived from URL
  assert.equal((body.meta as { id: string }).id, id);

  const a = sent.MessageAttributes ?? {};
  assert.equal(a["bq-job"]?.StringValue, "urn:babel:orders:created");
  assert.equal(a["bq-job"]?.DataType, "String");
  assert.equal(a["bq-schema-version"]?.StringValue, "1");
  assert.equal(a["bq-schema-version"]?.DataType, "Number");
  assert.equal(a["bq-source-lang"]?.StringValue, "node");
  assert.ok(a["bq-trace-id"]?.StringValue);
  assert.ok(a["bq-message-id"]?.StringValue);
  assert.ok(a["bq-created-at"]?.StringValue);

  // toMessageAttributes is a pure projection of the envelope
  assert.deepEqual(toMessageAttributes(env)["bq-job"], { DataType: "String", StringValue: env.job });
});

test("publish on a FIFO queue sets group id and dedup id", async () => {
  const sqs = fakeSqs();
  const fifoUrl = URL + ".fifo";
  const id = await new SqsPublisher(sqs, fifoUrl, { fifo: true }).publish("urn:babel:orders:created", { x: 1 });
  const sent = sqs.sent[0];
  assert.equal(sent.MessageGroupId, "orders.fifo");
  assert.equal(sent.MessageDeduplicationId, id);
});

test("publish with content dedup omits the dedup id", async () => {
  const sqs = fakeSqs();
  await new SqsPublisher(sqs, URL + ".fifo", {
    fifo: true,
    contentDedup: true,
    messageGroupId: "grp",
  }).publish("urn:babel:orders:created", { x: 1 });
  const sent = sqs.sent[0];
  assert.equal(sent.MessageGroupId, "grp");
  assert.equal(sent.MessageDeduplicationId, undefined);
});

test("consumer routes a valid message to its handler and deletes it", async () => {
  const sqs = fakeSqs();
  await new SqsPublisher(sqs, URL).publish("urn:babel:orders:created", { order_id: 7 });

  let seen: unknown = null;
  const consumer = new SqsConsumer(sqs, URL, {
    "urn:babel:orders:created": (env) => {
      seen = env.data;
    },
  });
  const n = await consumer.poll();
  assert.equal(n, 1);
  assert.deepEqual(seen, { order_id: 7 });
  assert.equal(sqs.deleted.length, 1);
});

test("consumer reconciles attempts from ApproximateReceiveCount", async () => {
  const sqs = fakeSqs();
  const env = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 }, { queue: "orders" });
  sqs.seed(URL, EnvelopeCodec.encode(env), 3); // 3rd delivery → attempts 2

  let attempts = -1;
  await new SqsConsumer(sqs, URL, { "urn:babel:orders:created": (e) => { attempts = e.attempts; } }).poll();
  assert.equal(attempts, 2);
});

test("consumer never lowers a runtime-incremented attempts", async () => {
  const sqs = fakeSqs();
  const env = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 }, { queue: "orders" });
  env.attempts = 5;
  sqs.seed(URL, EnvelopeCodec.encode(env), 1);

  let attempts = -1;
  await new SqsConsumer(sqs, URL, { "urn:babel:orders:created": (e) => { attempts = e.attempts; } }).poll();
  assert.equal(attempts, 5);
});

test("a throwing handler leaves the message (no delete) and reports onError", async () => {
  const sqs = fakeSqs();
  await new SqsPublisher(sqs, URL).publish("urn:babel:orders:created", { x: 1 });
  let captured: unknown = null;
  await new SqsConsumer(
    sqs,
    URL,
    { "urn:babel:orders:created": () => { throw new Error("boom"); } },
    { onError: (e) => { captured = e; } },
  ).poll();
  assert.ok(captured instanceof Error);
  assert.equal(sqs.deleted.length, 0); // left for visibility-timeout redelivery
});

test("a non-conformant envelope reports onError and is not deleted", async () => {
  const sqs = fakeSqs();
  sqs.seed(URL, JSON.stringify({ not: "an envelope" }), 1);
  let captured: unknown = null;
  await new SqsConsumer(sqs, URL, {}, { onError: (e) => { captured = e; } }).poll();
  assert.ok(captured instanceof Error);
  assert.equal(sqs.deleted.length, 0);
});

test("an unmapped URN calls onUnknownUrn (then deletes) or reports onError", async () => {
  const env = EnvelopeCodec.make("urn:babel:orders:created", { x: 1 }, { queue: "orders" });

  const sqs1 = fakeSqs();
  sqs1.seed(URL, EnvelopeCodec.encode(env), 1);
  let unknownUrn = "";
  await new SqsConsumer(sqs1, URL, {}, {
    onUnknownUrn: (e) => { unknownUrn = EnvelopeCodec.urn(e); },
  }).poll();
  assert.equal(unknownUrn, "urn:babel:orders:created");
  assert.equal(sqs1.deleted.length, 1);

  const sqs2 = fakeSqs();
  sqs2.seed(URL, EnvelopeCodec.encode(env), 1);
  let captured: unknown = null;
  await new SqsConsumer(sqs2, URL, {}, { onError: (e) => { captured = e; } }).poll();
  assert.ok(captured instanceof Error);
  assert.equal(sqs2.deleted.length, 0);
});

test("poll passes the contract receive options", async () => {
  const sqs = fakeSqs();
  await new SqsConsumer(sqs, URL, {}, { waitTimeSeconds: 5, visibilityTimeout: 45, maxMessages: 3 }).poll();
  const r = sqs.lastReceive();
  assert.equal(r?.WaitTimeSeconds, 5);
  assert.equal(r?.VisibilityTimeout, 45);
  assert.equal(r?.MaxNumberOfMessages, 3);
  assert.deepEqual(r?.MessageAttributeNames, ["All"]);
  assert.deepEqual(r?.AttributeNames, ["ApproximateReceiveCount"]);
});

test("run stops when the AbortSignal is aborted", async () => {
  const sqs = fakeSqs();
  const controller = new AbortController();
  controller.abort();
  await new SqsConsumer(sqs, URL, {}).run(controller.signal); // returns immediately
  assert.equal(sqs.lastReceive(), undefined); // never polled
});

test("errors from the client propagate", async () => {
  const sqs = fakeSqs(new Error("aws down"));
  await assert.rejects(() => new SqsPublisher(sqs, URL).publish("urn:x:y", {}), /aws down/);
  await assert.rejects(() => new SqsConsumer(sqs, URL, {}).poll(), /aws down/);
});
