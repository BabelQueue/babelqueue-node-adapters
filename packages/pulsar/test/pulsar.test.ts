import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  PulsarConsumer,
  PulsarPublisher,
  pulsarProperties,
  toPulsarMessage,
  type BabelHandlers,
  type PulsarConsumerClient,
  type PulsarProducer,
  type PulsarProducerMessage,
  type PulsarReceivedMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  const env = EnvelopeCodec.make(URN, { order_id: 1042 }, { queue: "orders" });
  env.attempts = attempts;
  return env;
}

function body(attempts = 0): string {
  return EnvelopeCodec.encode(envelope(attempts));
}

class FakeProducer implements PulsarProducer {
  sent: PulsarProducerMessage[] = [];
  constructor(private readonly topic = "persistent://public/default/orders") {}
  getTopic(): string {
    return this.topic;
  }
  async send(message: PulsarProducerMessage): Promise<unknown> {
    this.sent.push(message);
    return { messageId: "m-1" };
  }
}

class FakeMessage implements PulsarReceivedMessage {
  constructor(
    private readonly raw: string,
    private readonly redeliveryCount = 0,
    private readonly properties: { [key: string]: string } = {},
  ) {}
  getData(): Buffer {
    return Buffer.from(this.raw, "utf8");
  }
  getProperties(): { [key: string]: string } {
    return this.properties;
  }
  getRedeliveryCount(): number {
    return this.redeliveryCount;
  }
}

class FakeConsumer implements PulsarConsumerClient {
  acked: PulsarReceivedMessage[] = [];
  nacked: PulsarReceivedMessage[] = [];
  constructor(private readonly messages: PulsarReceivedMessage[] = []) {}
  async receive(): Promise<PulsarReceivedMessage> {
    const message = this.messages.shift();
    if (message === undefined) throw new Error("Failed to receive message: TimeOut");
    return message;
  }
  acknowledge(message: PulsarReceivedMessage): void {
    this.acked.push(message);
  }
  negativeAcknowledge(message: PulsarReceivedMessage): void {
    this.nacked.push(message);
  }
}

// --- projection ---------------------------------------------------------------

test("projection maps contract fields onto string properties", () => {
  const env = envelope();
  const props = pulsarProperties(env);
  assert.equal(props["bq-job"], URN);
  assert.equal(props["bq-trace-id"], env.trace_id);
  assert.equal(props["bq-message-id"], env.meta.id);
  assert.equal(props["bq-schema-version"], String(env.meta.schema_version));
  assert.equal(props["bq-source-lang"], env.meta.lang);
  assert.equal(props["bq-attempts"], "0");
  assert.ok(Object.values(props).every((v) => typeof v === "string"));
});

test("toPulsarMessage carries the canonical envelope as the payload", () => {
  const env = envelope();
  const message = toPulsarMessage(env);
  assert.ok(Buffer.isBuffer(message.data));
  assert.equal(EnvelopeCodec.urn(EnvelopeCodec.decode(message.data.toString("utf8"))), URN);
  assert.equal(message.properties?.["bq-job"], URN);
});

// --- publisher ----------------------------------------------------------------

test("publish projects properties and returns message id", async () => {
  const producer = new FakeProducer();
  const id = await new PulsarPublisher(producer).publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  assert.equal(producer.sent.length, 1);
  const message = producer.sent[0]!;
  assert.equal(message.properties?.["bq-job"], URN);
  assert.equal(message.properties?.["bq-trace-id"], "trace-1");
  assert.equal(message.properties?.["bq-message-id"], id);
  assert.equal(EnvelopeCodec.urn(EnvelopeCodec.decode(message.data.toString("utf8"))), URN);
});

test("publish without a trace id mints a fresh trace", async () => {
  const producer = new FakeProducer();
  await new PulsarPublisher(producer).publish(URN, { order_id: 7 });
  assert.ok(producer.sent[0]!.properties?.["bq-trace-id"]);
});

test("publish with a delay sets deliverAfter and bq-delay", async () => {
  const producer = new FakeProducer();
  await new PulsarPublisher(producer).publish(URN, {}, { delayMs: 30000 });
  const message = producer.sent[0]!;
  assert.equal(message.deliverAfter, 30000);
  assert.equal(message.properties?.["bq-delay"], "30000");
});

// --- consumer -----------------------------------------------------------------

test("attempts is the redelivery count and the message is acknowledged", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 2)]);
  let seen = -1;
  const handlers: BabelHandlers = { [URN]: (env) => void (seen = env.attempts ?? -1) };
  const count = await new PulsarConsumer(consumer, handlers).poll();
  assert.equal(count, 1);
  assert.equal(seen, 2);
  assert.equal(consumer.acked.length, 1);
});

test("first delivery is zero attempts", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let seen = -1;
  await new PulsarConsumer(consumer, { [URN]: (env) => void (seen = env.attempts ?? -1) }).poll();
  assert.equal(seen, 0);
});

test("a higher body attempt count is never lowered by the redelivery count", async () => {
  // Republish-driven retry carried attempts=5 in the body; redelivery count is only 1.
  const consumer = new FakeConsumer([new FakeMessage(body(5), 1)]);
  let seen = -1;
  await new PulsarConsumer(consumer, { [URN]: (env) => void (seen = env.attempts ?? -1) }).poll();
  assert.equal(seen, 5);
});

test("a throwing handler negative-acknowledges and reports onError", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let reported: unknown = null;
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await new PulsarConsumer(consumer, handlers, { onError: (e) => void (reported = e) }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
  assert.equal(consumer.acked.length, 0);
});

test("an unknown URN with a hook acknowledges", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let called = false;
  await new PulsarConsumer(consumer, {}, { onUnknownUrn: () => void (called = true) }).poll();
  assert.equal(called, true);
  assert.equal(consumer.acked.length, 1);
});

test("an unknown URN without a hook negative-acknowledges and reports onError", async () => {
  const consumer = new FakeConsumer([new FakeMessage(body(0), 0)]);
  let reported: unknown = null;
  await new PulsarConsumer(consumer, {}, { onError: (e) => void (reported = e) }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
});

test("a non-conformant envelope negative-acknowledges and reports onError", async () => {
  const bad = JSON.stringify({
    trace_id: "t",
    data: { x: 1 },
    meta: { id: "m", queue: "q", lang: "node", schema_version: 1, created_at: 1 },
    attempts: 0,
  });
  const consumer = new FakeConsumer([new FakeMessage(bad, 0)]);
  let reported: unknown = null;
  await new PulsarConsumer(consumer, {}, { onError: (e) => void (reported = e) }).poll();
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
});

// --- §5.2 / §5.7 routing and attempts reconciliation ---------------------------

const OTHER = "urn:babel:orders:refunded";

function bodyWithUrn(job: string, attempts = 0): string {
  const env = envelope(attempts);
  env.job = job;
  return EnvelopeCodec.encode(env);
}

test("bq-job property routes the message even when the body names a different URN", async () => {
  const consumer = new FakeConsumer([new FakeMessage(bodyWithUrn(OTHER), 0, { "bq-job": URN })]);
  const seen: string[] = [];
  const handlers: BabelHandlers = {
    [URN]: (env) => void seen.push(`property:${EnvelopeCodec.urn(env)}`),
    [OTHER]: () => void seen.push("body"),
  };
  await new PulsarConsumer(consumer, handlers).poll();
  // routed to the property's handler; the handler still gets the body's envelope
  assert.deepEqual(seen, [`property:${OTHER}`]);
  assert.equal(consumer.acked.length, 1);
});

test("onRoutingMismatch is called with both URNs and the body envelope; onError is not", async () => {
  const consumer = new FakeConsumer([new FakeMessage(bodyWithUrn(OTHER), 0, { "bq-job": `  ${URN}  ` })]);
  const calls: Array<[string, string, string]> = [];
  const errors: unknown[] = [];
  await new PulsarConsumer(
    consumer,
    { [URN]: () => undefined },
    {
      onRoutingMismatch: (propertyUrn, bodyUrn, env) => void calls.push([propertyUrn, bodyUrn, EnvelopeCodec.urn(env)]),
      onError: (e) => void errors.push(e),
    },
  ).poll();
  assert.deepEqual(calls, [[URN, OTHER, OTHER]]);
  assert.equal(errors.length, 0);
  assert.equal(consumer.acked.length, 1);
});

test("onRoutingMismatch is not called when there is no mismatch", async () => {
  const noJob = JSON.stringify({ trace_id: "t", data: {}, meta: { id: "m", queue: "q", lang: "node", schema_version: 1, created_at: 1 }, attempts: 0 });
  const cases: Array<[string, { [key: string]: string }]> = [
    [body(), { "bq-job": URN }],
    [body(), { "bq-job": `  ${URN}  ` }],
    [body(), {}],
    [body(), { "bq-job": "   " }],
    [noJob, { "bq-job": URN }],
  ];
  for (const [raw, props] of cases) {
    let called = false;
    const consumer = new FakeConsumer([new FakeMessage(raw, 0, props)]);
    await new PulsarConsumer(consumer, { [URN]: () => undefined }, { onRoutingMismatch: () => void (called = true), onError: () => undefined }).poll();
    assert.equal(called, false, JSON.stringify(props));
  }
});

test("a throwing or rejecting onRoutingMismatch hook still routes and acknowledges", async () => {
  for (const hook of [
    () => {
      throw new Error("hook failure");
    },
    () => Promise.reject(new Error("hook failure")),
  ]) {
    const consumer = new FakeConsumer([new FakeMessage(bodyWithUrn(OTHER), 0, { "bq-job": URN })]);
    let handled = 0;
    let reported = 0;
    await new PulsarConsumer(consumer, { [URN]: () => void handled++ }, { onRoutingMismatch: hook, onError: () => void reported++ }).poll();
    assert.equal(handled, 1);
    assert.equal(reported, 0);
    assert.equal(consumer.acked.length, 1);
    assert.equal(consumer.nacked.length, 0);
  }
});

test("blank or missing bq-job falls back to the body URN", async () => {
  for (const props of [{}, { "bq-job": "" }, { "bq-job": "   " }] as Array<{ [key: string]: string }>) {
    const consumer = new FakeConsumer([new FakeMessage(body(), 0, props)]);
    let handled = 0;
    await new PulsarConsumer(consumer, { [URN]: () => void handled++ }).poll();
    assert.equal(handled, 1, JSON.stringify(props));
    assert.equal(consumer.acked.length, 1);
  }
});

test("a body without a job is rejected even when bq-job is set", async () => {
  const noJob = JSON.stringify({ trace_id: "t", data: {}, meta: { id: "m", queue: "q", lang: "node", schema_version: 1, created_at: 1 }, attempts: 0 });
  const consumer = new FakeConsumer([new FakeMessage(noJob, 0, { "bq-job": URN })]);
  let handled = 0;
  let reported: unknown = null;
  await new PulsarConsumer(consumer, { [URN]: () => void handled++ }, { onError: (e) => void (reported = e) }).poll();
  assert.equal(handled, 0);
  assert.ok(reported instanceof Error);
  assert.equal(consumer.nacked.length, 1);
  assert.equal(consumer.acked.length, 0);
});

async function attemptsSeen(raw: string, props: { [key: string]: string }, redelivery = 0): Promise<number> {
  const consumer = new FakeConsumer([new FakeMessage(raw, redelivery, props)]);
  let seen = -1;
  await new PulsarConsumer(consumer, { [URN]: (env) => void (seen = env.attempts ?? -1) }).poll();
  return seen;
}

test("bq-attempts is authoritative over the body", async () => {
  const cases: Array<[string, number, string, number, number]> = [
    ["property replaces body", 5, "3", 0, 3],
    ["property raises body", 1, "4", 0, 4],
    ["redelivery raises property", 0, "2", 6, 6],
    ["property above redelivery", 0, "7", 2, 7],
    ["leading zeros are digits", 0, "007", 0, 7],
    ["zero property lowers body", 5, "0", 0, 0],
    ["int32 max is valid", 0, "2147483647", 0, 2147483647],
  ];
  for (const [name, bodyAttempts, prop, redelivery, want] of cases) {
    assert.equal(await attemptsSeen(body(bodyAttempts), { "bq-attempts": prop }, redelivery), want, name);
  }
});

test("an invalid bq-attempts falls back to the body value", async () => {
  const bad = ["", " ", "abc", "-1", "+3", "-0", "3.5", "1e2", " 3", "3 ", "3\n", " 3x", "\u0663", "2147483648", "99999999999999999999"];
  for (const value of bad) {
    assert.equal(await attemptsSeen(body(5), { "bq-attempts": value }), 5, JSON.stringify(value));
  }
  assert.equal(await attemptsSeen(body(5), { "bq-attempts": "nope" }, 9), 9, "redelivery still applies");
});

test("a missing bq-attempts falls back to the body value", async () => {
  assert.equal(await attemptsSeen(body(5), {}), 5);
});

test("poll returns 0 on a receive timeout", async () => {
  const consumer = new FakeConsumer([]);
  const count = await new PulsarConsumer(consumer, {}).poll();
  assert.equal(count, 0);
});

test("poll rethrows a non-timeout receive error", async () => {
  const consumer: PulsarConsumerClient = {
    async receive() {
      throw new Error("connection refused");
    },
    acknowledge() {},
    negativeAcknowledge() {},
  };
  await assert.rejects(new PulsarConsumer(consumer, {}).poll(), /connection refused/);
});

test("run stops once the signal is aborted", async () => {
  const consumer = new FakeConsumer([]);
  const controller = new AbortController();
  controller.abort();
  await new PulsarConsumer(consumer, {}).run(controller.signal); // returns immediately
  assert.equal(consumer.acked.length, 0);
});

// --- Fix round 1: attempts repair and hook order -------------------------------

test("a missing or non-integer body attempts stays rejected, whatever bq-attempts / redelivery say", async () => {
  const base = JSON.parse(body()) as { [key: string]: unknown };
  const variants: Array<[string, (env: { [key: string]: unknown }) => void]> = [
    ["missing", (env) => void delete env.attempts],
    ["string", (env) => void (env.attempts = "3")],
    ["null", (env) => void (env.attempts = null)],
    ["float", (env) => void (env.attempts = 1.5)],
  ];
  for (const [name, mutate] of variants) {
    const env = JSON.parse(JSON.stringify(base)) as { [key: string]: unknown };
    mutate(env);
    const consumer = new FakeConsumer([new FakeMessage(JSON.stringify(env), 2, { "bq-attempts": "4" })]);
    let handled = 0;
    let reported: unknown = null;
    await new PulsarConsumer(consumer, { [URN]: () => void handled++ }, { onError: (e) => void (reported = e) }).poll();
    assert.equal(handled, 0, name);
    assert.ok(reported instanceof Error, name);
    assert.equal(consumer.nacked.length, 1, name);
    assert.equal(consumer.acked.length, 0, name);
  }
});

test("attempts is written only when the integer body value really changes", async () => {
  const seenRaw: string[] = [];
  const capture = (env: Envelope): void => void seenRaw.push(JSON.stringify(env));
  const same = body(3);
  await new PulsarConsumer(new FakeConsumer([new FakeMessage(same, 3, { "bq-attempts": "3" })]), { [URN]: capture }).poll();
  assert.equal(seenRaw[0], JSON.stringify(JSON.parse(same)));
  assert.equal(await attemptsSeen(body(3), { "bq-attempts": "4" }), 4);
});

test("onRoutingMismatch sees the reconciled attempts", async () => {
  const consumer = new FakeConsumer([new FakeMessage(bodyWithUrn(OTHER), 0, { "bq-job": URN, "bq-attempts": "4" })]);
  const seen: unknown[] = [];
  await new PulsarConsumer(consumer, { [URN]: () => undefined }, { onRoutingMismatch: (_p, _b, env) => void seen.push(env.attempts) }).poll();
  assert.deepEqual(seen, [4]);
});

test("onRoutingMismatch is not called for a rejected envelope", async () => {
  const bad = JSON.parse(bodyWithUrn(OTHER)) as { meta: { schema_version: number } };
  bad.meta.schema_version = 2;
  const consumer = new FakeConsumer([new FakeMessage(JSON.stringify(bad), 0, { "bq-job": URN })]);
  let called = false;
  await new PulsarConsumer(consumer, { [URN]: () => undefined }, { onRoutingMismatch: () => void (called = true), onError: () => undefined }).poll();
  assert.equal(called, false);
  assert.equal(consumer.nacked.length, 1);
});
