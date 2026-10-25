import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  DEFAULT_MAX_ATTEMPTS,
  type PulsarClientLike,
  PulsarConsumer,
  type BabelHandlers,
  type PulsarConsumerClient,
  type PulsarProducer,
  type PulsarProducerMessage,
  type PulsarReceivedMessage,
} from "../src/index.js";
import { sharedConsumer } from "./support.js";

// ADR-0036 decisions 1, 2, 4 and 8: bounded redelivery, the poison route, dead_letter.attempts.

const URN = "urn:babel:orders:created";

function bodyOf(attempts = 0): string {
  const env = EnvelopeCodec.make(URN, { order_id: 1042 }, { queue: "orders" });
  env.attempts = attempts;
  return EnvelopeCodec.encode(env);
}

class FakeMessage implements PulsarReceivedMessage {
  constructor(
    private readonly raw: Buffer | string,
    private redeliveryCount = 0,
    private readonly properties: { [key: string]: string } = {},
  ) {}
  getData(): Buffer | string {
    return this.raw;
  }
  getProperties(): { [key: string]: string } {
    return this.properties;
  }
  getRedeliveryCount(): number {
    return this.redeliveryCount;
  }
  /** The broker redelivers a negatively acknowledged message with the count advanced. */
  redeliver(): void {
    this.redeliveryCount += 1;
  }
}

/** Serves one message repeatedly the way a Shared subscription redelivers a nacked message. */
class RedeliveringConsumer implements PulsarConsumerClient {
  acked = 0;
  nacked = 0;
  constructor(private readonly message: FakeMessage) {}
  async receive(): Promise<PulsarReceivedMessage> {
    return this.message;
  }
  acknowledge(): void {
    this.acked += 1;
  }
  negativeAcknowledge(): void {
    this.nacked += 1;
    this.message.redeliver();
  }
}

class FakeProducer implements PulsarProducer {
  sent: PulsarProducerMessage[] = [];
  failures = 0;
  getTopic(): string {
    return "persistent://public/default/orders.dlq";
  }
  async send(message: PulsarProducerMessage): Promise<unknown> {
    if (this.failures > 0) {
      this.failures -= 1;
      throw new Error("broker down");
    }
    this.sent.push(message);
    return {};
  }
}

const failing: BabelHandlers = {
  [URN]: () => {
    throw new Error("boom");
  },
};

/** Poll until the message is acknowledged; returns the number of deliveries (capped to catch a loop). */
async function deliveriesUntilAck(consumer: PulsarConsumer, client: RedeliveringConsumer): Promise<number> {
  let deliveries = 0;
  while (client.acked === 0 && deliveries < 50) {
    await consumer.poll();
    deliveries += 1;
  }
  return deliveries;
}

function dlqRecord(producer: FakeProducer): Record<string, unknown> & { dead_letter: Record<string, unknown> } {
  assert.equal(producer.sent.length, 1);
  return JSON.parse((producer.sent[0] as PulsarProducerMessage).data.toString("utf8"));
}

test("the default max attempts is 3", () => {
  assert.equal(DEFAULT_MAX_ATTEMPTS, 3);
});

test("a handler that keeps failing is dead-lettered on its 3rd delivery, not redelivered forever", async () => {
  const message = new FakeMessage(bodyOf(0));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  const errors: unknown[] = [];
  const consumer = sharedConsumer(client, failing, { deadLetterProducer: producer, onError: (e) => errors.push(e) });

  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(client.nacked, 2);
  assert.equal(errors.length, 3);

  const record = dlqRecord(producer);
  assert.equal(record.dead_letter.reason, "failed");
  assert.equal(record.dead_letter.attempts, 3);
  assert.equal(record.dead_letter.original_queue, "orders");
  assert.equal(record.dead_letter.error, "boom");
  assert.equal(record.attempts, 3);
  assert.equal(record.job, URN);
  assert.equal((producer.sent[0] as PulsarProducerMessage).properties?.["bq-attempts"], "3");
  assert.equal((producer.sent[0] as PulsarProducerMessage).properties?.["bq-job"], URN);
});

test("with dead-lettering off the 3rd delivery is acknowledged and dropped with a warning", async () => {
  const message = new FakeMessage(bodyOf(0));
  const client = new RedeliveringConsumer(message);
  const warnings: string[] = [];
  const consumer = sharedConsumer(client, failing, { onWarn: (w) => warnings.push(w) });

  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(client.nacked, 2);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0] as string, /dropped a failed/);
});

test("an unmapped URN without a hook is bounded and dead-lettered with reason unknown_urn", async () => {
  const message = new FakeMessage(bodyOf(0));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  const consumer = sharedConsumer(client, {}, { deadLetterProducer: producer });

  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  const record = dlqRecord(producer);
  assert.equal(record.dead_letter.reason, "unknown_urn");
  assert.equal(record.dead_letter.attempts, 3);
});

test("maxAttempts 1 means no retry: the first failure is dead-lettered with attempts 1", async () => {
  const message = new FakeMessage(bodyOf(0));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  await sharedConsumer(client, failing, { maxAttempts: 1, deadLetterProducer: producer }).poll();
  assert.equal(client.nacked, 0);
  assert.equal(client.acked, 1);
  assert.equal(dlqRecord(producer).dead_letter.attempts, 1);
});

test("an onUnknownUrn hook keeps its behaviour: the hook runs and the message is acknowledged", async () => {
  const message = new FakeMessage(bodyOf(5));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  let hooked = 0;
  await sharedConsumer(client, {}, { deadLetterProducer: producer, onUnknownUrn: () => void hooked++ }).poll();
  assert.equal(hooked, 1);
  assert.equal(client.acked, 1);
  assert.equal(producer.sent.length, 0);
});

test("the dead-letter record keeps the redelivered attempts (bq-attempts floored by the redelivery count)", async () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(0), 4, { "bq-attempts": "1" }));
  const producer = new FakeProducer();
  await sharedConsumer(client, failing, { deadLetterProducer: producer }).poll();
  assert.equal(dlqRecord(producer).dead_letter.attempts, 5);
});

test("a failed DLQ write leaves the original negatively acknowledged, and the next delivery writes it", async () => {
  const message = new FakeMessage(bodyOf(2));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  producer.failures = 1;
  const warnings: string[] = [];
  const consumer = sharedConsumer(client, failing, { deadLetterProducer: producer, onWarn: (w) => warnings.push(w) });

  await consumer.poll();
  assert.equal(client.acked, 0);
  assert.equal(client.nacked, 1);
  assert.equal(producer.sent.length, 0);
  assert.match(warnings[0] as string, /dead-letter write to .*orders\.dlq failed \(broker down\)/);

  await consumer.poll();
  assert.equal(client.acked, 1);
  assert.equal(producer.sent.length, 1);
});

test("maxAttempts must be an integer of at least 1", () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(0)));
  for (const bad of [0, -1, 1.5, Number.NaN]) {
    assert.throws(() => sharedConsumer(client, {}, { maxAttempts: bad }), /maxAttempts must be an integer of at least 1/);
  }
});

test("the default onWarn writes to console.warn", async () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(2)));
  const original = console.warn;
  const seen: string[] = [];
  console.warn = (m: unknown) => void seen.push(String(m));
  try {
    await sharedConsumer(client, failing).poll();
  } finally {
    console.warn = original;
  }
  assert.equal(seen.length, 1);
});

// --- poison (decisions 2 and 4) ----------------------------------------------------

const bodies: [string, Buffer | string][] = [
  ["not JSON", "not-json"],
  ["an empty body", ""],
  ["a JSON array", "[1,2]"],
  ["an identity-less object", '{"data":{"x":1},"attempts":0}'],
  ["an object with a blank job and no urn", '{"job":"  ","data":{}}'],
  ["invalid UTF-8", Buffer.from([0xff, 0xfe, 0x00, 0x7b])],
];

for (const [label, raw] of bodies) {
  test(`poison (${label}) with dead-lettering on goes to <queue>.dlq at once, never to a handler`, async () => {
    const client = new RedeliveringConsumer(new FakeMessage(raw));
    const producer = new FakeProducer();
    let handled = 0;
    let hooked = 0;
    await sharedConsumer(
      client,
      { [URN]: () => void handled++ },
      { deadLetterProducer: producer, onUnknownUrn: () => void hooked++ },
    ).poll();

    assert.equal(handled, 0);
    assert.equal(hooked, 0);
    assert.equal(client.nacked, 0, "no retry");
    assert.equal(client.acked, 1);
    const record = dlqRecord(producer);
    assert.equal(record.dead_letter.reason, "poison");
    assert.equal(record.dead_letter.original_queue, "orders", "queue is the DLQ topic without .dlq");
  });
}

test("a poison body keeps its bytes: an object is spliced, anything else is wrapped (ADR-0035)", async () => {
  const spliced = new FakeProducer();
  await sharedConsumer(
    new RedeliveringConsumer(new FakeMessage('{"data":{"big":1e400},"attempts":0}')),
    {},
    { deadLetterProducer: spliced },
  ).poll();
  const text = (spliced.sent[0] as PulsarProducerMessage).data.toString("utf8");
  assert.ok(text.startsWith('{"data":{"big":1e400},"attempts":0,"dead_letter":{'), text);

  const wrapped = new FakeProducer();
  await sharedConsumer(
    new RedeliveringConsumer(new FakeMessage(Buffer.from([0xff, 0xfe, 0x41]))),
    {},
    { deadLetterProducer: wrapped },
  ).poll();
  const record = dlqRecord(wrapped);
  assert.equal(record["raw"], Buffer.from([0xff, 0xfe, 0x41]).toString("base64"));
  assert.equal(record["raw_encoding"], "base64");

  const text2 = new FakeProducer();
  await sharedConsumer(
    new RedeliveringConsumer(new FakeMessage("not-json")),
    {},
    { deadLetterProducer: text2 },
  ).poll();
  const record2 = dlqRecord(text2);
  assert.equal(record2["raw"], "not-json");
  assert.equal(record2["raw_encoding"], undefined);
});

test("an identity-less object that still carries a trace id and message id projects them onto the DLQ message", async () => {
  const producer = new FakeProducer();
  await sharedConsumer(
    new RedeliveringConsumer(new FakeMessage('{"trace_id":"t-1","meta":{"id":"m-1"},"data":{}}')),
    {},
    { deadLetterProducer: producer },
  ).poll();
  assert.deepEqual((producer.sent[0] as PulsarProducerMessage).properties, { "bq-trace-id": "t-1", "bq-message-id": "m-1" });
});

test("the poison queue name is the queue option when given", async () => {
  const producer = new FakeProducer();
  await sharedConsumer(
    new RedeliveringConsumer(new FakeMessage("not-json")),
    {},
    { deadLetterProducer: producer, queue: "billing" },
  ).poll();
  assert.equal(dlqRecord(producer).dead_letter.original_queue, "billing");
});

test("poison with dead-lettering off takes the bounded retry, then a drop", async () => {
  const message = new FakeMessage("not-json");
  const client = new RedeliveringConsumer(message);
  const warnings: string[] = [];
  const consumer = sharedConsumer(client, failing, { onWarn: (w) => warnings.push(w) });
  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(client.nacked, 2);
  assert.match(warnings[0] as string, /dropped a poison/);
});

test("poison counts attempts from bq-attempts and the redelivery count, not from the body", async () => {
  const client = new RedeliveringConsumer(new FakeMessage('{"attempts":0,"data":{}}', 0, { "bq-attempts": "2" }));
  await sharedConsumer(client, {}, { onWarn: () => undefined }).poll();
  assert.equal(client.nacked, 0);
  assert.equal(client.acked, 1, "reconciled 2 + 1 reaches the default cap of 3");
});

test("a failed DLQ write of a poison body leaves it negatively acknowledged", async () => {
  const client = new RedeliveringConsumer(new FakeMessage("not-json"));
  const producer = new FakeProducer();
  producer.failures = 1;
  const consumer = sharedConsumer(client, {}, { deadLetterProducer: producer, onWarn: () => undefined });
  await consumer.poll();
  assert.equal(client.acked, 0);
  assert.equal(client.nacked, 1);
  await consumer.poll();
  assert.equal(client.acked, 1);
  assert.equal(dlqRecord(producer).dead_letter.reason, "poison");
});

test("a message routed on bq-job to a handler still fails through the bounded path", async () => {
  const env: Envelope = EnvelopeCodec.make("urn:babel:other", {}, { queue: "orders" });
  const client = new RedeliveringConsumer(new FakeMessage(EnvelopeCodec.encode(env), 0, { "bq-job": URN }));
  const producer = new FakeProducer();
  const consumer = sharedConsumer(client, failing, { deadLetterProducer: producer });
  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(dlqRecord(producer).dead_letter.reason, "failed");
});

// --- retryProducer: Go-model retry, bounded on every subscription type (Exclusive / Failover) ------------

/**
 * An Exclusive / Failover subscription: pulsar-client's negative acknowledgement redelivers the message
 * but its redelivery count stays 0, so a nack-driven retry never advances the native count. A re-published
 * message is a new message (count 0 as well): only `bq-attempts` carries the attempts.
 */
class ExclusiveTopic implements PulsarConsumerClient, PulsarProducer {
  queue: FakeMessage[] = [];
  acked = 0;
  nacked = 0;
  sent: PulsarProducerMessage[] = [];
  failSends = 0;
  constructor(first: FakeMessage) {
    this.queue.push(first);
  }
  getTopic(): string {
    return "persistent://public/default/orders";
  }
  async receive(): Promise<PulsarReceivedMessage> {
    const next = this.queue.shift();
    if (next === undefined) throw new Error("Failed to receive message: TimeOut");
    return next;
  }
  acknowledge(): void {
    this.acked += 1;
  }
  negativeAcknowledge(message: PulsarReceivedMessage): void {
    this.nacked += 1;
    this.queue.push(message as FakeMessage); // redelivered unchanged, redelivery count stays 0
  }
  async send(message: PulsarProducerMessage): Promise<unknown> {
    if (this.failSends > 0) {
      this.failSends -= 1;
      throw new Error("broker down");
    }
    this.sent.push(message);
    this.queue.push(new FakeMessage(message.data, 0, message.properties ?? {}));
    return {};
  }
}

async function drain(consumer: PulsarConsumer, topic: ExclusiveTopic, limit = 20): Promise<number> {
  let polls = 0;
  // Done when nothing is left to deliver: a retry acknowledges the original but re-publishes a new message.
  while (topic.queue.length > 0 && polls < limit) {
    await consumer.poll();
    polls += 1;
  }
  return polls;
}

test("Exclusive/Failover regression: without a retryProducer a nack never advances the count (the loop this option closes)", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  const consumer = sharedConsumer(topic, failing, { deadLetterProducer: new FakeProducer(), onError: () => undefined });
  assert.equal(await drain(consumer, topic), 20, "still not terminated after 20 deliveries");
  assert.equal(topic.nacked, 20);
});

test("Exclusive/Failover: with a retryProducer a failing handler stops at its 3rd delivery and is dead-lettered", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  const dlq = new FakeProducer();
  const consumer = sharedConsumer(topic, failing, { retryProducer: topic, deadLetterProducer: dlq, onError: () => undefined });

  assert.equal(await drain(consumer, topic), 3);
  assert.equal(topic.nacked, 0, "retry is a re-publish + ack, never a nack");
  assert.equal(topic.sent.length, 2);
  assert.equal(topic.acked, 3, "each delivery is acknowledged (2 retries + the dead-letter)");
  const record = dlqRecord(dlq);
  assert.equal(record.dead_letter.reason, "failed");
  assert.equal(record.dead_letter.attempts, 3);
});

test("a retry re-publishes bq-attempts = counted with the body's attempts in sync, on the consumer's own topic", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0), 0, { traceparent: "00-aa-bb-01", "bq-delay": "500" }));
  await sharedConsumer(topic, failing, { retryProducer: topic, onError: () => undefined }).poll();

  assert.equal(topic.sent.length, 1);
  const sent = topic.sent[0] as PulsarProducerMessage;
  assert.equal(sent.properties?.["bq-attempts"], "1");
  assert.equal(sent.properties?.["bq-job"], URN);
  assert.equal(sent.properties?.["traceparent"], "00-aa-bb-01", "out-of-band headers are carried");
  assert.equal(sent.properties?.["bq-delay"], undefined, "the one-shot delay is not repeated");
  assert.equal((JSON.parse(sent.data.toString("utf8")) as { attempts: number }).attempts, 1);
  assert.equal(topic.acked, 1);
  assert.equal(topic.nacked, 0);
});

test("an unmapped URN is bounded the same way on Exclusive/Failover", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  const dlq = new FakeProducer();
  const consumer = sharedConsumer(topic, {}, { retryProducer: topic, deadLetterProducer: dlq, onError: () => undefined });
  assert.equal(await drain(consumer, topic), 3);
  assert.equal(dlqRecord(dlq).dead_letter.reason, "unknown_urn");
});

test("poison with dead-lettering off is retried byte for byte with bq-attempts + 1, then dropped (Exclusive/Failover)", async () => {
  const raw = Buffer.from([0xff, 0xfe, 0x41]);
  const topic = new ExclusiveTopic(new FakeMessage(raw));
  const warnings: string[] = [];
  const consumer = sharedConsumer(topic, failing, { retryProducer: topic, onError: () => undefined, onWarn: (w) => warnings.push(w) });

  assert.equal(await drain(consumer, topic), 3);
  assert.equal(topic.sent.length, 2);
  assert.deepEqual((topic.sent[0] as PulsarProducerMessage).data, raw, "body bytes untouched");
  assert.equal((topic.sent[0] as PulsarProducerMessage).properties?.["bq-attempts"], "1");
  assert.equal((topic.sent[1] as PulsarProducerMessage).properties?.["bq-attempts"], "2");
  assert.match(warnings[0] as string, /dropped a poison/);
});

test("a failed retry re-publish leaves the original negatively acknowledged and the next delivery retries again", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  topic.failSends = 1;
  const warnings: string[] = [];
  const consumer = sharedConsumer(topic, failing, { retryProducer: topic, onError: () => undefined, onWarn: (w) => warnings.push(w) });

  await consumer.poll();
  assert.equal(topic.acked, 0);
  assert.equal(topic.nacked, 1);
  assert.match(warnings[0] as string, /retry re-publish to .*orders failed \(broker down\)/);

  await consumer.poll();
  assert.equal(topic.acked, 1);
  assert.equal(topic.sent.length, 1);
});

test("a re-published message and the native redelivery count reconcile to the higher value", async () => {
  // bq-attempts 1 (a re-publish) with a Shared redelivery count of 2: reconciled 2, counted 3 -> dead-letter.
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(1), 2, { "bq-attempts": "1" }));
  const dlq = new FakeProducer();
  await sharedConsumer(topic, failing, { retryProducer: topic, deadLetterProducer: dlq, onError: () => undefined }).poll();
  assert.equal(dlqRecord(dlq).dead_letter.attempts, 3);
  assert.equal(topic.sent.length, 0);
});

// --- the incoming route (bq-job) survives a retry and the DLQ record (Go publishKeepingRoute) ----------

test("a message routed on bq-job stays on that handler across retries and reaches the DLQ as failed", async () => {
  const OTHER = "urn:babel:other";
  const env = EnvelopeCodec.make(OTHER, { order_id: 1 }, { queue: "orders" }); // the body names another URN
  const topic = new ExclusiveTopic(new FakeMessage(EnvelopeCodec.encode(env), 0, { "bq-job": URN }));
  const dlq = new FakeProducer();
  const routedTo: string[] = [];
  const consumer = sharedConsumer(
    topic,
    {
      [URN]: () => {
        routedTo.push(URN);
        throw new Error("boom");
      },
      [OTHER]: () => {
        routedTo.push(OTHER);
      },
    },
    { retryProducer: topic, deadLetterProducer: dlq, onError: () => undefined },
  );

  assert.equal(await drain(consumer, topic), 3);
  assert.deepEqual(routedTo, [URN, URN, URN], "every delivery reaches the bq-job handler");
  for (const sent of topic.sent) assert.equal(sent.properties?.["bq-job"], URN);
  const record = dlqRecord(dlq);
  assert.equal(record.dead_letter.reason, "failed");
  assert.equal((dlq.sent[0] as PulsarProducerMessage).properties?.["bq-job"], URN, "the DLQ record keeps the route");
});

test("a message without a bq-job property is published with its body URN", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  await sharedConsumer(topic, failing, { retryProducer: topic, onError: () => undefined }).poll();
  assert.equal((topic.sent[0] as PulsarProducerMessage).properties?.["bq-job"], URN);
});

test("a poison body keeps the incoming bq-job on its retry and its DLQ record", async () => {
  const retry = new ExclusiveTopic(new FakeMessage("not-json", 0, { "bq-job": URN }));
  await sharedConsumer(retry, failing, { retryProducer: retry, onWarn: () => undefined, onError: () => undefined }).poll();
  assert.equal((retry.sent[0] as PulsarProducerMessage).properties?.["bq-job"], URN);

  const dlq = new FakeProducer();
  await sharedConsumer(new RedeliveringConsumer(new FakeMessage("not-json", 0, { "bq-job": URN })), {}, { deadLetterProducer: dlq }).poll();
  assert.equal((dlq.sent[0] as PulsarProducerMessage).properties?.["bq-job"], URN);
});

// --- the consumer builds its own retry producer from the client; Exclusive/Failover are checked at setup ---

/** An Exclusive topic that also acts as the client: createProducer hands out the topic as the producer. */
class ClientOver implements PulsarClientLike {
  created: { topic: string }[] = [];
  failures = 0;
  constructor(private readonly topic: ExclusiveTopic) {}
  async createProducer(config: { topic: string }): Promise<PulsarProducer> {
    if (this.failures > 0) {
      this.failures -= 1;
      throw new Error("no producer");
    }
    this.created.push(config);
    return this.topic;
  }
}

class TopicAwareExclusive extends ExclusiveTopic {
  override getTopic(): string {
    return "persistent://public/default/orders";
  }
}

test("with a client the consumer builds the retry producer itself, once, on the subscription's topic", async () => {
  const topic = new TopicAwareExclusive(new FakeMessage(bodyOf(0)));
  const client = new ClientOver(topic);
  const dlq = new FakeProducer();
  const consumer = new PulsarConsumer(topic, failing, {
    client,
    subscriptionType: "Exclusive",
    deadLetterProducer: dlq,
    onError: () => undefined,
  });

  assert.equal(client.created.length, 0, "lazy: nothing is created before the first retry");
  assert.equal(await drain(consumer, topic), 3);
  assert.deepEqual(client.created, [{ topic: "persistent://public/default/orders" }], "created once");
  assert.equal(topic.nacked, 0);
  assert.equal(dlqRecord(dlq).dead_letter.attempts, 3);
});

/**
 * The real pulsar-client `Consumer` shape: receive / acknowledge / negativeAcknowledge and NO `getTopic()`
 * (only pulsar-client's Producer has one). Redelivery on Exclusive/Failover leaves the count at 0.
 */
class RealShapeConsumer implements PulsarConsumerClient {
  queue: FakeMessage[] = [];
  acked = 0;
  nacked = 0;
  constructor(first: FakeMessage) {
    this.queue.push(first);
  }
  async receive(): Promise<PulsarReceivedMessage> {
    const next = this.queue.shift();
    if (next === undefined) throw new Error("Failed to receive message: TimeOut");
    return next;
  }
  acknowledge(): void {
    this.acked += 1;
  }
  negativeAcknowledge(message: PulsarReceivedMessage): void {
    this.nacked += 1;
    this.queue.push(message as FakeMessage);
  }
}

test("pulsar-client shape (no Consumer.getTopic): a client alone on Exclusive / Failover / undeclared throws and asks for `topic`", () => {
  const real = new RealShapeConsumer(new FakeMessage(bodyOf(0)));
  assert.equal((real as { getTopic?: unknown }).getTopic, undefined);
  const client: PulsarClientLike = { createProducer: async () => new FakeProducer() };
  for (const subscriptionType of ["Exclusive", "Failover", undefined] as const) {
    assert.throws(
      () => new PulsarConsumer(real, failing, { client, ...(subscriptionType ? { subscriptionType } : {}) }),
      /with a client needs the `topic` option.*cannot report its own topic/s,
      String(subscriptionType),
    );
  }
});

test("pulsar-client shape (no Consumer.getTopic): client + topic builds the retry producer on that topic and bounds Exclusive", async () => {
  const real = new RealShapeConsumer(new FakeMessage(bodyOf(0)));
  const created: { topic: string }[] = [];
  const republished: PulsarProducerMessage[] = [];
  const client: PulsarClientLike = {
    async createProducer(config) {
      created.push(config);
      return {
        getTopic: () => config.topic,
        async send(message: PulsarProducerMessage) {
          republished.push(message);
          real.queue.push(new FakeMessage(message.data, 0, message.properties ?? {})); // count stays 0
          return {};
        },
      };
    },
  };
  const dlq = new FakeProducer();
  const consumer = new PulsarConsumer(real, failing, {
    client,
    topic: "persistent://public/default/orders",
    subscriptionType: "Exclusive",
    deadLetterProducer: dlq,
    onError: () => undefined,
  });

  let polls = 0;
  while (real.queue.length > 0 && polls < 20) {
    await consumer.poll();
    polls += 1;
  }
  assert.equal(polls, 3);
  assert.equal(real.nacked, 0);
  assert.deepEqual(created, [{ topic: "persistent://public/default/orders" }], "built once, on the given topic");
  assert.equal(republished.length, 2);
  assert.equal(dlqRecord(dlq).dead_letter.attempts, 3);
});

test("a given retryProducer wins over the client; the topic option overrides getTopic", async () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  const client = new ClientOver(topic);
  await new PulsarConsumer(topic, failing, { client, retryProducer: topic, onError: () => undefined }).poll();
  assert.equal(client.created.length, 0);
  assert.equal(topic.sent.length, 1);

  const other = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  const otherClient = new ClientOver(other);
  await new PulsarConsumer(other, failing, { client: otherClient, topic: "orders-x", onError: () => undefined }).poll();
  assert.deepEqual(otherClient.created, [{ topic: "orders-x" }]);
});

test("a failed producer creation warns, nacks, and is tried again on the next retry", async () => {
  const topic = new TopicAwareExclusive(new FakeMessage(bodyOf(0)));
  const client = new ClientOver(topic);
  client.failures = 1;
  const warnings: string[] = [];
  const consumer = new PulsarConsumer(topic, failing, { client, onError: () => undefined, onWarn: (w) => warnings.push(w) });

  await consumer.poll();
  assert.equal(topic.nacked, 1);
  assert.match(warnings[0] as string, /creating the retry producer on .*orders failed \(no producer\)/);

  await consumer.poll();
  assert.equal(topic.sent.length, 1);
  assert.equal(topic.acked, 1);
});

test("Exclusive / Failover without a retry producer or client is rejected at construction", () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  for (const subscriptionType of ["Exclusive", "Failover"] as const) {
    assert.throws(
      () => new PulsarConsumer(topic, failing, { subscriptionType }),
      /needs a retryProducer \(or a client plus a topic\)/,
      subscriptionType,
    );
  }
  // a client alone is not enough when no topic can be resolved (this subscription has no getTopic)
  const noTopic = new RedeliveringConsumer(new FakeMessage(bodyOf(0)));
  assert.throws(
    () => new PulsarConsumer(noTopic, failing, { subscriptionType: "Exclusive", client: new ClientOver(topic) }),
    /needs the `topic` option/,
  );
});

test("Exclusive / Failover are accepted with a producer, a client plus topic, or maxAttempts 1", () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  new PulsarConsumer(topic, failing, { subscriptionType: "Exclusive", retryProducer: topic });
  new PulsarConsumer(topic, failing, { subscriptionType: "Failover", client: new ClientOver(topic), topic: "orders" });
  new PulsarConsumer(topic, failing, { subscriptionType: "Exclusive", maxAttempts: 1 });
});

test("Shared / Key_Shared / KeyShared keep the nack path without a producer or client", async () => {
  for (const subscriptionType of ["Shared", "Key_Shared", "KeyShared"] as const) {
    const client = new RedeliveringConsumer(new FakeMessage(bodyOf(0)));
    const consumer = new PulsarConsumer(client, failing, { subscriptionType, onError: () => undefined, onWarn: () => undefined });
    assert.equal(await deliveriesUntilAck(consumer, client), 3, subscriptionType);
    assert.equal(client.nacked, 2);
  }
});

test("an undeclared subscriptionType counts as Exclusive: no retry producer or client is rejected at construction", () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(0)));
  assert.throws(
    () => new PulsarConsumer(client, failing),
    /An undeclared subscriptionType \(treated as Exclusive\) needs a retryProducer.*subscriptionType: "Shared"/s,
  );
  assert.throws(() => new PulsarConsumer(client, failing, { deadLetterProducer: new FakeProducer() }), /undeclared subscriptionType/);
});

test("an undeclared subscriptionType is accepted with a retryProducer, a client plus topic, or maxAttempts 1", () => {
  const topic = new ExclusiveTopic(new FakeMessage(bodyOf(0)));
  new PulsarConsumer(topic, failing, { retryProducer: topic });
  new PulsarConsumer(topic, failing, { client: new ClientOver(topic), topic: "orders" });
  new PulsarConsumer(topic, failing, { maxAttempts: 1 });
});

test("an unknown subscriptionType is rejected", () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(0)));
  assert.throws(() => new PulsarConsumer(client, {}, { subscriptionType: "Nope" as never }), /Unknown subscriptionType "Nope"/);
});
