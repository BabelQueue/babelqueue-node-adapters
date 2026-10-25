import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  DEFAULT_MAX_ATTEMPTS,
  PulsarConsumer,
  type BabelHandlers,
  type PulsarConsumerClient,
  type PulsarProducer,
  type PulsarProducerMessage,
  type PulsarReceivedMessage,
} from "../src/index.js";

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
  const consumer = new PulsarConsumer(client, failing, { deadLetterProducer: producer, onError: (e) => errors.push(e) });

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
  const consumer = new PulsarConsumer(client, failing, { onWarn: (w) => warnings.push(w) });

  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(client.nacked, 2);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0] as string, /dropped a failed/);
});

test("an unmapped URN without a hook is bounded and dead-lettered with reason unknown_urn", async () => {
  const message = new FakeMessage(bodyOf(0));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  const consumer = new PulsarConsumer(client, {}, { deadLetterProducer: producer });

  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  const record = dlqRecord(producer);
  assert.equal(record.dead_letter.reason, "unknown_urn");
  assert.equal(record.dead_letter.attempts, 3);
});

test("maxAttempts 1 means no retry: the first failure is dead-lettered with attempts 1", async () => {
  const message = new FakeMessage(bodyOf(0));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  await new PulsarConsumer(client, failing, { maxAttempts: 1, deadLetterProducer: producer }).poll();
  assert.equal(client.nacked, 0);
  assert.equal(client.acked, 1);
  assert.equal(dlqRecord(producer).dead_letter.attempts, 1);
});

test("an onUnknownUrn hook keeps its behaviour: the hook runs and the message is acknowledged", async () => {
  const message = new FakeMessage(bodyOf(5));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  let hooked = 0;
  await new PulsarConsumer(client, {}, { deadLetterProducer: producer, onUnknownUrn: () => void hooked++ }).poll();
  assert.equal(hooked, 1);
  assert.equal(client.acked, 1);
  assert.equal(producer.sent.length, 0);
});

test("the dead-letter record keeps the redelivered attempts (bq-attempts floored by the redelivery count)", async () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(0), 4, { "bq-attempts": "1" }));
  const producer = new FakeProducer();
  await new PulsarConsumer(client, failing, { deadLetterProducer: producer }).poll();
  assert.equal(dlqRecord(producer).dead_letter.attempts, 5);
});

test("a failed DLQ write leaves the original negatively acknowledged, and the next delivery writes it", async () => {
  const message = new FakeMessage(bodyOf(2));
  const client = new RedeliveringConsumer(message);
  const producer = new FakeProducer();
  producer.failures = 1;
  const warnings: string[] = [];
  const consumer = new PulsarConsumer(client, failing, { deadLetterProducer: producer, onWarn: (w) => warnings.push(w) });

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
    assert.throws(() => new PulsarConsumer(client, {}, { maxAttempts: bad }), /maxAttempts must be an integer of at least 1/);
  }
});

test("the default onWarn writes to console.warn", async () => {
  const client = new RedeliveringConsumer(new FakeMessage(bodyOf(2)));
  const original = console.warn;
  const seen: string[] = [];
  console.warn = (m: unknown) => void seen.push(String(m));
  try {
    await new PulsarConsumer(client, failing).poll();
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
    await new PulsarConsumer(
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
  await new PulsarConsumer(
    new RedeliveringConsumer(new FakeMessage('{"data":{"big":1e400},"attempts":0}')),
    {},
    { deadLetterProducer: spliced },
  ).poll();
  const text = (spliced.sent[0] as PulsarProducerMessage).data.toString("utf8");
  assert.ok(text.startsWith('{"data":{"big":1e400},"attempts":0,"dead_letter":{'), text);

  const wrapped = new FakeProducer();
  await new PulsarConsumer(
    new RedeliveringConsumer(new FakeMessage(Buffer.from([0xff, 0xfe, 0x41]))),
    {},
    { deadLetterProducer: wrapped },
  ).poll();
  const record = dlqRecord(wrapped);
  assert.equal(record["raw"], Buffer.from([0xff, 0xfe, 0x41]).toString("base64"));
  assert.equal(record["raw_encoding"], "base64");

  const text2 = new FakeProducer();
  await new PulsarConsumer(
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
  await new PulsarConsumer(
    new RedeliveringConsumer(new FakeMessage('{"trace_id":"t-1","meta":{"id":"m-1"},"data":{}}')),
    {},
    { deadLetterProducer: producer },
  ).poll();
  assert.deepEqual((producer.sent[0] as PulsarProducerMessage).properties, { "bq-trace-id": "t-1", "bq-message-id": "m-1" });
});

test("the poison queue name is the queue option when given", async () => {
  const producer = new FakeProducer();
  await new PulsarConsumer(
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
  const consumer = new PulsarConsumer(client, failing, { onWarn: (w) => warnings.push(w) });
  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(client.nacked, 2);
  assert.match(warnings[0] as string, /dropped a poison/);
});

test("poison counts attempts from bq-attempts and the redelivery count, not from the body", async () => {
  const client = new RedeliveringConsumer(new FakeMessage('{"attempts":0,"data":{}}', 0, { "bq-attempts": "2" }));
  await new PulsarConsumer(client, {}, { onWarn: () => undefined }).poll();
  assert.equal(client.nacked, 0);
  assert.equal(client.acked, 1, "reconciled 2 + 1 reaches the default cap of 3");
});

test("a failed DLQ write of a poison body leaves it negatively acknowledged", async () => {
  const client = new RedeliveringConsumer(new FakeMessage("not-json"));
  const producer = new FakeProducer();
  producer.failures = 1;
  const consumer = new PulsarConsumer(client, {}, { deadLetterProducer: producer, onWarn: () => undefined });
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
  const consumer = new PulsarConsumer(client, failing, { deadLetterProducer: producer });
  assert.equal(await deliveriesUntilAck(consumer, client), 3);
  assert.equal(dlqRecord(producer).dead_letter.reason, "failed");
});
