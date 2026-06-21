import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import {
  PulsarConsumer,
  PulsarPublisher,
  headersOf,
  pulsarProperties,
  toPulsarMessage,
  type BabelHandlers,
  type PulsarConsumerClient,
  type PulsarProducer,
  type PulsarProducerMessage,
  type PulsarReceivedMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(): Envelope {
  return EnvelopeCodec.make(URN, { order_id: 1042 }, { queue: "orders", traceId: "trace-1" });
}

class FakeProducer implements PulsarProducer {
  sent: PulsarProducerMessage[] = [];
  getTopic(): string {
    return "persistent://public/default/orders";
  }
  async send(message: PulsarProducerMessage): Promise<unknown> {
    this.sent.push(message);
    return { messageId: "m-1" };
  }
}

class FakeMessage implements PulsarReceivedMessage {
  constructor(private readonly raw: string, private readonly props: { [key: string]: string } = {}) {}
  getData(): Buffer {
    return Buffer.from(this.raw, "utf8");
  }
  getProperties(): { [key: string]: string } {
    return this.props;
  }
  getRedeliveryCount(): number {
    return 0;
  }
}

class FakeConsumer implements PulsarConsumerClient {
  acked: PulsarReceivedMessage[] = [];
  nacked: PulsarReceivedMessage[] = [];
  private queued: PulsarReceivedMessage[];
  constructor(queued: PulsarReceivedMessage[] = []) {
    this.queued = [...queued];
  }
  async receive(): Promise<PulsarReceivedMessage> {
    const m = this.queued.shift();
    if (!m) throw new Error("receive timeout");
    return m;
  }
  acknowledge(m: PulsarReceivedMessage): void {
    this.acked.push(m);
  }
  negativeAcknowledge(m: PulsarReceivedMessage): void {
    this.nacked.push(m);
  }
}

// --- inject: merge beside the contract bq-* (contract wins) --------------------

test("toPulsarMessage merges a traceparent beside the contract bq-* properties", () => {
  const message = toPulsarMessage(envelope(), { traceparent: TRACEPARENT, tracestate: "v=1" });
  assert.equal(message.properties!["traceparent"], TRACEPARENT);
  assert.equal(message.properties!["tracestate"], "v=1");
  assert.equal(message.properties!["bq-job"], URN);
});

test("a contract bq-* property wins a key collision (merge-not-clobber)", () => {
  const message = toPulsarMessage(envelope(), { "bq-trace-id": "HIJACK", traceparent: TRACEPARENT } as HeaderCarrier);
  assert.equal(message.properties!["bq-trace-id"], "trace-1");
  assert.equal(message.properties!["traceparent"], TRACEPARENT);
});

test("a header-less publish carries only the contract properties", async () => {
  const producer = new FakeProducer();
  await new PulsarPublisher(producer).publish(URN, { order_id: 1042 });
  assert.ok(!("traceparent" in producer.sent[0]!.properties!));
});

test("publish with a traceparent puts it on the message properties", async () => {
  const producer = new FakeProducer();
  await new PulsarPublisher(producer).publish(URN, { order_id: 1042 }, { headers: { traceparent: TRACEPARENT } });
  assert.equal(producer.sent[0]!.properties!["traceparent"], TRACEPARENT);
});

// --- extract: headersOf -------------------------------------------------------

test("headersOf reads getProperties() back into a Record<string,string>", () => {
  const props = { ...pulsarProperties(envelope()), traceparent: TRACEPARENT };
  assert.equal(headersOf(new FakeMessage("{}", props))["traceparent"], TRACEPARENT);
  assert.deepEqual(headersOf(new FakeMessage("{}", {})), {});
});

// --- end-to-end through the fakes ---------------------------------------------

test("the consumer surfaces the carried traceparent to the handler", async () => {
  const env = envelope();
  const message = new FakeMessage(EnvelopeCodec.encode(env), toPulsarMessage(env, { traceparent: TRACEPARENT }).properties);
  const consumer = new FakeConsumer([message]);
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _m, headers) => { seen = headers; } };

  await new PulsarConsumer(consumer, handlers).poll();

  assert.equal(seen!["traceparent"], TRACEPARENT);
  assert.equal(consumer.acked.length, 1);
});
