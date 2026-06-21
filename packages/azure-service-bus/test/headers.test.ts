import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import {
  AsbConsumer,
  AsbPublisher,
  headersOf,
  toServiceBusMessage,
  type AsbMessage,
  type AsbReceivedMessage,
  type AsbReceiver,
  type AsbSender,
  type BabelHandlers,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(): Envelope {
  return EnvelopeCodec.make(URN, { order_id: 1042 }, { queue: "orders", traceId: "trace-1" });
}

class FakeSender implements AsbSender {
  entityPath = "orders";
  sent: AsbMessage[] = [];
  scheduled: { message: AsbMessage; at: Date }[] = [];
  async sendMessages(message: AsbMessage): Promise<void> {
    this.sent.push(message);
  }
  async scheduleMessages(message: AsbMessage, at: Date): Promise<bigint[]> {
    this.scheduled.push({ message, at });
    return [1n];
  }
}

class FakeReceiver implements AsbReceiver {
  completed: AsbReceivedMessage[] = [];
  abandoned: AsbReceivedMessage[] = [];
  constructor(private readonly messages: AsbReceivedMessage[] = []) {}
  async receiveMessages(max: number): Promise<AsbReceivedMessage[]> {
    return this.messages.splice(0, max);
  }
  async completeMessage(m: AsbReceivedMessage): Promise<void> {
    this.completed.push(m);
  }
  async abandonMessage(m: AsbReceivedMessage): Promise<void> {
    this.abandoned.push(m);
  }
}

// --- inject: merge beside the contract bq-* (contract wins) --------------------

test("toServiceBusMessage merges a traceparent into applicationProperties beside bq-*", () => {
  const message = toServiceBusMessage(envelope(), { traceparent: TRACEPARENT, tracestate: "v=1" });
  assert.equal(message.applicationProperties!["traceparent"], TRACEPARENT);
  assert.equal(message.applicationProperties!["tracestate"], "v=1");
  assert.equal(message.applicationProperties!["bq-schema-version"], 1);
  // subject/correlationId stay the contract projection
  assert.equal(message.subject, URN);
  assert.equal(message.correlationId, "trace-1");
});

test("a contract bq-* property wins a key collision (merge-not-clobber)", () => {
  const message = toServiceBusMessage(envelope(), { "bq-source-lang": "HIJACK", traceparent: TRACEPARENT } as HeaderCarrier);
  assert.equal(message.applicationProperties!["bq-source-lang"], "node"); // not "HIJACK"
  assert.equal(message.applicationProperties!["traceparent"], TRACEPARENT);
});

test("a header-less publish carries no rider keys", async () => {
  const sender = new FakeSender();
  await new AsbPublisher(sender).publish(URN, { order_id: 1042 });
  assert.ok(!("traceparent" in (sender.sent[0]!.applicationProperties ?? {})));
});

test("publish with a traceparent puts it on applicationProperties", async () => {
  const sender = new FakeSender();
  await new AsbPublisher(sender).publish(URN, { order_id: 1042 }, { headers: { traceparent: TRACEPARENT } });
  assert.equal(sender.sent[0]!.applicationProperties!["traceparent"], TRACEPARENT);
});

// --- extract: headersOf -------------------------------------------------------

test("headersOf reads applicationProperties back into a Record<string,string>", () => {
  const message: AsbReceivedMessage = {
    body: "{}",
    applicationProperties: { traceparent: TRACEPARENT, "bq-schema-version": 1, empty: "" },
  };
  const headers = headersOf(message);
  assert.equal(headers["traceparent"], TRACEPARENT);
  assert.equal(headers["bq-schema-version"], "1"); // stringified defensively
  assert.ok(!("empty" in headers));
  assert.deepEqual(headersOf({ body: "{}" }), {});
});

// --- end-to-end through the fakes ---------------------------------------------

test("the consumer surfaces the carried traceparent to the handler", async () => {
  const env = envelope();
  const sent = toServiceBusMessage(env, { traceparent: TRACEPARENT });
  const message: AsbReceivedMessage = {
    body: sent.body,
    deliveryCount: 1,
    applicationProperties: sent.applicationProperties,
  };
  const receiver = new FakeReceiver([message]);
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _m, headers) => { seen = headers; } };

  await new AsbConsumer(receiver, handlers).poll();

  assert.equal(seen!["traceparent"], TRACEPARENT);
  assert.equal(receiver.completed.length, 1);
});
