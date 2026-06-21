import assert from "node:assert/strict";
import { test } from "node:test";

import { EnvelopeCodec } from "@babelqueue/core";
import type { Envelope, HeaderCarrier } from "@babelqueue/core";

import {
  ArtemisConsumer,
  ArtemisPublisher,
  artemisMessage,
  headersOf,
  type AmqpDelivery,
  type AmqpEventContext,
  type AmqpMessage,
  type AmqpSender,
  type BabelHandlers,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

function envelope(): Envelope {
  return EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" });
}

class FakeSender implements AmqpSender {
  sent: AmqpMessage[] = [];
  send(message: AmqpMessage): unknown {
    this.sent.push(message);
    return {};
  }
}

class FakeDelivery implements AmqpDelivery {
  accepted = 0;
  released: unknown[] = [];
  accept(): void {
    this.accepted += 1;
  }
  release(): void {
    this.released.push({});
  }
}

// --- inject: merge beside the contract bq_ (contract wins) --------------------

test("artemisMessage merges a traceparent into application_properties beside bq_", () => {
  const message = artemisMessage(envelope(), undefined, { traceparent: TRACEPARENT, tracestate: "v=1" });
  const props = message.application_properties!;
  assert.equal(props["traceparent"], TRACEPARENT);
  assert.equal(props["tracestate"], "v=1");
  assert.equal(props["bq_app_id"], "babelqueue");
});

test("a contract bq_ property wins a key collision (merge-not-clobber)", () => {
  const message = artemisMessage(envelope(), undefined, { "bq_app_id": "HIJACK", traceparent: TRACEPARENT } as HeaderCarrier);
  assert.equal(message.application_properties!["bq_app_id"], "babelqueue"); // not "HIJACK"
  assert.equal(message.application_properties!["traceparent"], TRACEPARENT);
});

test("a header-less publish carries no rider keys", async () => {
  const sender = new FakeSender();
  await ArtemisPublisher.create(sender, "orders").publish(URN, { order_id: 7 });
  assert.ok(!("traceparent" in sender.sent[0]!.application_properties!));
});

test("publish with a traceparent puts it on application_properties", async () => {
  const sender = new FakeSender();
  await ArtemisPublisher.create(sender, "orders").publish(URN, { order_id: 7 }, { headers: { traceparent: TRACEPARENT } });
  assert.equal(sender.sent[0]!.application_properties!["traceparent"], TRACEPARENT);
});

// --- extract: headersOf -------------------------------------------------------

test("headersOf reads application_properties back into a Record<string,string>", () => {
  const message = artemisMessage(envelope(), undefined, { traceparent: TRACEPARENT });
  const headers = headersOf(message);
  assert.equal(headers["traceparent"], TRACEPARENT);
  assert.equal(headers["bq_app_id"], "babelqueue");
  assert.deepEqual(headersOf({}), {});
});

// --- end-to-end through the fakes ---------------------------------------------

test("the consumer surfaces the carried traceparent to the handler", async () => {
  const env = envelope();
  const message = artemisMessage(env, undefined, { traceparent: TRACEPARENT });
  const delivery = new FakeDelivery();
  const context: AmqpEventContext = { message, delivery };
  let seen: HeaderCarrier | null = null;
  const handlers: BabelHandlers = { [URN]: (_e, _m, headers) => { seen = headers; } };

  await new ArtemisConsumer(handlers).handle(context);

  assert.equal(seen!["traceparent"], TRACEPARENT);
  assert.equal(delivery.accepted, 1);
});
