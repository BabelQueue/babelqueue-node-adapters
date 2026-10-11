import assert from "node:assert/strict";
import { mock, test } from "node:test";

import { EnvelopeCodec, UnknownUrnStrategy, annotate, redrive } from "@babelqueue/core";
import type { Envelope } from "@babelqueue/core";

import {
  KafkaConsumer,
  KafkaPublisher,
  KafkaRedriveIO,
  RetryTopics,
  kafkaHeaders,
  type BabelHandlers,
  type EachMessagePayload,
  type IncomingHeaders,
  type KafkaConsumerClient,
  type KafkaProducerClient,
  type KafkaProducerMessage,
  type KafkaRedriveMessage,
} from "../src/index.js";

const URN = "urn:babel:orders:created";

function envelope(attempts = 0): Envelope {
  return { ...EnvelopeCodec.make(URN, { order_id: 7 }, { queue: "orders", traceId: "trace-1" }), attempts };
}

function payloadFor(env: Envelope, offset = "10", topic = "orders"): EachMessagePayload {
  const headers: IncomingHeaders = {};
  for (const [k, v] of Object.entries(kafkaHeaders(env))) headers[k] = Buffer.from(v, "utf8");
  return {
    topic,
    partition: 0,
    message: {
      value: Buffer.from(EnvelopeCodec.encode(env), "utf8"),
      headers,
      offset,
      timestamp: String(env.meta.created_at),
    },
  };
}

class FakeProducer implements KafkaProducerClient {
  sent: { topic: string; messages: KafkaProducerMessage[] }[] = [];
  async send(record: { topic: string; messages: KafkaProducerMessage[] }): Promise<unknown> {
    this.sent.push(record);
    return {};
  }
}

class FakeConsumer implements KafkaConsumerClient {
  committed: { topic: string; partition: number; offset: string }[] = [];
  eachMessage?: (payload: EachMessagePayload) => Promise<void>;
  async run(config: { autoCommit?: boolean; eachMessage: (p: EachMessagePayload) => Promise<void> }): Promise<void> {
    this.eachMessage = config.eachMessage;
  }
  async commitOffsets(offsets: { topic: string; partition: number; offset: string }[]): Promise<void> {
    this.committed.push(...offsets);
  }
}

function topology(): RetryTopics {
  return new RetryTopics("orders", [5000, 60000]);
}

// --- projection ---------------------------------------------------------------

test("kafkaHeaders projects the contract fields as strings", () => {
  const env = envelope();
  const h = kafkaHeaders(env);
  assert.equal(h["bq-job"], URN);
  assert.equal(h["bq-trace-id"], env.trace_id);
  assert.equal(h["bq-message-id"], env.meta.id);
  assert.equal(h["bq-schema-version"], "1");
  assert.equal(h["bq-source-lang"], env.meta.lang);
  assert.equal(h["bq-attempts"], "0");
  assert.ok(Object.values(h).every((v) => typeof v === "string"));
});

// --- RetryTopics --------------------------------------------------------------

test("RetryTopics names tiers ascending and maps delay/attempt", () => {
  const rt = new RetryTopics("orders", [60000, 5000]);
  assert.equal(rt.tiers[0]!.topic, "orders.retry.1");
  assert.equal(rt.tiers[0]!.delayMs, 5000);
  assert.equal(rt.tiers[1]!.topic, "orders.retry.2");
  assert.equal(rt.dlqTopic, "orders.dlq");
  assert.equal(rt.tierForDelay(3000).topic, "orders.retry.1");
  assert.equal(rt.tierForDelay(10000).topic, "orders.retry.2");
  assert.equal(rt.tierForAttempt(0).topic, "orders.retry.1");
  assert.equal(rt.tierForAttempt(9).topic, "orders.retry.2");
});

test("RetryTopics raises on too-large delay or no tiers", () => {
  assert.throws(() => new RetryTopics("orders", [5000]).tierForDelay(60000));
  assert.throws(() => new RetryTopics("orders", []).tierForAttempt(0));
  assert.equal(new RetryTopics("orders", [], null).dlqTopic, null);
});

// --- publisher ----------------------------------------------------------------

test("publish projects value, headers and timestamp", async () => {
  const producer = new FakeProducer();
  const id = await KafkaPublisher.create(producer, "orders").publish(URN, { order_id: 7 }, { traceId: "trace-1" });
  assert.equal(producer.sent.length, 1);
  const msg = producer.sent[0]!.messages[0]!;
  assert.equal(producer.sent[0]!.topic, "orders");
  assert.equal(msg.headers!["bq-job"], URN);
  assert.equal(msg.headers!["bq-message-id"], id);
  assert.equal(msg.headers!["bq-attempts"], "0");
  const decoded = EnvelopeCodec.decode(String(msg.value));
  assert.equal(EnvelopeCodec.urn(decoded), URN);
  assert.equal(msg.timestamp, String((decoded as Envelope).meta.created_at));
});

test("publish delay without retry topics raises", async () => {
  const producer = new FakeProducer();
  await assert.rejects(
    KafkaPublisher.create(producer, "orders").publish(URN, {}, { delayMs: 30000 }),
    /retry topics/,
  );
});

test("publish delay routes to the smallest sufficient retry tier", async () => {
  const producer = new FakeProducer();
  await KafkaPublisher.withRetryTopics(producer, topology()).publish(URN, {}, { delayMs: 30000 });
  const msg = producer.sent[0]!;
  assert.equal(msg.topic, "orders.retry.2");
  assert.equal(msg.messages[0]!.headers!["bq-delay"], "30000");
  assert.equal(msg.messages[0]!.headers!["bq-original-topic"], "orders");
});

// --- consumer -----------------------------------------------------------------

test("success processes then commits", async () => {
  const consumer = new FakeConsumer();
  let seen = -1;
  const handlers: BabelHandlers = { [URN]: (env) => void (seen = env.attempts) };
  await new KafkaConsumer(consumer, handlers).handle(payloadFor(envelope(0), "41"));
  assert.equal(seen, 0);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "42" }]);
});

test("attempts header is authoritative", async () => {
  const consumer = new FakeConsumer();
  let seen = -1;
  await new KafkaConsumer(consumer, { [URN]: (env) => void (seen = env.attempts) }).handle(payloadFor(envelope(2)));
  assert.equal(seen, 2);
});

test("throwing handler republishes to retry with attempts+1", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  let reported: unknown = null;
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await new KafkaConsumer(consumer, handlers, {
    producer,
    retryTopics: topology(),
    maxTries: 3,
    onError: (e) => void (reported = e),
  }).handle(payloadFor(envelope(0), "10"));

  assert.ok(reported instanceof Error);
  const retry = producer.sent[0]!;
  assert.equal(retry.topic, "orders.retry.1");
  assert.equal(retry.messages[0]!.headers!["bq-attempts"], "1");
  assert.equal(retry.messages[0]!.headers!["bq-delay"], "5000");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "11" }]);
});

test("terminal failure goes to the DLQ with a dead_letter block", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await new KafkaConsumer(consumer, handlers, { producer, retryTopics: topology(), maxTries: 3 }).handle(
    payloadFor(envelope(2), "7"),
  );
  const dlq = producer.sent[0]!;
  assert.equal(dlq.topic, "orders.dlq");
  const dead = EnvelopeCodec.decode(String(dlq.messages[0]!.value)) as Envelope;
  assert.equal(dead.dead_letter!.reason, "failed");
});

test("retry without topics or DLQ raises", async () => {
  const consumer = new FakeConsumer();
  const handlers: BabelHandlers = {
    [URN]: () => {
      throw new Error("boom");
    },
  };
  await assert.rejects(new KafkaConsumer(consumer, handlers).handle(payloadFor(envelope(0))));
});

test("unknown URN fail throws and does not commit", async () => {
  const consumer = new FakeConsumer();
  await assert.rejects(new KafkaConsumer(consumer, {}).handle(payloadFor(envelope(0))));
  assert.equal(consumer.committed.length, 0);
});

test("unknown URN delete commits", async () => {
  const consumer = new FakeConsumer();
  await new KafkaConsumer(consumer, {}, { unknownUrn: UnknownUrnStrategy.DELETE }).handle(payloadFor(envelope(0), "8"));
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "9" }]);
});

test("unknown URN dead_letter goes to the DLQ", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  await new KafkaConsumer(consumer, {}, {
    producer,
    retryTopics: topology(),
    unknownUrn: UnknownUrnStrategy.DEAD_LETTER,
  }).handle(payloadFor(envelope(0)));
  const dlq = producer.sent[0]!;
  assert.equal(dlq.topic, "orders.dlq");
  const dead = EnvelopeCodec.decode(String(dlq.messages[0]!.value)) as Envelope;
  assert.equal(dead.dead_letter!.reason, "unknown_urn");
});

test("unknown URN release republishes to retry", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  await new KafkaConsumer(consumer, {}, {
    producer,
    retryTopics: topology(),
    unknownUrn: UnknownUrnStrategy.RELEASE,
  }).handle(payloadFor(envelope(0)));
  assert.equal(producer.sent[0]!.topic, "orders.retry.1");
});

test("poison body is forwarded raw to the DLQ", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  let reported: unknown = null;
  const payload: EachMessagePayload = {
    topic: "orders",
    partition: 0,
    message: { value: Buffer.from("not-json", "utf8"), headers: {}, offset: "4" },
  };
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology(), onError: (e) => void (reported = e) }).handle(
    payload,
  );
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "5" }]);
  assert.ok(reported);
});

test("run wires a manual-commit eachMessage loop", async () => {
  const consumer = new FakeConsumer();
  let seen = -1;
  await new KafkaConsumer(consumer, { [URN]: (env) => void (seen = env.attempts) }).run();
  assert.ok(consumer.eachMessage, "run should register eachMessage");
  await consumer.eachMessage!(payloadFor(envelope(0)));
  assert.equal(seen, 0);
});

function richPayload(value: Buffer | null): EachMessagePayload {
  return {
    topic: "orders",
    partition: 2,
    message: {
      value,
      key: Buffer.from("order-7", "utf8"),
      headers: {
        "x-tenant": Buffer.from("acme", "utf8"),
        "x-str": "plain",
        "x-multi": [Buffer.from("a", "utf8"), "b"],
        "x-undef": undefined,
        "bq-original-topic": Buffer.from("orders", "utf8"),
      },
      offset: "20",
    },
  };
}

function assertForwardedWithMetadata(producer: FakeProducer, payload: EachMessagePayload): void {
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  const sent = producer.sent[0]!.messages[0]!;
  assert.deepEqual(sent.value, payload.message.value ?? Buffer.alloc(0));
  assert.deepEqual(sent.key, Buffer.from("order-7", "utf8"));
  assert.deepEqual(sent.headers, {
    "x-tenant": Buffer.from("acme", "utf8"),
    "x-str": Buffer.from("plain", "utf8"),
    "x-multi": [Buffer.from("a", "utf8"), Buffer.from("b", "utf8")],
    "bq-original-topic": Buffer.from("orders", "utf8"),
  });
  // Buffers are copies, not the inbound instances.
  assert.notEqual(sent.headers!["x-tenant"], payload.message.headers!["x-tenant"]);
  assert.notEqual(sent.key, payload.message.key);
}

test("raw DLQ (decode error): the original key and headers are forwarded, headers as Buffer copies", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload = richPayload(Buffer.from("not-json", "utf8"));
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology() }).handle(payload);
  assertForwardedWithMetadata(producer, payload);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 2, offset: "21" }]);
});

test("raw DLQ (non-conformant envelope): the original key and headers are forwarded", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload = richPayload(Buffer.from('{"foo":"bar"}', "utf8"));
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology() }).handle(payload);
  assertForwardedWithMetadata(producer, payload);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 2, offset: "21" }]);
});

test("raw DLQ (version gate): the original key and headers are forwarded", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload = richPayload(payloadFor(envelope(0)).message.value);
  payload.message.headers!["bq-schema-version"] = Buffer.from("2", "utf8");
  await new KafkaConsumer(consumer, { [URN]: () => undefined }, { producer, retryTopics: topology() }).handle(payload);
  const sent = producer.sent[0]!.messages[0]!;
  assert.deepEqual(sent.key, Buffer.from("order-7", "utf8"));
  assert.deepEqual(sent.headers!["x-multi"], [Buffer.from("a", "utf8"), Buffer.from("b", "utf8")]);
  assert.deepEqual(sent.headers!["bq-schema-version"], Buffer.from("2", "utf8"));
  assert.deepEqual(sent.value, payload.message.value);
});

test("raw DLQ: a record without a key or headers forwards no key and an empty header set", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload: EachMessagePayload = {
    topic: "orders",
    partition: 0,
    message: { value: Buffer.from("not-json", "utf8"), offset: "1", key: null },
  };
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology() }).handle(payload);
  const sent = producer.sent[0]!.messages[0]!;
  assert.equal("key" in sent, false);
  assert.deepEqual(sent.headers, {});
});

test("raw DLQ: a null-valued header is dropped, the forward succeeds and the offset is committed", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload: EachMessagePayload = {
    topic: "orders",
    partition: 1,
    message: {
      value: Buffer.from("not-json", "utf8"),
      offset: "9",
      // A null array element is not in the declared header type; the cast keeps the runtime case covered.
      headers: {
        foo: null,
        "x-keep": Buffer.from("v", "utf8"),
        "x-arr": [null, Buffer.from("a", "utf8")] as unknown as Buffer[],
      },
    },
  };
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology() }).handle(payload);
  assert.equal(producer.sent.length, 1);
  const sent = producer.sent[0]!.messages[0]!;
  assert.equal("foo" in sent.headers!, false);
  assert.deepEqual(sent.headers!["x-keep"], Buffer.from("v", "utf8"));
  assert.deepEqual(sent.headers!["x-arr"], [Buffer.from("a", "utf8")]);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 1, offset: "10" }]);
});

// --- schema-version gate (contract §6.7) ---------------------------------------

function gatePayload(version: string | Buffer | null | undefined, body = payloadFor(envelope(0))): EachMessagePayload {
  const headers: IncomingHeaders = { ...body.message.headers };
  delete headers["bq-schema-version"];
  if (version !== undefined) headers["bq-schema-version"] = version;
  return { ...body, message: { ...body.message, headers } };
}

test("gate: a missing or blank bq-schema-version header follows the normal decode path", async () => {
  for (const version of [undefined, "", " \t", "\n\u000b\f\r", Buffer.from("  ", "utf8")]) {
    const consumer = new FakeConsumer();
    let calls = 0;
    await new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }).handle(gatePayload(version));
    assert.equal(calls, 1, JSON.stringify(version));
    assert.equal(consumer.committed.length, 1);
  }
});

test("gate: a null bq-schema-version header value is treated as absent and decodes", async () => {
  const consumer = new FakeConsumer();
  let calls = 0;
  const errors: unknown[] = [];
  await new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }, { onError: (e) => void errors.push(e) }).handle(
    gatePayload(null),
  );
  assert.equal(calls, 1);
  assert.equal(errors.length, 0);
  assert.equal(consumer.committed.length, 1);
});

test("gate: \"1\" (Buffer or string) is decoded and the handler runs", async () => {
  for (const version of ["1", Buffer.from("1", "utf8")]) {
    const consumer = new FakeConsumer();
    let calls = 0;
    await new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }).handle(gatePayload(version));
    assert.equal(calls, 1);
  }
});

test("gate: an unsupported version is dead-lettered raw as poison and committed without decoding", async () => {
  for (const version of ["2", "x", "01", " 1", "\u00a0", Buffer.from("2", "utf8")]) {
    const consumer = new FakeConsumer();
    const producer = new FakeProducer();
    let calls = 0;
    const errors: unknown[] = [];
    const payload = gatePayload(version);
    await new KafkaConsumer(
      consumer,
      { [URN]: () => void (calls += 1) },
      { producer, retryTopics: topology(), onError: (e, env) => void errors.push([e, env]) },
    ).handle(payload);

    assert.equal(calls, 0, JSON.stringify(version));
    assert.equal(producer.sent.length, 1);
    assert.equal(producer.sent[0]!.topic, "orders.dlq");
    assert.deepEqual(producer.sent[0]!.messages[0]!.value, payload.message.value);
    // Literal expectation (not derived from the implementation): every inbound header is a Buffer
    // except the version under test, which is forwarded as its Buffer-encoded value.
    assert.deepEqual(producer.sent[0]!.messages[0]!.headers, {
      ...payload.message.headers,
      "bq-schema-version": Buffer.from(version),
    });
    assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "11" }]);
    assert.equal(errors.length, 1);
    assert.equal((errors[0] as unknown[])[1], null);
  }
});

test("gate: without a DLQ the rejected record is only reported and committed", async () => {
  const consumer = new FakeConsumer();
  let calls = 0;
  let errors = 0;
  await new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }, { onError: () => void (errors += 1) }).handle(
    gatePayload("2"),
  );
  assert.equal(calls, 0);
  assert.equal(errors, 1);
  assert.equal(consumer.committed.length, 1);
});

test("gate: a failing DLQ produce does not commit and never reaches the handler", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  producer.send = async () => {
    throw new Error("dlq produce failed");
  };
  let calls = 0;
  await assert.rejects(
    new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }, {
      producer,
      retryTopics: topology(),
    }).handle(gatePayload("2")),
    /dlq produce failed/,
  );
  assert.equal(calls, 0);
  assert.deepEqual(consumer.committed, []);
});

test("gate: a DLQ configured without a producer does not commit and never reaches the handler", async () => {
  const consumer = new FakeConsumer();
  let calls = 0;
  await assert.rejects(
    new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }, { retryTopics: topology() }).handle(gatePayload("2")),
  );
  assert.equal(calls, 0);
  assert.deepEqual(consumer.committed, []);
});

// --- record key preservation (retry / release / envelope DLQ / raw DLQ) ---------

const KEY_CASES: { name: string; key: Buffer | null | undefined }[] = [
  { name: "keyed", key: Buffer.from("order-1042", "utf8") },
  { name: "empty key", key: Buffer.alloc(0) },
  { name: "null key", key: null },
  { name: "absent key", key: undefined },
];

function keyedPayload(env: Envelope, key: Buffer | null | undefined): EachMessagePayload {
  const payload = payloadFor(env);
  if (key !== undefined) payload.message.key = key;
  return payload;
}

/** Asserts the single produced record carries `key` as-is (null/absent stays off the record). */
function assertKeyCarried(producer: FakeProducer, topic: string, key: Buffer | null | undefined): void {
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.topic, topic);
  const sent = producer.sent[0]!.messages[0]!;
  if (key === undefined || key === null) {
    assert.ok(sent.key === undefined || sent.key === null, "a null key must not be synthesised");
  } else {
    assert.ok(Buffer.isBuffer(sent.key), "the key must be carried");
    assert.deepEqual(sent.key, key);
    assert.notEqual(sent.key, key, "the key is a copy, not the inbound instance");
  }
}

for (const { name, key } of KEY_CASES) {
  test(`key: a failing handler's retry republish keeps the key (${name})`, async () => {
    const producer = new FakeProducer();
    const handlers: BabelHandlers = {
      [URN]: () => {
        throw new Error("boom");
      },
    };
    await new KafkaConsumer(new FakeConsumer(), handlers, { producer, retryTopics: topology(), maxTries: 3 }).handle(
      keyedPayload(envelope(0), key),
    );
    assertKeyCarried(producer, "orders.retry.1", key);
  });

  test(`key: the envelope DLQ write keeps the key (${name})`, async () => {
    const producer = new FakeProducer();
    const handlers: BabelHandlers = {
      [URN]: () => {
        throw new Error("boom");
      },
    };
    await new KafkaConsumer(new FakeConsumer(), handlers, { producer, retryTopics: topology(), maxTries: 3 }).handle(
      keyedPayload(envelope(2), key),
    );
    assertKeyCarried(producer, "orders.dlq", key);
  });

  test(`key: unknown-URN release keeps the key (${name})`, async () => {
    const producer = new FakeProducer();
    await new KafkaConsumer(new FakeConsumer(), {}, {
      producer,
      retryTopics: topology(),
      unknownUrn: UnknownUrnStrategy.RELEASE,
    }).handle(keyedPayload(envelope(0), key));
    assertKeyCarried(producer, "orders.retry.1", key);
  });

  test(`key: unknown-URN dead_letter keeps the key (${name})`, async () => {
    const producer = new FakeProducer();
    await new KafkaConsumer(new FakeConsumer(), {}, {
      producer,
      retryTopics: topology(),
      unknownUrn: UnknownUrnStrategy.DEAD_LETTER,
    }).handle(keyedPayload(envelope(0), key));
    assertKeyCarried(producer, "orders.dlq", key);
  });

  test(`key: the raw (poison) DLQ write keeps the key (${name})`, async () => {
    const producer = new FakeProducer();
    const payload: EachMessagePayload = {
      topic: "orders",
      partition: 0,
      message: { value: Buffer.from("not-json", "utf8"), headers: {}, offset: "4" },
    };
    if (key !== undefined) payload.message.key = key;
    await new KafkaConsumer(new FakeConsumer(), {}, { producer, retryTopics: topology(), onError: () => undefined }).handle(
      payload,
    );
    assertKeyCarried(producer, "orders.dlq", key);
  });
}

// --- onError isolation: a throwing callback never bypasses settlement ---------------

const throwingOnError = (): never => {
  throw new Error("onError exploded");
};

function rawPayload(value: string, headers: IncomingHeaders = {}): EachMessagePayload {
  return { topic: "orders", partition: 0, message: { value: Buffer.from(value, "utf8"), headers, offset: "4" } };
}

test("onError isolation: a throwing onError on a non-JSON body (rejected by the conformance check) still dead-letters and commits", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload = rawPayload("not-json");
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology(), onError: throwingOnError }).handle(payload);
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(producer.sent[0]!.messages[0]!.value, payload.message.value);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "5" }]);
});

test("onError isolation: a throwing onError when decode itself throws still dead-letters the raw record and commits", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload = rawPayload("{}");
  const decode = mock.method(EnvelopeCodec, "decode", () => {
    throw new Error("decode exploded");
  });
  try {
    await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology(), onError: throwingOnError }).handle(payload);
  } finally {
    decode.mock.restore();
  }
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(producer.sent[0]!.messages[0]!.value, payload.message.value);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "5" }]);
});

test("onError isolation: an async onError that rejects neither crashes the process nor bypasses settlement", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown): void => void unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);
  try {
    await new KafkaConsumer(consumer, {}, {
      producer,
      retryTopics: topology(),
      onError: async () => {
        throw new Error("async onError exploded");
      },
    }).handle(rawPayload('{"foo":"bar"}'));
    // Let any unhandled rejection surface before asserting.
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
  assert.deepEqual(unhandled, []);
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "5" }]);
});

test("onError isolation: a throwing onError on a non-conformant envelope still dead-letters and commits", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  const payload = rawPayload('{"foo":"bar"}');
  await new KafkaConsumer(consumer, {}, { producer, retryTopics: topology(), onError: throwingOnError }).handle(payload);
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(producer.sent[0]!.messages[0]!.value, payload.message.value);
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "5" }]);
});

test("onError isolation: a throwing onError on an unsupported schema version still dead-letters and commits", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  let calls = 0;
  await new KafkaConsumer(consumer, { [URN]: () => void (calls += 1) }, {
    producer,
    retryTopics: topology(),
    onError: throwingOnError,
  }).handle(gatePayload("2"));
  assert.equal(calls, 0);
  assert.equal(producer.sent.length, 1);
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "11" }]);
});

test("onError isolation: a throwing onError on a handler failure still retries and commits", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  await new KafkaConsumer(
    consumer,
    {
      [URN]: () => {
        throw new Error("boom");
      },
    },
    { producer, retryTopics: topology(), maxTries: 3, onError: throwingOnError },
  ).handle(payloadFor(envelope(0), "10"));
  assert.equal(producer.sent[0]!.topic, "orders.retry.1");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "11" }]);
});

test("onError isolation: a throwing onError on a terminal handler failure still dead-letters and commits", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  await new KafkaConsumer(
    consumer,
    {
      [URN]: () => {
        throw new Error("boom");
      },
    },
    { producer, retryTopics: topology(), maxTries: 3, onError: throwingOnError },
  ).handle(payloadFor(envelope(2), "10"));
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(consumer.committed, [{ topic: "orders", partition: 0, offset: "11" }]);
});

test("onError isolation: the consumer keeps running — the next record is processed after a throwing onError", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  let seen = 0;
  const worker = new KafkaConsumer(consumer, { [URN]: () => void (seen += 1) }, {
    producer,
    retryTopics: topology(),
    onError: throwingOnError,
  });
  await worker.handle(rawPayload("not-json"));
  await worker.handle(payloadFor(envelope(0), "6"));
  assert.equal(seen, 1);
  assert.deepEqual(consumer.committed, [
    { topic: "orders", partition: 0, offset: "5" },
    { topic: "orders", partition: 0, offset: "7" },
  ]);
});

test("onError isolation: a failing DLQ write is not committed and the original error is still reported", async () => {
  const consumer = new FakeConsumer();
  const producer = new FakeProducer();
  producer.send = async () => {
    throw new Error("dlq produce failed");
  };
  const errors: unknown[] = [];
  await assert.rejects(
    new KafkaConsumer(consumer, {}, { producer, retryTopics: topology(), onError: (e) => void errors.push(e) }).handle(
      rawPayload("not-json"),
    ),
    /dlq produce failed/,
  );
  assert.deepEqual(consumer.committed, []);
  assert.equal(errors.length, 1);
});

test("onError isolation: unknown-URN FAIL still surfaces UnknownUrnError when onError throws", async () => {
  const consumer = new FakeConsumer();
  await assert.rejects(
    new KafkaConsumer(consumer, {}, { onError: throwingOnError }).handle(payloadFor(envelope(0), "10")),
    (e: Error) => e.name === "UnknownUrnError" || /urn/i.test(e.message),
  );
  assert.deepEqual(consumer.committed, []);
});

// --- DLQ redrive: record key preservation (contract §6.5) ---------------------

function redriveIO(producer: FakeProducer): KafkaRedriveIO {
  return new KafkaRedriveIO({ producer, pop: async () => null });
}

function dlqMessage(key: Buffer | null | undefined): KafkaRedriveMessage {
  return { body: EnvelopeCodec.encode(envelope(0)), key, ack: async () => {} };
}

test("redrive publishFrom carries a non-empty key byte-for-byte", async () => {
  const producer = new FakeProducer();
  const key = Buffer.from([0x00, 0xff, 0x10, 0x7f]);
  const from = dlqMessage(key);
  await redriveIO(producer).publishFrom("orders", from.body, from);
  const sent = producer.sent[0]!;
  assert.equal(sent.topic, "orders");
  assert.ok(Buffer.isBuffer(sent.messages[0]!.key));
  assert.deepEqual(sent.messages[0]!.key, key);
});

test("redrive publishFrom keeps an empty key empty", async () => {
  const producer = new FakeProducer();
  const from = dlqMessage(Buffer.alloc(0));
  await redriveIO(producer).publishFrom("orders", from.body, from);
  const key = producer.sent[0]!.messages[0]!.key;
  assert.ok(Buffer.isBuffer(key) && key.length === 0, "empty key must stay empty, not null");
});

test("redrive publishFrom keeps a null / absent key null (never synthesised)", async () => {
  for (const key of [null, undefined]) {
    const producer = new FakeProducer();
    const from = dlqMessage(key);
    await redriveIO(producer).publishFrom("orders", from.body, from);
    assert.ok(!("key" in producer.sent[0]!.messages[0]!), "no key on the record");
  }
});

test("redrive publishFrom projects the contract headers from the body, and restores an undecodable body verbatim", async () => {
  const producer = new FakeProducer();
  const from = dlqMessage(Buffer.from("k"));
  await redriveIO(producer).publishFrom("orders", from.body, from);
  const msg = producer.sent[0]!.messages[0]!;
  assert.equal(msg.headers!["bq-job"], URN);
  assert.equal(msg.headers!["bq-attempts"], "0");

  await redriveIO(producer).publishFrom("orders.dlq", "not-json", from);
  const raw = producer.sent[1]!.messages[0]!;
  assert.equal(raw.value, "not-json");
  assert.equal(raw.headers, undefined);
  assert.deepEqual(raw.key, Buffer.from("k"));
});

test("redrive plain publish stays keyless (non-keyed behaviour unchanged)", async () => {
  const producer = new FakeProducer();
  await redriveIO(producer).publish("orders", EnvelopeCodec.encode(envelope(0)));
  assert.ok(!("key" in producer.sent[0]!.messages[0]!));
});

// --- DLQ redrive: restore writes the consumed record back verbatim -------------

/** A DLQ record as consumed: odd value bytes, custom + stale headers, a key, an old `meta.created_at`. */
function consumedDlqRecord(body: string, value: Buffer | null, ack: () => Promise<void> = async () => {}): KafkaRedriveMessage {
  return {
    body,
    value,
    key: Buffer.from([0x00, 0xff, 0x10]),
    headers: {
      "bq-job": Buffer.from("urn:babel:stale:job"),
      "bq-original-topic": Buffer.from("orders"),
      "x-custom": [Buffer.from("a"), Buffer.from([0xfe, 0xff])],
    },
    topic: "orders.dlq",
    partition: 0,
    offset: "5",
    ack,
  };
}

test("redrive restore re-writes value bytes, headers and key verbatim with a fresh timestamp", async () => {
  const producer = new FakeProducer();
  const env = envelope(2);
  env.meta.created_at = 1_000;
  // Same JSON, different bytes than the re-encoded body: trailing newline + double space.
  const value = Buffer.from(EnvelopeCodec.encode(env) + "\n  ", "utf8");
  const from = consumedDlqRecord(EnvelopeCodec.encode(env), value);
  const before = Date.now();
  await redriveIO(producer).publishFrom("orders.dlq", from.body, from);
  const sent = producer.sent[0]!;
  assert.equal(sent.topic, "orders.dlq");
  const rec = sent.messages[0]!;
  assert.ok(Buffer.isBuffer(rec.value));
  assert.deepEqual(rec.value, value, "value bytes verbatim");
  assert.deepEqual(rec.key, from.key);
  assert.deepEqual(rec.headers, {
    "bq-job": Buffer.from("urn:babel:stale:job"),
    "bq-original-topic": Buffer.from("orders"),
    "x-custom": [Buffer.from("a"), Buffer.from([0xfe, 0xff])],
  }, "headers verbatim, not re-projected from the body");
  assert.notEqual(rec.timestamp, "1000", "timestamp is not meta.created_at");
  assert.ok(Number(rec.timestamp) >= before && Number(rec.timestamp) <= Date.now(), "timestamp is fresh");
});

test("redrive restore keeps a non-UTF-8 value byte-for-byte", async () => {
  const producer = new FakeProducer();
  const value = Buffer.from([0xff, 0xfe, 0x00, 0x80]);
  const from = consumedDlqRecord(value.toString("utf8"), value);
  await redriveIO(producer).publishFrom("orders.dlq", from.body, from);
  assert.deepEqual(producer.sent[0]!.messages[0]!.value, value);
});

test("redrive restore keeps a null value null and an absent key off the record", async () => {
  const producer = new FakeProducer();
  const from = { ...consumedDlqRecord("", null), key: null };
  await redriveIO(producer).publishFrom("orders.dlq", from.body, from);
  const rec = producer.sent[0]!.messages[0]!;
  assert.equal(rec.value, null);
  assert.ok(!("key" in rec));
});

test("redrive republish to the work topic still projects headers and mirrors meta.created_at", async () => {
  const producer = new FakeProducer();
  const env = envelope(0);
  env.meta.created_at = 1_000;
  const from = consumedDlqRecord(EnvelopeCodec.encode(env), Buffer.from(EnvelopeCodec.encode(env)));
  await redriveIO(producer).publishFrom("orders", EnvelopeCodec.encode(env), from);
  const rec = producer.sent[0]!.messages[0]!;
  assert.equal(rec.headers!["bq-job"], URN);
  assert.equal(rec.timestamp, "1000");
  assert.equal(rec.value, EnvelopeCodec.encode(env));
  assert.deepEqual(rec.key, from.key);
});

test("redrive writes to the DLQ topic with a changed body are a republish, not a restore", async () => {
  const producer = new FakeProducer();
  const env = envelope(0);
  const from = consumedDlqRecord("something else", Buffer.from("something else"));
  env.meta.created_at = 1_000;
  const changed = EnvelopeCodec.encode(env);
  await redriveIO(producer).publishFrom("orders.dlq", changed, from);
  const rec = producer.sent[0]!.messages[0]!;
  assert.equal(rec.headers!["bq-job"], URN, "headers projected from the new body, not copied from the record");
  assert.equal(rec.value, changed, "value is the new body, not from.value");
  assert.equal(rec.timestamp, "1000", "timestamp mirrors meta.created_at (republish)");
  assert.deepEqual(rec.key, from.key);
});

test("core redrive() dry run restores the DLQ record verbatim through the tracked copy", async () => {
  const producer = new FakeProducer();
  const env = envelope(1);
  env.meta.created_at = 1_000;
  const value = Buffer.from(EnvelopeCodec.encode(env) + "\n", "utf8");
  let acked = false;
  let pending: KafkaRedriveMessage | null = consumedDlqRecord(EnvelopeCodec.encode(env), value, async () => {
    acked = true;
  });
  const io = new KafkaRedriveIO({
    producer,
    consumer: { seek: () => {} },
    pop: async () => {
      const m = pending;
      pending = null;
      return m;
    },
  });
  const result = await redrive(io, "orders.dlq", { toQueue: "orders", dryRun: true });
  assert.equal(result.skipped, 1);
  assert.equal(acked, true);
  const rec = producer.sent[0]!.messages[0]!;
  assert.equal(producer.sent[0]!.topic, "orders.dlq");
  assert.deepEqual(rec.value, value);
  assert.equal((rec.headers as unknown as Record<string, Buffer>)["bq-job"].toString(), "urn:babel:stale:job");
  assert.notEqual(rec.timestamp, "1000");
});

test("redrive pop delegates to the caller-supplied reserve", async () => {
  const producer = new FakeProducer();
  const msg = dlqMessage(Buffer.from("k"));
  const io = new KafkaRedriveIO({ producer, pop: async (q) => (q === "orders.dlq" ? msg : null) });
  assert.equal(await io.pop("orders.dlq"), msg);
  assert.equal(await io.pop("other"), null);
});

test("core redrive() over KafkaRedriveIO keeps the DLQ record key end to end", async () => {
  for (const key of [Buffer.from("order-42"), Buffer.alloc(0), null]) {
    const producer = new FakeProducer();
    let pending: KafkaRedriveMessage | null = dlqMessage(key);
    const io = new KafkaRedriveIO({
      producer,
      pop: async () => {
        const m = pending;
        pending = null;
        return m;
      },
    });
    const result = await redrive(io, "orders.dlq", { toQueue: "orders" });
    assert.equal(result.redriven, 1);
    const sent = producer.sent[0]!;
    assert.equal(sent.topic, "orders");
    if (key === null) {
      assert.ok(!("key" in sent.messages[0]!), "null key must stay null");
    } else {
      assert.deepEqual(sent.messages[0]!.key, key);
    }
  }
});

// --- DLQ redrive: rewind of a record whose restore write failed ----------------

class FakeSeeker {
  seeks: { topic: string; partition: number; offset: string }[] = [];
  failure?: Error;
  seek(position: { topic: string; partition: number; offset: string }): void {
    this.seeks.push(position);
    if (this.failure) throw this.failure;
  }
}

/** A producer whose every send is refused, so the restore write back to the DLQ fails. */
const refusingProducer: KafkaProducerClient = {
  async send(): Promise<unknown> {
    throw new Error("broker refused the write");
  },
};

function positioned(offset: string, partition = 2, body = "not an envelope"): KafkaRedriveMessage & { acked: boolean } {
  const m = {
    body,
    key: Buffer.from("k"),
    topic: "orders.dlq",
    partition,
    offset,
    acked: false,
    async ack(): Promise<void> {
      m.acked = true;
    },
  };
  return m;
}

test("KafkaRedriveIO offers rewind only when a consumer is supplied", () => {
  assert.equal(new KafkaRedriveIO({ producer: new FakeProducer(), pop: async () => null }).rewind, undefined);
  const io = new KafkaRedriveIO({ producer: new FakeProducer(), pop: async () => null, consumer: new FakeSeeker() });
  assert.equal(typeof io.rewind, "function");
});

test("KafkaRedriveIO rewind seeks the consumer to the record's own offset", async () => {
  const seeker = new FakeSeeker();
  const io = new KafkaRedriveIO({ producer: new FakeProducer(), pop: async () => null, consumer: seeker });
  await io.rewind!(positioned("41"));
  assert.deepEqual(seeker.seeks, [{ topic: "orders.dlq", partition: 2, offset: "41" }]);
});

test("KafkaRedriveIO rewind refuses a record that carries no position", async () => {
  const seeker = new FakeSeeker();
  const io = new KafkaRedriveIO({ producer: new FakeProducer(), pop: async () => null, consumer: seeker });
  await assert.rejects(io.rewind!(dlqMessage(Buffer.from("k"))), /cannot rewind a record without topic, partition and offset/);
  assert.deepEqual(seeker.seeks, []);
});

/** An IO whose `pop` hands out `messages` in order, then `null`. */
function queuedIO(messages: KafkaRedriveMessage[], seeker: FakeSeeker, producer: KafkaProducerClient = new FakeProducer()) {
  const queue = [...messages];
  return new KafkaRedriveIO({ producer, consumer: seeker, pop: async () => queue.shift() ?? null });
}

const sortedSeeks = (seeker: FakeSeeker) =>
  [...seeker.seeks].sort((a, b) => a.partition - b.partition || Number(BigInt(a.offset) - BigInt(b.offset)));

test("KafkaRedriveIO rewind seeks every partition back to its first unacknowledged record", async () => {
  const seeker = new FakeSeeker();
  const batch = [positioned("41", 0), positioned("42", 0), positioned("9", 1)];
  const io = queuedIO(batch, seeker);
  // The whole batch is popped before it is processed, so the consumer is already past all three records.
  const popped: KafkaRedriveMessage[] = [];
  for (let m = await io.pop("orders.dlq"); m; m = await io.pop("orders.dlq")) popped.push(m);
  await io.rewind!(popped[0]!);
  assert.deepEqual(sortedSeeks(seeker), [
    { topic: "orders.dlq", partition: 0, offset: "41" },
    { topic: "orders.dlq", partition: 1, offset: "9" },
  ]);
});

test("KafkaRedriveIO rewind leaves already acknowledged records alone", async () => {
  const seeker = new FakeSeeker();
  const batch = [positioned("41", 0), positioned("42", 0), positioned("9", 1)];
  const io = queuedIO(batch, seeker);
  const popped: KafkaRedriveMessage[] = [];
  for (let m = await io.pop("orders.dlq"); m; m = await io.pop("orders.dlq")) popped.push(m);
  await popped[0]!.ack();
  await io.rewind!(popped[1]!);
  assert.deepEqual(sortedSeeks(seeker), [
    { topic: "orders.dlq", partition: 0, offset: "42" },
    { topic: "orders.dlq", partition: 1, offset: "9" },
  ]);
});

test("KafkaRedriveIO rewind skips an acknowledged `from` and rewinds the rest of the batch", async () => {
  const seeker = new FakeSeeker();
  const io = queuedIO([positioned("41", 0), positioned("42", 0), positioned("9", 1)], seeker);
  const popped: KafkaRedriveMessage[] = [];
  for (let m = await io.pop("orders.dlq"); m; m = await io.pop("orders.dlq")) popped.push(m);
  // Restored and acknowledged after a failed redrive write: committed past, so it must not come back.
  await popped[0]!.ack();
  await io.rewind!(popped[0]!);
  assert.deepEqual(sortedSeeks(seeker), [
    { topic: "orders.dlq", partition: 0, offset: "42" },
    { topic: "orders.dlq", partition: 1, offset: "9" },
  ]);
});

test("KafkaRedriveIO rewind of an acknowledged sole record seeks nothing", async () => {
  const seeker = new FakeSeeker();
  const io = queuedIO([positioned("41", 0)], seeker);
  const only = await io.pop("orders.dlq");
  await only!.ack();
  await io.rewind!(only!);
  assert.deepEqual(seeker.seeks, [], "seeking an acknowledged record back would redeliver a duplicate");
});

test("KafkaRedriveIO pop leaves the caller's message untouched", async () => {
  const seeker = new FakeSeeker();
  let acked = false;
  const ackFn = async (): Promise<void> => {
    acked = true;
  };
  // Frozen: assigning `ack` on it would throw in strict mode, so pop must not mutate it.
  const frozen = Object.freeze({ body: "not an envelope", topic: "orders.dlq", partition: 0, offset: "41", ack: ackFn });
  const io = queuedIO([frozen], seeker);
  const handedOut = await io.pop("orders.dlq");
  assert.notEqual(handedOut, frozen, "a tracked copy is handed out");
  assert.equal(frozen.ack, ackFn, "the caller's own ack is not replaced");
  assert.equal(handedOut!.offset, "41");
  await handedOut!.ack();
  assert.equal(acked, true, "acknowledging the copy acknowledges the original");
  await io.rewind!(handedOut!);
  assert.deepEqual(seeker.seeks, []);
});

test("KafkaRedriveIO pop keeps the fields of a message that exposes them as prototype getters", async () => {
  const seeker = new FakeSeeker();
  class GetterMessage implements KafkaRedriveMessage {
    get body(): string {
      return "not an envelope";
    }
    get key(): Buffer {
      return Buffer.from("order-7");
    }
    get topic(): string {
      return "orders.dlq";
    }
    get partition(): number {
      return 2;
    }
    get offset(): string {
      return "41";
    }
    async ack(): Promise<void> {}
  }
  const io = queuedIO([new GetterMessage()], seeker);
  const handedOut = await io.pop("orders.dlq");
  assert.equal(handedOut!.body, "not an envelope", "a spread copy would have lost the body");
  assert.deepEqual(handedOut!.key, Buffer.from("order-7"));
  assert.equal(handedOut!.topic, "orders.dlq");
  assert.equal(handedOut!.partition, 2);
  assert.equal(handedOut!.offset, "41");
  await io.rewind!(handedOut!);
  assert.deepEqual(seeker.seeks, [{ topic: "orders.dlq", partition: 2, offset: "41" }]);
});

test("KafkaRedriveIO rewind compares offsets numerically, not as strings", async () => {
  const seeker = new FakeSeeker();
  const io = queuedIO([positioned("9"), positioned("10")], seeker);
  const first = await io.pop("orders.dlq");
  await io.pop("orders.dlq");
  await io.rewind!(first!);
  assert.deepEqual(seeker.seeks, [{ topic: "orders.dlq", partition: 2, offset: "9" }]);
});

test("KafkaRedriveIO rewind keeps seeking the other partitions when one seek fails", async () => {
  const seeker = new FakeSeeker();
  const failing = new Error("seek failed");
  const origSeek = seeker.seek.bind(seeker);
  seeker.seek = (position) => {
    origSeek(position);
    if (position.partition === 0) throw failing;
  };
  const io = queuedIO([positioned("41", 0), positioned("9", 1)], seeker);
  const first = await io.pop("orders.dlq");
  await io.pop("orders.dlq");
  await assert.rejects(io.rewind!(first!), (err: unknown) => err === failing);
  assert.equal(seeker.seeks.length, 2);
});

/** True when the installed `@babelqueue/core` calls `RedriveIO.rewind` (added after core 1.9.0). */
async function coreSupportsRewind(): Promise<boolean> {
  let called = false;
  let popped = false;
  const probe = {
    async pop(): Promise<{ body: string; ack(): Promise<void> } | null> {
      if (popped) return null;
      popped = true;
      return { body: "x", ack: async () => {} };
    },
    async publish(): Promise<void> {
      throw new Error("refused");
    },
    async rewind(): Promise<void> {
      called = true;
    },
  };
  await redrive(probe, "q").catch(() => undefined);
  return called;
}

test("core redrive(): a failed restore seeks the consumer back and never acks", async (t) => {
  if (!(await coreSupportsRewind())) {
    t.skip("the installed @babelqueue/core predates RedriveIO.rewind");
    return;
  }
  const seeker = new FakeSeeker();
  const msg = positioned("41");
  let pending: KafkaRedriveMessage | null = msg;
  const io = new KafkaRedriveIO({
    producer: refusingProducer,
    consumer: seeker,
    pop: async () => {
      const m = pending;
      pending = null;
      return m;
    },
  });
  await assert.rejects(redrive(io, "orders.dlq", { toQueue: "orders" }), /broker refused the write/);
  assert.deepEqual(seeker.seeks, [{ topic: "orders.dlq", partition: 2, offset: "41" }]);
  assert.equal(msg.acked, false, "a record whose restore failed must not be committed");
});

test("core redrive(): a failing seek is reported together with the restore error", async (t) => {
  if (!(await coreSupportsRewind())) {
    t.skip("the installed @babelqueue/core predates RedriveIO.rewind");
    return;
  }
  const seeker = new FakeSeeker();
  seeker.failure = new Error("seek failed");
  const msg = positioned("41");
  let pending: KafkaRedriveMessage | null = msg;
  const io = new KafkaRedriveIO({
    producer: refusingProducer,
    consumer: seeker,
    pop: async () => {
      const m = pending;
      pending = null;
      return m;
    },
  });
  await assert.rejects(redrive(io, "orders.dlq", { toQueue: "orders" }), (err: unknown) => {
    assert.ok(err instanceof AggregateError);
    assert.deepEqual(
      err.errors.map((e: Error) => e.message),
      ["broker refused the write", "seek failed"],
    );
    return true;
  });
  assert.equal(msg.acked, false);
});

test("core redrive(): a failed restore in a multi-partition batch seeks every partition back", async (t) => {
  if (!(await coreSupportsRewind())) {
    t.skip("the installed @babelqueue/core predates RedriveIO.rewind");
    return;
  }
  const seeker = new FakeSeeker();
  const first = positioned("41", 0);
  const second = positioned("9", 1);
  const io = queuedIO([first, second], seeker, refusingProducer);
  await assert.rejects(redrive(io, "orders.dlq", { toQueue: "orders" }), /broker refused the write/);
  assert.deepEqual(sortedSeeks(seeker), [
    { topic: "orders.dlq", partition: 0, offset: "41" },
    { topic: "orders.dlq", partition: 1, offset: "9" },
  ]);
  assert.equal(first.acked, false);
  assert.equal(second.acked, false);
});

test("core redrive(): a failed work write with a successful restore rewinds the rest, not the acknowledged record", async (t) => {
  if (!(await coreSupportsRewind())) {
    t.skip("the installed @babelqueue/core predates RedriveIO.rewind");
    return;
  }
  const seeker = new FakeSeeker();
  const body = EnvelopeCodec.encode(annotate(envelope(0), "failed", "orders"));
  const first = positioned("41", 0, body);
  const second = positioned("9", 1, body);
  // The work topic refuses the write, the DLQ restore goes through.
  const workRefused: KafkaProducerClient = {
    async send(record: { topic: string }): Promise<unknown> {
      if (record.topic !== "orders.dlq") throw new Error("work topic refused the write");
      return {};
    },
  };
  const io = queuedIO([first, second], seeker, workRefused);
  await assert.rejects(redrive(io, "orders.dlq"), /work topic refused the write/);
  assert.equal(first.acked, true, "the restored record was acknowledged (committed)");
  assert.equal(second.acked, false);
  // Only the unprocessed record comes back; seeking partition 0 would redeliver the restored copy.
  assert.deepEqual(seeker.seeks, [{ topic: "orders.dlq", partition: 1, offset: "9" }]);
});
