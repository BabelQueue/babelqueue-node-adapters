/**
 * Live-broker integration tests for the Artemis adapter (§7, AMQP 1.0) — opt-in.
 *
 * Skipped in the normal suite. Enable with `BQ_INTEGRATION=1` and (optionally) `BQ_ARTEMIS_HOST`
 * (default `localhost`), `BQ_ARTEMIS_PORT` (default `5672`), `BQ_ARTEMIS_USER` / `BQ_ARTEMIS_PASSWORD`
 * (default `artemis`). `rhea` is an optional peer and is not a workspace dependency, so it is loaded lazily
 * (the CI `integration` job installs it with `npm install --no-save`).
 *
 * Artemis auto-creates an AMQP address as MULTICAST, so a message published before a receiver is attached
 * can be dropped. Every test therefore attaches its receivers (the work queue and the DLQ) before it
 * publishes anything.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import {
  ArtemisConsumer,
  ArtemisPublisher,
  type AmqpEventContext,
  type AmqpMessage,
  type AmqpReceiver,
  type AmqpSender,
} from "../src/index.js";

const URN = "urn:babel:orders:created";
const HOST = process.env["BQ_ARTEMIS_HOST"] ?? "localhost";
const PORT = Number(process.env["BQ_ARTEMIS_PORT"] ?? "5672");
const USER = process.env["BQ_ARTEMIS_USER"] ?? "artemis";
const PASSWORD = process.env["BQ_ARTEMIS_PASSWORD"] ?? "artemis";
const SKIP = process.env["BQ_INTEGRATION"] === "1" ? false : "live-broker test: set BQ_INTEGRATION=1 (and BQ_ARTEMIS_HOST/PORT) to run";
const DEADLINE_MS = 60_000;
const TEST_TIMEOUT_MS = 120_000; // bounds a test that would otherwise hang on a broker that never answers

// --- Minimal rhea shapes (the package is an optional peer, so no static import) ---

interface RheaEmitter {
  once(event: string, handler: (context?: unknown) => void): unknown;
}
type RheaLink = RheaEmitter;
interface RheaSender extends AmqpSender, RheaLink {
  sendable(): boolean;
}
interface RheaReceiver extends AmqpReceiver, RheaLink {}
interface RheaConnection extends RheaEmitter {
  open_sender(options: { target: { address: string } }): RheaSender;
  open_receiver(options: { source: { address: string }; autoaccept: boolean }): RheaReceiver;
  close(): void;
}
interface RheaContainer {
  connect(options: { host: string; port: number; username: string; password: string; reconnect: boolean }): RheaConnection;
}
type RheaModule = { create_container?: () => RheaContainer; default?: { create_container: () => RheaContainer } };

async function connect(): Promise<RheaConnection> {
  const specifier = "rhea"; // non-literal: keeps `tsc` from resolving an optional peer that may be absent
  const mod = (await import(specifier)) as RheaModule;
  const create = mod.create_container ?? mod.default?.create_container;
  assert.ok(create, "rhea must be installed to run the live-broker tests");
  return create().connect({ host: HOST, port: PORT, username: USER, password: PASSWORD, reconnect: false });
}

/** Reason a rhea event context reports (an `error`, a `condition` description, or the event name). */
function describeFailure(event: string, context: unknown): string {
  const detail = context as { error?: { message?: string; condition?: string }; message?: string } | undefined;
  return detail?.error?.message ?? detail?.error?.condition ?? detail?.message ?? event;
}

/**
 * Resolve when `link` emits `openEvent`; reject (instead of hanging until the test timeout) if the
 * link or its connection errors, closes or disconnects first.
 */
function whenOpen<T extends RheaLink>(connection: RheaConnection, link: T, openEvent: string, ready: () => Promise<void> | void): Promise<T> {
  const kind = openEvent.split("_")[0]!;
  return new Promise<T>((resolve, reject) => {
    const fail = (event: string) => (context?: unknown) =>
      reject(new Error(`${kind} did not open: ${event}: ${describeFailure(event, context)}`));
    link.once(`${kind}_error`, fail(`${kind}_error`));
    link.once(`${kind}_close`, fail(`${kind}_close`));
    connection.once("connection_error", fail("connection_error"));
    connection.once("connection_close", fail("connection_close"));
    connection.once("disconnected", fail("disconnected"));
    link.once(openEvent, () => {
      Promise.resolve(ready()).then(() => resolve(link), reject);
    });
  });
}

function openReceiver(connection: RheaConnection, address: string, autoaccept: boolean): Promise<RheaReceiver> {
  const receiver = connection.open_receiver({ source: { address }, autoaccept });
  return whenOpen(connection, receiver, "receiver_open", () => undefined);
}

function openSender(connection: RheaConnection, address: string): Promise<RheaSender> {
  const sender = connection.open_sender({ target: { address } });
  return whenOpen(connection, sender, "sender_open", () => {
    if (sender.sendable()) return;
    return new Promise<void>((resolve) => sender.once("sendable", () => resolve()));
  });
}

async function waitFor(condition: () => boolean, what: string): Promise<void> {
  const stop = Date.now() + DEADLINE_MS;
  while (!condition()) {
    if (Date.now() > stop) assert.fail(`timed out after ${DEADLINE_MS}ms waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function bodyText(message: AmqpMessage): string {
  const body = message.body;
  if (typeof body === "string") return body;
  if (Buffer.isBuffer(body)) return body.toString("utf8");
  return String(body);
}

/** Receive (and auto-accept) every message that lands on the DLQ. */
function collectInto(receiver: RheaReceiver): AmqpMessage[] {
  const messages: AmqpMessage[] = [];
  receiver.on("message", (context: AmqpEventContext) => {
    if (context.message) messages.push(context.message);
  });
  return messages;
}

test("handler error raises delivery-count, then dead-letters after maxTries with a dead_letter block", { skip: SKIP, timeout: TEST_TIMEOUT_MS }, async () => {
  const queue = `bq-it-orders-${randomUUID().replace(/-/g, "").slice(0, 8)}`;
  const dlqName = `${queue}.dlq`;
  const connection = await connect();
  try {
    // Receivers first (multicast-drop rule).
    const workReceiver = await openReceiver(connection, queue, false);
    const dlqReceiver = await openReceiver(connection, dlqName, true);
    const dead = collectInto(dlqReceiver);
    const workSender = await openSender(connection, queue);
    const dlqSender = await openSender(connection, dlqName);

    const deliveryCounts: number[] = [];
    const consumer = new ArtemisConsumer(
      {
        [URN]: (_envelope, message) => {
          deliveryCounts.push(message.delivery_count ?? 0);
          throw new Error("boom");
        },
      },
      { deadLetterSender: dlqSender, maxTries: 3 },
    );
    consumer.listen(workReceiver);

    await ArtemisPublisher.create(workSender, queue).publish(URN, { order_id: 7 }, { traceId: "trace-it-1" });

    await waitFor(() => dead.length >= 1, `a message on ${dlqName}`);
    assert.equal(deliveryCounts.length, 3);
    // Scenario 3: a handler error (released with delivery_failed) makes the broker bump delivery-count.
    assert.ok(
      deliveryCounts[0]! < deliveryCounts[1]! && deliveryCounts[1]! < deliveryCounts[2]!,
      `delivery-count must strictly increase across handler failures: ${deliveryCounts.join(",")}`,
    );

    const envelope = JSON.parse(bodyText(dead[0]!)) as {
      trace_id: string;
      dead_letter?: { reason: string; original_queue: string };
    };
    assert.ok(envelope.dead_letter, "the DLQ message must carry a dead_letter block");
    assert.equal(envelope.dead_letter.reason, "failed");
    assert.equal(envelope.dead_letter.original_queue, queue);
    assert.equal(envelope.trace_id, "trace-it-1");
  } finally {
    connection.close();
  }
});

test("schema-version gate rejection forwards the raw body to the DLQ", { skip: SKIP, timeout: TEST_TIMEOUT_MS }, async () => {
  const queue = `bq-it-gate-${randomUUID().replace(/-/g, "").slice(0, 8)}`;
  const dlqName = `${queue}.dlq`;
  // Deliberately not a valid v1 envelope: the gate must reject on the property alone (§7.7).
  const body = '{"job":"urn:babel:orders:created","data":{},"future":true}';
  const connection = await connect();
  try {
    const workReceiver = await openReceiver(connection, queue, false);
    const dlqReceiver = await openReceiver(connection, dlqName, true);
    const dead = collectInto(dlqReceiver);
    const workSender = await openSender(connection, queue);
    const dlqSender = await openSender(connection, dlqName);

    let handled = 0;
    let rejected = 0;
    const consumer = new ArtemisConsumer(
      {
        [URN]: () => {
          handled += 1;
        },
      },
      {
        deadLetterSender: dlqSender,
        onError: () => {
          rejected += 1;
        },
      },
    );
    consumer.listen(workReceiver);

    workSender.send({
      body,
      application_properties: { bq_schema_version: "2" },
      message_annotations: { "x-opt-jms-type": URN },
    });

    await waitFor(() => dead.length >= 1, `a message on ${dlqName}`);
    assert.equal(bodyText(dead[0]!), body);
    assert.equal(handled, 0);
    assert.ok(rejected >= 1, "the gate must report the rejection");
  } finally {
    connection.close();
  }
});
