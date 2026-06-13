# Changelog

All notable changes to `@babelqueue/artemis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Apache ActiveMQ Artemis adapter on `@babelqueue/core` over **AMQP 1.0**
  (rhea), implementing §7 of the broker-bindings contract. Artemis offers native
  settlement/scheduled-delivery/delivery-counter/dead-letter-address, so the binding maps onto
  them rather than re-implementing: `ArtemisPublisher` (body = canonical envelope,
  `correlation-id` = `trace_id`, `creation-time` = `meta.created_at`, the `x-opt-jms-type`
  annotation = URN — so a Java/JMS or AMQP consumer routes on `JMSType`; a delay uses native
  AMQP scheduled delivery via `x-opt-delivery-time`) and `ArtemisConsumer` (per-message
  settlement — `accept` after success; a throwing handler `release`s the message for broker
  redelivery; **`attempts = max(body, delivery-count)`** with the AMQP counter 0-based, so no
  −1; terminal failures go to `<queue>.dlq` with the additive `dead_letter` block;
  `fail`/`delete`/`release`/`dead_letter` unknown-URN strategies; poison bodies forwarded raw to
  the DLQ). `listen(receiver)` wires the consumer to a rhea receiver's `message` event;
  `handle(context)` is exposed for testing. Built on `@babelqueue/core`; `rhea` is an optional
  peer (the sender/receiver are injected, so the unit tests use fakes — no Artemis, no broker).
  Dual ESM+CJS. The envelope is unchanged (`schema_version: 1`); Apache ActiveMQ Artemis is
  purely additive.
