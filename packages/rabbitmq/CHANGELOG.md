# Changelog

All notable changes to `@babelqueue/rabbitmq` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-14

### Added
- Initial release. A RabbitMQ adapter on `@babelqueue/core` over AMQP 0-9-1 (`amqplib`),
  implementing §2 of the broker-bindings contract. `RabbitMQPublisher` (the §2 projection — body =
  envelope, `type` = URN, `correlation_id` = `trace_id`, `message_id` = `meta.id`, `app_id`,
  persistent delivery, plus the native-typed `x-schema-version` / `x-source-lang` / `x-attempts`
  headers) and `RabbitMQConsumer` (`basic.get` + manual ack; route on `properties.type` falling
  back to the body URN; a throwing handler republishes with `attempts + 1` then dead-letters to
  `<queue>.dlq`; `fail`/`delete`/`release`/`dead_letter` unknown-URN strategies; poison bodies
  forwarded raw to the DLQ). `attempts` lives in the body (the runtime owns retry). Built on
  `@babelqueue/core`; `amqplib` is an optional peer (the channel is injected, so the unit tests
  use a fake — no RabbitMQ, no broker). Dual ESM+CJS. The envelope is unchanged
  (`schema_version: 1`).
