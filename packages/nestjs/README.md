# @babelqueue/nestjs

[![npm](https://img.shields.io/npm/v/@babelqueue/nestjs.svg)](https://www.npmjs.com/package/@babelqueue/nestjs)

> **Polyglot Queues, Simplified.** A NestJS adapter for BabelQueue: an injectable
> publisher (over BullMQ) that emits the canonical BabelQueue envelope, so NestJS
> services interoperate with the PHP/Laravel, Python, Go, Java and .NET SDKs.

```bash
npm install @babelqueue/nestjs @nestjs/common bullmq
```

## Register the module

```ts
import { Module } from "@nestjs/common";
import { BabelQueueModule } from "@babelqueue/nestjs";

@Module({
  imports: [
    BabelQueueModule.forRoot({
      queue: "orders",
      connection: { host: "localhost", port: 6379 },
    }),
  ],
})
export class AppModule {}
```

## Produce

```ts
import { Injectable } from "@nestjs/common";
import { BabelQueuePublisher } from "@babelqueue/nestjs";

@Injectable()
export class Orders {
  constructor(private readonly babelQueue: BabelQueuePublisher) {}

  create() {
    return this.babelQueue.publish("urn:babel:orders:created", { order_id: 1042 });
  }
}
```

## Consume

Build a BullMQ worker with the re-exported `processor` (URN routing):

```ts
import { Worker } from "bullmq";
import { processor } from "@babelqueue/nestjs";

new Worker("orders", processor({
  "urn:babel:orders:created": async (env) => { /* ... */ },
}), { connection: { host: "localhost", port: 6379 } });
```

## License

[MIT](../../LICENSE) © Muhammet Şafak · [babelqueue.com](https://babelqueue.com)
