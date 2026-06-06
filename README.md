# BabelQueue — Node.js adapters

[![CI](https://github.com/BabelQueue/babelqueue-node-adapters/actions/workflows/ci.yml/badge.svg)](https://github.com/BabelQueue/babelqueue-node-adapters/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **Polyglot Queues, Simplified.** Framework adapters that bind the
> [`@babelqueue/core`](https://www.npmjs.com/package/@babelqueue/core) codec to the
> Node ecosystem, so your Node services exchange the canonical BabelQueue envelope
> with the PHP/Laravel, Python, Go, Java and .NET SDKs.

An npm-workspaces monorepo with two published packages:

| Package | What |
| :--- | :--- |
| [`@babelqueue/bullmq`](packages/bullmq) | BullMQ jobs carry the canonical envelope + a URN-routed processor |
| [`@babelqueue/nestjs`](packages/nestjs) | A NestJS module + injectable publisher (over BullMQ) |

The full standard is documented at **[babelqueue.com](https://babelqueue.com)**.

## Develop

```bash
npm install        # links workspaces, installs deps
npm run build      # builds @babelqueue/bullmq then @babelqueue/nestjs
npm run typecheck
npm test           # node:test + tsx, no broker required
```

## License

[MIT](LICENSE) © Muhammet Şafak
