# Sirannon DB - Client SDK Example

Browser client connecting to a Sirannon server over HTTP and WebSocket transports.

## Prerequisites

- Node.js >= 22
- pnpm

## Setup

From the monorepo root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

## Run

Start both server and client:

```bash
cd packages/ts/examples/web-client
pnpm start
```

Or run them separately:

```bash
pnpm server   # starts Sirannon server on port 9876
pnpm dev      # starts Vite dev server on port 5174
```

Open the Vite URL in your browser.

## Features demonstrated

### Server (`src/server.ts`)

- Sirannon server with `createServer()`, `cors: true`, and no `onRequest` authentication (fine for local dev)
- Binds to `127.0.0.1` by default; set `HOST` (for example `0.0.0.0`) only if you need remote interfaces, then add auth and tighten CORS yourself
- Database hooks (`onDatabaseOpen`, `onDatabaseClose`)
- CDC watch on multiple tables
- Seed data insertion

### Client (`src/client.ts`)

- `SirannonClient` with HTTP transport (query, execute, transaction)
- `SirannonClient` with WebSocket transport (query, subscribe to CDC events)
- Remote database proxy via `client.database(id)`
- Real-time CDC subscriptions through WebSocket
