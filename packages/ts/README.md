# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, migrations, backups, device sync, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, while Sirannon nodes replicate primary-owned changes over gRPC.

**Read the full documentation at [sirannon.sondelali.com/docs](https://sirannon.sondelali.com/docs).** This page gets you running; the [guides](#documentation) hold the reference depth. Benchmarks against Postgres 17 are in [BENCHMARKS.md](../../BENCHMARKS.md).

The core engine, server, client, and primary-replica replication are stable. Coordinator-backed failover and the Bun and Expo drivers are experimental.

> *sirannon* means 'gate-stream' in Sindarin.

## Install

```bash
pnpm add -E @delali/sirannon-db
```

Then add the SQLite driver for your runtime:

| Driver | Import | Runtime | Install |
| --- | --- | --- | --- |
| better-sqlite3 | `@delali/sirannon-db/driver/better-sqlite3` | Node.js | `pnpm add -E better-sqlite3` |
| Node built-in | `@delali/sirannon-db/driver/node` | Node.js >= 22 | None (flag-free from 22.13.0 and 23.4.0) |
| wa-sqlite | `@delali/sirannon-db/driver/wa-sqlite` | Browser (IndexedDB) | `pnpm add -E wa-sqlite` |
| Bun | `@delali/sirannon-db/driver/bun` | Bun | None (uses `bun:sqlite`) |
| Expo | `@delali/sirannon-db/driver/expo` | React Native | `pnpm add -E expo-sqlite` |

Write a custom driver by passing `capabilities` and an `open` function to `defineDriver`.

## Quick start

```ts
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const sirannon = new Sirannon({ driver: betterSqlite3() })
const db = await sirannon.open('app', './data/app.db')

await db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
```

In the browser, open the database directly and use one read connection, since the `Sirannon` registry is built for server-side use:

```ts
import { Database } from '@delali/sirannon-db'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'

const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
const db = await Database.create('app', '/app.db', driver, { readPoolSize: 1, walMode: false })
```

React Native uses the same shape through `expoSqlite()` with `readPoolSize: 1`.

## Package exports

| Import | What you get |
| --- | --- |
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters (see table above) |
| `@delali/sirannon-db/file-migrations` | Load `.up.sql` / `.down.sql` files from a directory |
| `@delali/sirannon-db/backup-scheduler` | Cron-scheduled backup runner with file rotation |
| `@delali/sirannon-db/server` | HTTP + WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Client SDK with auto-reconnect, subscription restore, and device sync |
| `@delali/sirannon-db/replication` | Replication engine, conflict resolvers, topologies, HLC |
| `@delali/sirannon-db/replication/coordinator/etcd` | etcd-backed coordinator for primary authority and failover |
| `@delali/sirannon-db/transport/grpc` | gRPC replication transport with TLS support |
| `@delali/sirannon-db/transport/memory` | In-memory transport for testing |

## Queries and transactions

```ts
const row = await db.queryOne<{ count: number }>('SELECT count(*) as count FROM users')

const result = await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Grace', 'grace@example.com'])

await db.executeBatch('INSERT INTO tags (label) VALUES (?)', [['typescript'], ['sqlite'], ['realtime']])

const balance = await db.transaction(async tx => {
  await tx.execute('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1])
  await tx.execute('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2])
  return tx.queryOne<{ balance: number }>('SELECT balance FROM accounts WHERE id = ?', [2])
})
```

A large import runs faster through `bulkLoad`, which trades durability for speed inside one transaction and restores the configured level afterwards. The [core engine guide](../../docs/core.md) covers it, along with migrations, backups, hooks, metrics, and the multi-tenant lifecycle.

## Change data capture

```ts
await db.watch('orders')

const subscription = db
  .on('orders')
  .filter({ status: 'shipped' })
  .subscribe(event => {
    console.log(event.type, event.table, event.row, event.oldRow, event.seq)
  })

subscription.unsubscribe()
await db.unwatch('orders')
```

## Serve it over the network

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, { port: 9876 })
await server.listen()
```

Clients reach that server through the SDK, which restores subscriptions across reconnects:

```ts
import { SirannonClient } from '@delali/sirannon-db/client'

const client = new SirannonClient('http://localhost:9876', { transport: 'websocket', autoReconnect: true })
const db = client.database('app')

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
const sub = await db.on('users').subscribe(event => console.log('User changed:', event))
```

The [server guide](../../docs/server.md) lists the HTTP routes, the WebSocket messages, the three write shapes, and the JSON encoding for blobs and large integers. The [client guide](../../docs/client.md) covers the two transports.

## Security

Sirannon's server can execute SQL sent by a client, so treat it as a database endpoint and not as a public application API. Put it behind an application layer that exposes domain actions, a private network boundary, or a `resolveExecutionTarget` layer that allows only known statements.

```ts
const server = createServer(sirannon, {
  port: 9876,
  cors: { origin: ['https://app.example.com'] },
  onRequest: ({ headers }) => {
    if (headers.authorization !== `Bearer ${process.env.SIRANNON_API_TOKEN}`) {
      return { status: 401, code: 'UNAUTHORIZED', message: 'Invalid or missing token' }
    }
  },
})
```

Browsers cannot attach an `Authorization` header to `new WebSocket(...)`, so authenticate the upgrade with a same-site cookie or a short-lived value in `Sec-WebSocket-Protocol`, and check the `Origin` header in the same hook. Pass that value through the client with `webSocketProtocols`.

- Bind to `127.0.0.1` or a private interface unless a proxy enforces TLS and access control.
- Use HTTPS and WSS for non-local traffic; the built-in server binds plain HTTP.
- Authenticate every HTTP database route and every WebSocket upgrade, and validate `Origin` against an allowlist.
- Keep SQL behind application actions or a strict allowlist, and keep user input in parameters.
- Restrict CORS to known origins; `cors: true` allows every origin and belongs in local development.
- Keep long-lived secrets out of browser-visible configuration, and redact credentials from access logs.
- Add rate limits, audit logs, and abuse monitoring at the application or edge layer.

The [security guide](https://sirannon.sondelali.com/docs) covers each of these in full.

## Documentation

| Guide | What it covers |
| --- | --- |
| [Core engine](../../docs/core.md) | Bulk load, migrations, backups, hooks, metrics, and the multi-tenant lifecycle |
| [Server](../../docs/server.md) | HTTP routes, WebSocket messages, write shapes, the writer worker, and value encoding |
| [Client SDK](../../docs/client.md) | Connecting over HTTP or WebSocket, subscriptions, and transactions |
| [Device sync](../../docs/device-sync.md) | Offline-first two-way sync between a device's local database and a server |
| [Distributed replication](../../docs/replication.md) | Primary-replica replication, first sync, write concerns, coordinator failover, conflict resolvers, and transports |
| [Configuration reference](../../docs/configuration.md) | Every option table, from `SirannonOptions` to `TransportConfig` |
| [Errors](../../docs/errors.md) | Every error class, its code, and whether the call is safe to retry |

The [specification](../spec/) defines the wire formats, value encodings, and replication invariants every implementation follows.

## Example projects

| Example | Runtime | What it demonstrates |
| --- | --- | --- |
| [`node`](examples/node/) | Node.js >= 22 | Schema, migrations, CRUD, transactions, CDC, pools, metrics, multi-tenant, hooks, backup, shutdown |
| [`web-wa-sqlite`](examples/web-wa-sqlite/) | Browser (Vite) | CRUD, transactions, and CDC subscriptions in the browser |
| [`web-client`](examples/web-client/) | Browser + Node.js | Client SDK over HTTP and WebSocket |
| [`distributed-entitlements`](examples/distributed-entitlements/) | Node.js + browser | Three-node coordinator-backed replication over gRPC with etcd authority, mTLS, and Toxiproxy failure controls |

```bash
pnpm install && pnpm --filter @delali/sirannon-db build
cd packages/ts/examples/node && pnpm start
```

## Development

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
pnpm lint
```

## License

Apache-2.0
