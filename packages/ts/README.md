# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, live queries, migrations, backups, device sync, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, and Sirannon nodes replicate primary-owned changes over gRPC.

**Read the full documentation at [sirannon.sondelali.com/docs](https://sirannon.sondelali.com/docs).** This page gets you running, and the [guides](#documentation) hold the reference depth. The suite that measures Sirannon against Postgres 17 is a Python project under [`benchmarks/server`](../../benchmarks/server), and the write-up generator rewrites [BENCHMARKS.md](../../BENCHMARKS.md) from the latest committed run.

The core engine, server, client, and primary-replica replication are stable. Coordinator-backed failover, device sync, and the Bun and Expo drivers are experimental.

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

In the browser, open the database directly and use one read connection, because the `Sirannon` registry is built for server-side use:

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
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, live queries, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters (see the table above) |
| `@delali/sirannon-db/file-migrations` | Load `.up.sql` and `.down.sql` files from a directory |
| `@delali/sirannon-db/backup-scheduler` | Cron-scheduled backup runner with file rotation |
| `@delali/sirannon-db/server` | HTTP and WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Client SDK with auto-reconnect, subscription restore, and device sync |
| `@delali/sirannon-db/client/topology` | Topology-aware client that routes across a replication group |
| `@delali/sirannon-db/react` | `useLiveQuery` and `useCommand` hooks |
| `@delali/sirannon-db/codegen` | Typed operation references generated from your server's registry |
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

## Change data capture and live queries

A subscription reports the rows that changed:

```ts
await db.watch('orders')

const subscription = db
  .on('orders')
  .filter({ status: 'shipped' })
  .subscribe(event => console.log(event.type, event.table, event.row, event.oldRow, event.seq))
```

A live query reports the current answer, updating the rows it holds from those same events:

```ts
const pending = await db.live<{ id: number; total: number }>(
  'SELECT id, total FROM orders WHERE status = ? ORDER BY id',
  ['pending'],
)

pending.subscribe(() => render(pending.getState()))
```

The [live queries guide](../../docs/live-queries.md) covers the update kinds, the statements a live query maintains, and the React hooks.

## Serve it over the network

A server accepts no SQL from the network by default. Register the reads and writes it runs, and callers invoke them by name:

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, {
  port: 9876,
  operations: {
    app: {
      reads: {
        activeUsers: {
          columns: ['id', 'name'],
          statement: () => ({ sql: 'SELECT id, name FROM users WHERE active = 1' }),
        },
      },
      writes: {
        addUser: {
          args: ['name'],
          statements: ({ name }) => ({ sql: 'INSERT INTO users (name) VALUES (?)', params: [name] }),
        },
      },
    },
  },
})

await server.listen()
```

```ts
import { SirannonClient } from '@delali/sirannon-db/client'
import { operationRef } from '@delali/sirannon-db'

const activeUsers = operationRef<Record<string, never>, { id: number; name: string }>('activeUsers')

const client = new SirannonClient('http://localhost:9876', { transport: 'websocket', autoReconnect: true })
const db = client.database('app')

const users = await db.query(activeUsers, {})
const sub = await db.on('users').subscribe(event => console.log('User changed:', event))
```

Run `sirannon-codegen` to generate those references from the registry instead of writing them by hand, and set `acceptSql: true` when you want the server to run statements a client sends. The [registered operations guide](../../docs/operations.md) covers both, the [server guide](../../docs/server.md) lists the routes and messages, and the [client guide](../../docs/client.md) covers the transports.

## Security

Registered operations keep SQL on the server, so a caller reaches only the reads and writes you defined. Turning on `acceptSql` gives every client the run of the database, so put such a server behind an application layer, a private network boundary, or a `resolveExecutionTarget` that allows only known statements.

Authenticate every request through the `authenticate` hook. Return the caller's identity, which registered operations read through `fromIdentity`, and throw to refuse:

```ts
import { RequestDeniedError } from '@delali/sirannon-db'

const server = createServer<Identity>(sirannon, {
  port: 9876,
  cors: { origin: ['https://app.example.com'] },
  operations,
  authenticate: ({ headers }) => {
    const offered = (headers['sec-websocket-protocol'] ?? '').split(',').map(value => value.trim())
    const ticket = offered.find(value => value.startsWith('sirannon.ticket.'))
    const identity = verifyBearerToken(headers.authorization) ?? verifyTicket(ticket)
    if (!identity) throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Invalid or missing token')
    return identity
  },
})
```

A Node client attaches `headers` to the WebSocket upgrade as well as to HTTP requests, so the hook reads `headers.authorization` on both transports:

```ts
const client = new SirannonClient('https://api.example.com', {
  headers: { Authorization: `Bearer ${token}` },
})
```

A browser attaches no header to `new WebSocket(...)`, so a browser client carries a short-lived ticket in `webSocketProtocols` instead. A browser client built with `headers` alone on the WebSocket transport fails at construction with `INVALID_ARGUMENT`, because that credential would never reach the server:

```ts
const client = new SirannonClient('https://api.example.com', {
  webSocketProtocols: [`sirannon.ticket.${ticket}`],
})
```

Pass both options when a browser client needs each of them, as the [entitlements example](examples/distributed-entitlements) does: the topology client sends `headers` on its coordinator discovery request to `GET /db/{id}/cluster` and the ticket on the socket handshake.

The client offers the plain `sirannon.v1` identifier ahead of your values and the server selects that identifier, so the ticket never comes back in the handshake response. Check the `Origin` header in the same hook. When the hook refuses an upgrade with status 401 or 403, the server closes the connection with code 4401 or 4403, and the client raises `UNAUTHORIZED` or `FORBIDDEN` and leaves that connection closed.

- Bind to `127.0.0.1` or a private interface unless a proxy enforces TLS and access control.
- Use HTTPS and WSS for non-local traffic, because the built-in server binds plain HTTP.
- Authenticate every HTTP database route and every WebSocket upgrade, and check `Origin` against an allowlist.
- Keep user input in parameters, which the driver binds rather than splicing into the SQL text.
- Restrict CORS to known origins; `cors: true` allows every origin and belongs in local development.
- Keep long-lived secrets out of browser-visible configuration, and redact credentials from access logs.
- Add rate limits, audit logs, and abuse monitoring at the application or edge layer.

The [security guide](https://sirannon.sondelali.com/docs) covers each of these in full.

## Documentation

| Guide | What it covers |
| --- | --- |
| [Core engine](../../docs/core.md) | Bulk load, live queries, migrations, backups, hooks, metrics, and the multi-tenant lifecycle |
| [Server](../../docs/server.md) | HTTP routes, WebSocket messages, authentication, write shapes, the writer worker, and value encoding |
| [Registered operations](../../docs/operations.md) | Naming the statements a server runs, identity-filled arguments, capabilities, and code generation |
| [Live queries](../../docs/live-queries.md) | Maintained query results locally, over the network, and in React |
| [Client SDK](../../docs/client.md) | Transports, subscriptions, topology-aware routing, and read concern |
| [Device sync](../../docs/device-sync.md) | Offline-first two-way sync between a device's local database and a server |
| [Distributed replication](../../docs/replication.md) | Replication, first sync, write and read concerns, coordinator failover, resolvers, and transports |
| [Configuration reference](../../docs/configuration.md) | Every option table, from `SirannonOptions` to `GrpcReplicationOptions` |
| [Errors](../../docs/errors.md) | Every code, when it happens, whether the call is safe to retry, and its HTTP status |

The [specification](../spec/) defines the wire formats, value encodings, and replication invariants every implementation follows.

## Example projects

| Example | Runtime | What it demonstrates |
| --- | --- | --- |
| [`node`](examples/node/) | Node.js >= 22 | Schema, migrations, CRUD, transactions, CDC, live queries, pools, metrics, multi-tenant lifecycle, hooks, backup, shutdown |
| [`web-wa-sqlite`](examples/web-wa-sqlite/) | Browser and Node.js | Offline-first device sync: a local database in the browser, snapshot load, offline writes, conflict resolution, and a local live query |
| [`web-client`](examples/web-client/) | Browser and Node.js | Registered operations, code generation, remote live queries, and the React hooks |
| [`distributed-entitlements`](examples/distributed-entitlements/) | Node.js and browser | Three-node coordinator-backed replication over gRPC with etcd authority, mTLS, and Toxiproxy failure controls |

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
