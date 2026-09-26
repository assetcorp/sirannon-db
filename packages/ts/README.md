# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, live queries, migrations, backups, device sync, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, while a primary replicates its changes to other Sirannon nodes over gRPC.

**Read the full documentation at [sirannon.sondelali.com/docs](https://sirannon.sondelali.com/docs).** Use this page to get started, and turn to the [guides](#documentation) for the detail. The benchmark harness that measures Sirannon against Postgres 17 is under [`benchmarks/server`](https://github.com/assetcorp/sirannon-db/tree/main/benchmarks/server), where a Node load generator drives both engines and a Python step joins their results. The write-up generator rewrites [BENCHMARKS.md](https://github.com/assetcorp/sirannon-db/blob/main/BENCHMARKS.md) from the latest committed run.

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
| Node built-in | `@delali/sirannon-db/driver/node` | Node.js >= 22 | None (no flag needed from 22.13.0 and 23.4.0) |
| wa-sqlite | `@delali/sirannon-db/driver/wa-sqlite` | Browser (IndexedDB) | `pnpm add -E wa-sqlite` |
| Bun | `@delali/sirannon-db/driver/bun` | Bun | None (built on `bun:sqlite`) |
| Expo | `@delali/sirannon-db/driver/expo` | React Native | `pnpm add -E expo-sqlite` |

Write a custom driver by passing `capabilities` and an `open` function to `defineDriver`.

To serve a registry over HTTP and WebSocket through `@delali/sirannon-db/server`, add uWebSockets.js as well. The npm registry has no package named `uWebSockets.js`, so install the tagged GitHub release v20.69.0, which is the version in Sirannon's own development dependencies:

```bash
pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"
```

When the process cannot load uWebSockets.js, `server.listen()` fails with code `SERVER_DEPENDENCY_MISSING` and a message that gives this install command.

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

In the browser, open the database directly with one read connection, because the `Sirannon` registry is for server-side code:

```ts
import { Database } from '@delali/sirannon-db'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'

const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
const db = await Database.create('app', '/app.db', driver, { readPoolSize: 1, walMode: false })
```

On React Native, open the database the same way with `expoSqlite()` and `readPoolSize: 1`.

## Package exports

| Import | What you get |
| --- | --- |
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, live queries, migrations, backups, hooks, metrics, and lifecycle |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters, listed in the table above |
| `@delali/sirannon-db/file-migrations` | A loader for `.up.sql` and `.down.sql` files in a directory |
| `@delali/sirannon-db/backup` | Backup destination types, chain records, and `restoreBackup` |
| `@delali/sirannon-db/backup-scheduler` | A cron-scheduled backup runner with file rotation |
| `@delali/sirannon-db/server` | An HTTP and WebSocket server built on uWebSockets.js |
| `@delali/sirannon-db/client` | A client SDK with automatic reconnection, subscription restore, and device sync |
| `@delali/sirannon-db/client/topology` | A topology-aware client that routes across a replication group |
| `@delali/sirannon-db/react` | The `useLiveQuery` and `useCommand` hooks |
| `@delali/sirannon-db/codegen` | Typed operation references generated from your server's registry |
| `@delali/sirannon-db/replication` | The replication engine, conflict resolvers, topologies, and HLC |
| `@delali/sirannon-db/replication/coordinator/etcd` | An etcd-backed coordinator for primary authority and failover |
| `@delali/sirannon-db/transport/grpc` | A gRPC replication transport with TLS support |
| `@delali/sirannon-db/transport/memory` | An in-memory transport for tests |

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

`bulkLoad` writes a large import in one transaction under relaxed durability, and then it restores the durability level that you configured. Read the [bulk load guide](https://sirannon.sondelali.com/docs/bulk-load) for the details, and the guides on [migrations](https://sirannon.sondelali.com/docs/migrations) and on [hooks, metrics, and the multi-tenant lifecycle](https://sirannon.sondelali.com/docs/hooks-metrics-and-lifecycle) for the rest of the core engine. Backups have a [guide of their own](https://sirannon.sondelali.com/docs/backups).

## Change data capture and live queries

Subscribe to a table to receive each row that changes:

```ts
await db.watch('orders')

const subscription = db
  .on('orders')
  .filter({ status: 'shipped' })
  .subscribe(event => console.log(event.type, event.table, event.row, event.oldRow, event.seq))
```

With a filter, you receive each change in a row's membership of the set that the filter matches. When an order's `status` changes from `pending` to `shipped`, the handler receives an insert, because the row joins the set. When an order's `status` changes away from `shipped`, the handler receives a delete with the previous row in `oldRow`. Read `event.type` as the row joining or leaving the set, because the handler receives the same event for a real insert and for a row that joins the set.

A live query holds the current result of a query and updates its rows from those same events:

```ts
const pending = await db.live<{ id: number; total: number }>(
  'SELECT id, total FROM orders WHERE status = ? ORDER BY id',
  ['pending'],
)

pending.subscribe(() => render(pending.getState()))
```

Read the [live queries guide](https://sirannon.sondelali.com/docs/live-queries) for the kinds of update, the statements that a live query can maintain, and the React hooks.

## Serve it over the network

A server rejects SQL from the network by default. Callers call the reads and writes that you register on it by name:

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

The `sirannon-codegen` command generates those references from the registry, so you can skip writing them by hand. Set `acceptSql: true` when you want the server to execute the statements that a client sends. Read the [registered operations guide](https://sirannon.sondelali.com/docs/registered-operations) and the [code generation guide](https://sirannon.sondelali.com/docs/code-generation) for operations and their references, the [server guide](https://sirannon.sondelali.com/docs/server) for `acceptSql`, the routes, and the messages, and the [client guide](https://sirannon.sondelali.com/docs/client-sdk) for the transports.

## Security

With registered operations, your SQL stays on the server, so a caller of a registered read or write supplies only the arguments that you declared for it. With `acceptSql: true`, the server executes the statements that an admitted caller sends, so put that server behind an application layer or a private network boundary, or give it a `resolveExecutionTarget` that accepts only the statements that you know.

Authenticate every request through the `authenticate` hook. Return the caller's identity from the hook so that the server can fill each `fromIdentity` argument of a registered operation, and throw to reject the request:

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

A Node client attaches `headers` to the WebSocket upgrade as well as to HTTP requests, so your hook can read `headers.authorization` on both transports:

```ts
const client = new SirannonClient('https://api.example.com', {
  headers: { Authorization: `Bearer ${token}` },
})
```

A browser cannot attach a header to `new WebSocket(...)`, so give a browser client a short-lived ticket in `webSocketProtocols`:

```ts
const client = new SirannonClient('https://api.example.com', {
  webSocketProtocols: [`sirannon.ticket.${ticket}`],
})
```

When you construct a browser client on the WebSocket transport with `headers` alone, the constructor throws `INVALID_ARGUMENT`, because the browser would leave that credential out of the handshake. Pass both options when a browser client needs each of them, as the [entitlements example](https://github.com/assetcorp/sirannon-db/tree/main/packages/ts/examples/distributed-entitlements) does. Its topology client sends `headers` with the discovery request to `GET /db/{id}/cluster`, while it sends the ticket with the socket handshake.

Because the client offers the plain `sirannon.v1` identifier ahead of your values and the server selects it, the ticket stays out of the handshake response. Check the `Origin` header in the same hook. When the hook rejects an upgrade with status 401 or 403, the server closes the connection with code 4401 or 4403, and the client raises `UNAUTHORIZED` or `FORBIDDEN` and leaves that connection closed.

- Bind to `127.0.0.1` or a private interface unless a proxy enforces TLS and access control.
- Use HTTPS and WSS for traffic beyond the local machine, because the built-in server listens on plain HTTP.
- Authenticate every HTTP database route and every WebSocket upgrade, and check `Origin` against an allowlist.
- Keep user input in parameters, which the driver binds separately from the SQL text.
- Restrict CORS to known origins, since `cors: true` allows every origin and is for local development only.
- Keep long-lived secrets out of browser-visible configuration, and redact credentials from access logs.
- Add rate limits, audit logs, and abuse monitoring at the application or edge layer.

Read the [security guide](https://sirannon.sondelali.com/docs/security) for each of these in full.

## Documentation

| Guide | Topics |
| --- | --- |
| [Queries and transactions](https://sirannon.sondelali.com/docs/queries-and-transactions) | Parameterised SQL, batches, transactions, and the connection pool |
| [Bulk load](https://sirannon.sondelali.com/docs/bulk-load) | One-transaction imports under relaxed durability, over the server, and through the client SDK |
| [Migrations](https://sirannon.sondelali.com/docs/migrations) | File-based, programmatic, and bundled migrations, rollback, checksums, baselines, concurrency, and registry migrations |
| [Hooks, metrics, and lifecycle](https://sirannon.sondelali.com/docs/hooks-metrics-and-lifecycle) | Before and after hooks, metrics callbacks, and the multi-tenant lifecycle |
| [Backups](https://sirannon.sondelali.com/docs/backups) | Copies of an open database to a file and on a schedule, followed by the guides on [destinations](https://sirannon.sondelali.com/docs/backup-destinations), [continuous backups](https://sirannon.sondelali.com/docs/backup-chains), and [restores](https://sirannon.sondelali.com/docs/backup-restore) |
| [Server](https://sirannon.sondelali.com/docs/server) | Registered operations on the server, caller identity, size limits, write shapes, `acceptSql`, the backup routes, HTTP routes, and the WebSocket protocol |
| [Registered operations](https://sirannon.sondelali.com/docs/registered-operations) | Named reads and writes, identity-filled arguments, calls over HTTP and WebSocket, capabilities, and refusals |
| [Live queries](https://sirannon.sondelali.com/docs/live-queries) | Maintained query results locally, over the network, and in React |
| [Client SDK](https://sirannon.sondelali.com/docs/client-sdk) | Reads, writes, subscriptions, live queries, bulk imports, read concern, refused credentials, and client options |
| [Device sync](https://sirannon.sondelali.com/docs/device-sync) | Offline-first two-way sync between a device's local database and a server |
| [Distributed replication](https://sirannon.sondelali.com/docs/distributed-replication) | Certificates, first sync, write and read concerns, conflict resolution, coordinator failover, and schema changes |
| [Configuration reference](https://sirannon.sondelali.com/docs/configuration) | Every option for the registry, a database, backups, the server, the client, device sync, and replication |
| [Error codes](https://sirannon.sondelali.com/docs/error-codes) | Every error code, whether a retry is safe, its HTTP status, and the WebSocket close codes |

The wire formats, the value encodings, and the replication invariants that every implementation follows are in the [specification](https://github.com/assetcorp/sirannon-db/tree/main/packages/spec).

## Example projects

| Example | Runtime | What it demonstrates |
| --- | --- | --- |
| [`node`](https://github.com/assetcorp/sirannon-db/tree/main/packages/ts/examples/node) | Node.js >= 22 | Schema, migrations, CRUD, transactions, CDC, live queries, pools, metrics, multi-tenant lifecycle, hooks, backup, and shutdown |
| [`web-wa-sqlite`](https://github.com/assetcorp/sirannon-db/tree/main/packages/ts/examples/web-wa-sqlite) | Browser and Node.js | Offline-first device sync with a local database in the browser, snapshot load, offline writes, conflict resolution, and a local live query |
| [`web-client`](https://github.com/assetcorp/sirannon-db/tree/main/packages/ts/examples/web-client) | Browser and Node.js | Registered operations, code generation, remote live queries, and the React hooks |
| [`distributed-entitlements`](https://github.com/assetcorp/sirannon-db/tree/main/packages/ts/examples/distributed-entitlements) | Node.js and browser | Three-node coordinator-backed replication over gRPC with etcd authority, mTLS, and Toxiproxy failure controls |

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
