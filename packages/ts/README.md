# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Turn any SQLite database into a distributed, networked data layer with real-time subscriptions and multi-node replication. One library gives you connection pooling, change data capture, migrations, scheduled backups, distributed replication with conflict resolution, and a client SDK that talks over HTTP or WebSocket.

> *sirannon* means 'gate-stream' in Sindarin.

## Install

```bash
pnpm add @delali/sirannon-db
```

Then install the SQLite driver for your platform:

```bash
pnpm add better-sqlite3    # Node.js
pnpm add wa-sqlite          # Browser (IndexedDB persistence)
pnpm add expo-sqlite        # React Native (Expo)
# Node 22+ built-in sqlite and Bun need no extra package
```

## Quick start

### Node.js

```bash
pnpm add @delali/sirannon-db better-sqlite3
```

```ts
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const driver = betterSqlite3()
const sirannon = new Sirannon({ driver })
const db = await sirannon.open('app', './data/app.db')

await db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
```

Node.js 22+ users can skip the extra dependency by using the built-in `node:sqlite` module (requires the `--experimental-sqlite` flag):

```ts
import { nodeSqlite } from '@delali/sirannon-db/driver/node'

const driver = nodeSqlite()
```

### Browser

```bash
pnpm add @delali/sirannon-db wa-sqlite
```

The browser driver persists data to IndexedDB through a WebAssembly SQLite build. Use `Database.create` directly since `Sirannon` registries are designed for server-side use.

```ts
import { Database } from '@delali/sirannon-db'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'

const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
const db = await Database.create('app', '/app.db', driver, {
  readPoolSize: 1,
  walMode: false,
})

await db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
```

### React Native (Expo)

```bash
pnpm add @delali/sirannon-db expo-sqlite
```

```ts
import { Sirannon } from '@delali/sirannon-db'
import { expoSqlite } from '@delali/sirannon-db/driver/expo'

const driver = expoSqlite()
const sirannon = new Sirannon({ driver })
const db = await sirannon.open('app', 'app.db', {
  readPoolSize: 1,
})

await db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
```

### Bun

No extra dependency needed since Bun ships `bun:sqlite` built in.

```ts
import { Sirannon } from '@delali/sirannon-db'
import { bunSqlite } from '@delali/sirannon-db/driver/bun'

const driver = bunSqlite()
const sirannon = new Sirannon({ driver })
const db = await sirannon.open('app', './data/app.db')
```

### Standalone databases

You can create databases without a `Sirannon` registry on any platform:

```ts
const db = await Database.create('app', './data/app.db', driver)
```

## Pluggable drivers

Sirannon-db separates the database engine from the library. You pick the driver that fits your runtime, and the rest of the API stays the same.

| Driver | Import | Runtime | Install |
| --- | --- | --- | --- |
| better-sqlite3 | `@delali/sirannon-db/driver/better-sqlite3` | Node.js | `pnpm add better-sqlite3` |
| Node built-in | `@delali/sirannon-db/driver/node` | Node.js >= 22 | None (use `--experimental-sqlite` flag) |
| wa-sqlite | `@delali/sirannon-db/driver/wa-sqlite` | Browser | `pnpm add wa-sqlite` |
| Bun | `@delali/sirannon-db/driver/bun` | Bun | None (uses `bun:sqlite`) |
| Expo | `@delali/sirannon-db/driver/expo` | React Native | `pnpm add expo-sqlite` |

```ts
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
const driver = betterSqlite3()

// or for Node 22's built-in sqlite:
import { nodeSqlite } from '@delali/sirannon-db/driver/node'
const driver = nodeSqlite()

// or for browser with IndexedDB persistence:
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'
const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
```

## Package exports

The package ships independent exports so you only bundle what you need:

| Import | What you get |
| --- | --- |
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters (see table above) |
| `@delali/sirannon-db/file-migrations` | Load `.up.sql` / `.down.sql` files from a directory |
| `@delali/sirannon-db/server` | HTTP + WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Browser/Node.js client SDK with auto-reconnect and subscription restore |
| `@delali/sirannon-db/replication` | Replication engine, conflict resolvers, topologies, HLC |
| `@delali/sirannon-db/transport/grpc` | gRPC replication transport with TLS support |
| `@delali/sirannon-db/transport/memory` | In-memory transport for testing |

## Core features

### Queries and transactions

```ts
const row = await db.queryOne<{ count: number }>('SELECT count(*) as count FROM users')

const result = await db.execute(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Grace', 'grace@example.com'],
)
// result.changes === 1, result.lastInsertRowId === 2

await db.executeBatch('INSERT INTO tags (label) VALUES (?)', [
  ['typescript'],
  ['sqlite'],
  ['realtime'],
])

const total = await db.transaction(async tx => {
  await tx.execute('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1])
  await tx.execute('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2])
  const [row] = await tx.query<{ balance: number }>('SELECT balance FROM accounts WHERE id = ?', [2])
  return row
})
```

### Connection pooling

Every database opens with 1 dedicated write connection and N read connections (default 4). WAL mode is enabled by default, allowing concurrent reads during writes.

```ts
const db = await sirannon.open('analytics', './data/analytics.db', {
  readPoolSize: 8,
  walMode: true,
})
```

### Change data capture (CDC)

Watch tables for INSERT, UPDATE, and DELETE events in real time. The CDC system installs SQLite triggers that record changes into a tracking table, then polls at a configurable interval.

```ts
await db.watch('orders')

const subscription = db
  .on('orders')
  .filter({ status: 'shipped' })
  .subscribe(event => {
    // event.type: 'insert' | 'update' | 'delete'
    // event.row: the current row
    // event.oldRow: previous row (updates and deletes)
    // event.seq: monotonic sequence number
    console.log(`Order ${event.row.id} was ${event.type}d`)
  })

// Stop listening:
subscription.unsubscribe()

// Stop tracking entirely:
await db.unwatch('orders')
```

### Migrations

Place numbered SQL files in a directory using the `.up.sql` / `.down.sql` convention. Each migration runs inside a transaction and is tracked in a `_sirannon_migrations` table so it only applies once. Down files are optional; rollback throws if a down file is missing for a version being rolled back.

```txt
migrations/
  001_create_users.up.sql
  001_create_users.down.sql
  002_add_email_index.up.sql
  003_create_orders.up.sql
  003_create_orders.down.sql
```

Timestamp-based versioning works the same way:

```txt
migrations/
  1709312400_create_users.up.sql
  1709312400_create_users.down.sql
```

#### File-based migrations

```ts
import { loadMigrations } from '@delali/sirannon-db/file-migrations'

const migrations = loadMigrations('./migrations')
const result = await db.migrate(migrations)
// result.applied: entries that ran this time
// result.skipped: number of entries already applied
```

#### Rollback

```ts
const migrations = loadMigrations('./migrations')
await db.rollback(migrations)            // undo the last applied migration
await db.rollback(migrations, 2)         // undo all migrations after version 2
await db.rollback(migrations, 0)         // undo everything
```

#### Programmatic migrations

Pass an array of migration objects instead of loading from files:

```ts
const migrations = [
  {
    version: 1,
    name: 'create_users',
    up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
    down: 'DROP TABLE users',
  },
]

await db.migrate(migrations)
await db.rollback(migrations)        // undo last migration
await db.rollback(migrations, 0)     // undo everything
```

### Backups

One-shot backups use `VACUUM INTO` for a consistent snapshot. Scheduled backups run on a cron expression with automatic file rotation.

```ts
await db.backup('./backups/snapshot.db')

db.scheduleBackup({
  cron: '0 */6 * * *',      // every 6 hours
  destDir: './backups',
  maxFiles: 10,              // keep the 10 most recent
  onError: err => console.error('Backup failed:', err),
})
```

### Hooks

Hooks run before or after key operations. Throwing from a before-hook denies the operation.

```ts
sirannon.onBeforeQuery(ctx => {
  if (ctx.sql.includes('DROP')) {
    throw new Error('DROP statements are not allowed')
  }
})

sirannon.onAfterQuery(ctx => {
  console.log(`[${ctx.databaseId}] ${ctx.sql} took ${ctx.durationMs}ms`)
})

sirannon.onDatabaseOpen(ctx => {
  console.log(`Opened ${ctx.databaseId} at ${ctx.path}`)
})
```

Global hooks on the `Sirannon` instance: `onBeforeQuery`, `onAfterQuery`, `onBeforeConnect`, `onDatabaseOpen`, `onDatabaseClose`. The `onBeforeSubscribe` hook is available through the `HookConfig` constructor option. Query hooks (`onBeforeQuery`, `onAfterQuery`) can also be registered locally on individual `Database` instances.

**Note:** The `ctx.sql.includes('DROP')` pattern shown above is for illustration only. Simple string matching is not a production SQL firewall because casing, comments, Unicode tricks, and concatenated SQL can bypass it. For real access control, combine `onBeforeQuery` with an allow-list of query patterns or a proper SQL parser.

### Metrics

Plug in callbacks to collect query timing, connection events, and CDC activity.

```ts
const sirannon = new Sirannon({
  driver,
  metrics: {
    onQueryComplete: m => histogram.observe(m.durationMs),
    onConnectionOpen: m => gauge.inc({ db: m.databaseId }),
    onConnectionClose: m => gauge.dec({ db: m.databaseId }),
    onCDCEvent: m => counter.inc({ table: m.table, op: m.operation }),
  },
})
```

### Lifecycle management

For multi-tenant setups, the lifecycle manager handles auto-opening, idle timeouts, and LRU eviction so you don't have to manage database handles yourself.

```ts
const sirannon = new Sirannon({
  driver,
  lifecycle: {
    autoOpen: {
      resolver: id => ({ path: `/data/tenants/${id}.db` }),
    },
    idleTimeout: 300_000, // close after 5 minutes of inactivity
    maxOpen: 50,          // evict least-recently-used when full
  },
})

// Databases resolve on first access:
const db = await sirannon.resolve('tenant-42') // opens /data/tenants/tenant-42.db
```

## Server

Expose any `Sirannon` instance over HTTP and WebSocket with a single function call. The server uses uWebSockets.js for high throughput.

```ts
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'

const driver = betterSqlite3()
const sirannon = new Sirannon({ driver })
await sirannon.open('app', './data/app.db')

const server = createServer(sirannon, { port: 9876 })
await server.listen()
```

See the [Security](#security) section for authentication, TLS, and CORS configuration.

### HTTP routes

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/db/:id/query` | Execute a SELECT, returns `{ rows }` |
| `POST` | `/db/:id/execute` | Execute a mutation, returns `{ changes, lastInsertRowId }` |
| `POST` | `/db/:id/transaction` | Execute a batch of statements atomically, returns `{ results }` |
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check with per-database status |

### WebSocket protocol

Connect to `ws://host:port/db/:id` and send JSON messages for queries, executions, and CDC subscriptions. The server dispatches change events to subscribers in real time.

## Client SDK

The client SDK mirrors the core `Database` API with async methods. It supports both HTTP and WebSocket transports, with automatic reconnection and subscription restoration on the WebSocket transport.

```ts
import { SirannonClient } from '@delali/sirannon-db/client'

const client = new SirannonClient('http://localhost:9876', {
  transport: 'websocket',
  autoReconnect: true,
  reconnectInterval: 1000,
})

const db = client.database('app')

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')

await db.execute('INSERT INTO users (name) VALUES (?)', ['Turing'])

const sub = db.subscribe('users', event => {
  console.log('User changed:', event)
})

// Cleanup:
sub.unsubscribe()
client.close()
```

Transactions use the HTTP transport:

```ts
const httpClient = new SirannonClient('http://localhost:9876', {
  transport: 'http',
})

const httpDb = httpClient.database('app')

await httpDb.transaction([
  { sql: 'UPDATE accounts SET balance = balance - 50 WHERE id = ?', params: [1] },
  { sql: 'UPDATE accounts SET balance = balance + 50 WHERE id = ?', params: [2] },
])

httpClient.close()
```

## Distributed replication

Sirannon can replicate a SQLite database across multiple nodes with automatic change propagation, conflict resolution, and new-node bootstrapping. The replication layer is transport-agnostic, topology-aware, and adds zero overhead when not enabled.

```ts
import { ReplicationEngine } from '@delali/sirannon-db/replication'
import { InMemoryTransport, MemoryBus } from '@delali/sirannon-db/transport/memory'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'
```

### Primary-replica setup

One node accepts writes and pushes changes to read replicas. Replicas forward writes to the primary when `writeForwarding` is enabled.

```ts
import { ReplicationEngine, PrimaryReplicaTopology } from '@delali/sirannon-db/replication'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'

const transport = new GrpcReplicationTransport({
  host: '0.0.0.0',
  port: 4200,
  tlsCert: './certs/primary.crt',
  tlsKey: './certs/primary.key',
  tlsCaCert: './certs/ca.crt',
})

const engine = new ReplicationEngine(db, writerConn, {
  nodeId: 'primary-us-east-1',
  topology: new PrimaryReplicaTopology('primary'),
  transport,
  snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
  changeTracker: tracker,
})

await engine.start()

await engine.execute('INSERT INTO orders (id, total) VALUES (?, ?)', [1, 4999])

const rows = await engine.query<{ id: number }>('SELECT * FROM orders')
```

On the replica side:

```ts
const replicaEngine = new ReplicationEngine(replicaDb, replicaConn, {
  nodeId: 'replica-eu-west-1',
  topology: new PrimaryReplicaTopology('replica'),
  transport: replicaTransport,
  transportConfig: { endpoints: ['primary.example.com:4200'] },
  writeForwarding: true,
  changeTracker: replicaTracker,
})

await replicaEngine.start()
```

When `initialSync` is `true` (the default), a new replica automatically pulls a full snapshot from the primary before accepting reads. The replica blocks reads and writes until the sync completes and incremental catch-up reaches the configured lag threshold.

### Conflict resolution

Three built-in strategies ship with the replication module:

| Strategy | Class | Behavior |
| --- | --- | --- |
| Last-Writer-Wins | `LWWResolver` | The change with the higher HLC timestamp wins. Ties break by node ID. |
| Field-Level Merge | `FieldMergeResolver` | Merges at the column level using per-column HLC tracking. Two nodes editing different columns on the same row both succeed. |
| Primary Wins | `PrimaryWinsResolver` | The primary's version always wins. Useful for reference data that replicas should never override. |

Custom resolvers can be built by creating a class with a `resolve(ctx: ConflictContext): ConflictResolution` method.

### Initial sync

When a new node joins a running cluster, it needs the full dataset before it can process incremental changes. The initial sync protocol handles this automatically:

1. The joiner connects and sends a sync request to the source
2. The source opens a consistent read-only snapshot and sends schema DDL (CREATE TABLE, CREATE INDEX)
3. The source streams table data in configurable batches (default 10,000 rows) with per-batch checksums
4. After all data is transferred, the source sends a manifest with row counts and primary-key hashes
5. The joiner verifies the manifest, transitions to catch-up mode, and applies incremental changes accumulated during the transfer
6. Once the replication lag drops below `maxSyncLagBeforeReady`, the joiner starts serving reads

The state machine is: `pending` -> `syncing` -> `catching-up` -> `ready`. You can monitor it via `engine.status().syncState`.

For large databases where a network transfer is impractical, the out-of-band path lets you copy the SQLite file directly and start from a known sequence:

```ts
const engine = new ReplicationEngine(db, writerConn, {
  initialSync: false,
  resumeFromSeq: 50000n,
  // ...
})
```

### Write concerns

Control how many replicas must acknowledge a write before it returns:

```ts
await engine.execute(
  'INSERT INTO orders (id, total) VALUES (?, ?)',
  [1, 4999],
  { writeConcern: { level: 'majority', timeoutMs: 5000 } },
)
```

Levels: `'local'` (default, returns after local write), `'majority'` (waits for >50% of peers), `'all'` (waits for every peer).

### Transport options

| Transport | Import | Use case |
| --- | --- | --- |
| gRPC | `@delali/sirannon-db/transport/grpc` | Production Node.js multi-node replication over the network with TLS support. |
| In-Memory | `@delali/sirannon-db/transport/memory` | Testing and single-process multi-node scenarios. Messages delivered via microtask scheduling. |
| Custom | Build your own | Any transport that satisfies the `ReplicationTransport` interface (Redis, NATS, MQTT, TCP, etc). |

The `TransportConfig.localRole` field defaults to `'replica'`. Set it to `'primary'` when configuring the primary node. Only two roles exist: `'primary'` and `'replica'`.

### Replication configuration reference

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `nodeId` | `string` | auto-generated | Unique identifier for this node |
| `topology` | `Topology` | required | `PrimaryReplicaTopology` |
| `transport` | `ReplicationTransport` | required | Transport for inter-node communication |
| `transportConfig` | `TransportConfig` | `{}` | Endpoints and metadata for the transport |
| `writeForwarding` | `boolean` | `false` | Forward writes from replicas to the primary |
| `defaultConflictResolver` | `ConflictResolver` | `LWWResolver` | Default conflict resolution strategy |
| `conflictResolvers` | `Record<string, ConflictResolver>` | - | Per-table conflict resolution overrides |
| `batchSize` | `number` | `100` | Changes per replication batch |
| `batchIntervalMs` | `number` | `100` | Sender loop interval in ms |
| `maxClockDriftMs` | `number` | `60000` | Maximum tolerated HLC drift before rejecting a batch |
| `maxPendingBatches` | `number` | `10` | In-flight batches per peer before backpressure |
| `ackTimeoutMs` | `number` | `5000` | Replication batch ack timeout |
| `initialSync` | `boolean` | `true` | Pull a full snapshot when joining a cluster |
| `syncBatchSize` | `number` | `10000` | Rows per sync batch during initial sync |
| `maxConcurrentSyncs` | `number` | `2` | Maximum simultaneous sync sessions on the source |
| `maxSyncDurationMs` | `number` | `1800000` | Source aborts sync after this duration (30 min) |
| `maxSyncLagBeforeReady` | `number` | `100` | Catch-up lag threshold (in sequences) to transition to ready |
| `syncAckTimeoutMs` | `number` | `30000` | Per-batch ack timeout during sync (30s) |
| `catchUpDeadlineMs` | `number` | `600000` | Max time in catch-up phase before transitioning to ready (10 min) |
| `resumeFromSeq` | `bigint` | - | Start replication from a specific sequence (out-of-band sync) |
| `snapshotConnectionFactory` | `() => Promise<SQLiteConnection>` | - | Factory for read-only connections used during sync serving |
| `changeTracker` | `ChangeTracker` | - | CDC trigger manager, required for initial sync |
| `flowControl` | `{ maxLagSeconds?, onLagExceeded? }` | - | Replication lag monitoring callbacks |

### Replication errors

| Error | Code | When |
| --- | --- | --- |
| `ReplicationError` | `REPLICATION_ERROR` | Base class for replication failures |
| `SyncError` | `SYNC_ERROR` | Initial sync failures (node not ready, timeout, integrity mismatch) |
| `ConflictError` | `CONFLICT_ERROR` | Unresolvable write conflict |
| `TransportError` | `TRANSPORT_ERROR` | Inter-node communication failure |
| `BatchValidationError` | `BATCH_VALIDATION_ERROR` | Checksum mismatch, clock drift, or oversized batch |
| `TopologyError` | `TOPOLOGY_ERROR` | Write on a read-only node without forwarding |
| `WriteConcernError` | `WRITE_CONCERN_ERROR` | Quorum not reached within timeout |

## Security

Sirannon-db gives you secure primitives, but the network server is intentionally low-level: it can execute SQL sent by a client. Treat the server like a database endpoint, not like a public application API, unless you add an explicit application security boundary around it.

### Built-in protections

- **Parameterized values** - Query and execute calls pass parameters through the driver layer. Keep user input in `params`; never concatenate user input into SQL strings.
- **Identifier validation** - CDC table and column names are validated against a strict allowlist regex (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`) and escaped with double quotes.
- **Path traversal prevention** - Migration and backup paths reject null bytes, `..` segments, and control characters before filesystem access.
- **Request size limits** - HTTP bodies and WebSocket payloads are capped at 1 MB to reduce memory-exhaustion risk.
- **Error isolation** - Remote errors use a machine-readable code and message. Stack traces and internal details are not returned to clients.
- **Connection isolation** - Read and write operations use separate connection pools. Read-only databases enforce immutability at the connection level.

### Deployment boundary

For production, put Sirannon behind one of these boundaries:

- A server-side application layer that exposes domain actions, not arbitrary SQL.
- A private network boundary where only trusted services can reach the Sirannon server.
- A custom `resolveExecutionTarget` or hook layer that allows only specific statements, tenants, and tables.

Do not expose unrestricted `/db/:id/query`, `/db/:id/execute`, `/db/:id/transaction`, or `/db/:id` WebSocket routes directly to untrusted browsers.

### HTTP authentication

Use `onRequest` to authenticate HTTP database routes. The hook runs before database routes and can deny requests by returning `{ status, code, message }`. Health endpoints bypass this hook.

```ts
const server = createServer(sirannon, {
  port: 9876,
  onRequest: ({ headers }) => {
    if (headers.authorization !== `Bearer ${process.env.SIRANNON_API_TOKEN}`) {
      return { status: 401, code: 'UNAUTHORIZED', message: 'Invalid or missing token' }
    }
  },
})
```

Send HTTP credentials through `headers` on the client:

```ts
const client = new SirannonClient('https://db.example.com', {
  transport: 'http',
  headers: { Authorization: `Bearer ${token}` },
})
```

### WebSocket authentication

Browsers cannot attach arbitrary `Authorization` headers to `new WebSocket(...)`. For browser clients, authenticate the upgrade with a mechanism the browser can send, such as same-site cookies or a short-lived value in `Sec-WebSocket-Protocol`. Also validate the `Origin` header during the upgrade.

```ts
const expectedOrigin = 'https://app.example.com'
const expectedProtocol = process.env.SIRANNON_WS_PROTOCOL

const server = createServer(sirannon, {
  port: 9876,
  onRequest: ({ headers, method, path }) => {
    const isWebSocketUpgrade = method === 'GET' && path.startsWith('/db/')
    if (!isWebSocketUpgrade) {
      return undefined
    }

    if (headers.origin !== expectedOrigin) {
      return { status: 403, code: 'FORBIDDEN_ORIGIN', message: 'Forbidden origin' }
    }

    const protocols = (headers['sec-websocket-protocol'] ?? '').split(',').map(value => value.trim())
    if (!expectedProtocol || !protocols.includes(expectedProtocol)) {
      return { status: 401, code: 'UNAUTHORIZED', message: 'Invalid WebSocket credentials' }
    }
  },
})
```

Then pass the browser-compatible protocol through the client:

```ts
const client = new SirannonClient('https://db.example.com', {
  transport: 'websocket',
  webSocketProtocols: [wsProtocol],
})
```

Protocol values must be valid `Sec-WebSocket-Protocol` tokens. If you derive them from secrets or signed data, encode them with a URL-safe format and keep them short-lived.

### TLS and transport security

The built-in server binds plain HTTP and WebSocket. For any traffic outside a trusted local network, terminate TLS upstream with a reverse proxy, load balancer, or platform edge and use `https://` and `wss://` client URLs. Without TLS, credentials, SQL text, parameters, and CDC payloads travel in cleartext.

### CORS and browser access

CORS is disabled by default. Enable it only if browser clients need direct HTTP access, and restrict origins to trusted domains:

```ts
const server = createServer(sirannon, {
  port: 9876,
  cors: {
    origin: ['https://app.example.com'],
  },
})
```

Passing `cors: true` allows all origins and should be limited to local development. CORS does not protect WebSocket upgrades; validate WebSocket `Origin` in `onRequest`.

### SQL access control

Authentication only identifies the caller. It does not make arbitrary SQL safe. For internet-facing systems, enforce one of these patterns:

- Prefer application endpoints that perform domain operations such as `createOrder`, `reserveInventory`, or `markPaid`.
- If clients must use the Sirannon data API, wrap the database with `resolveExecutionTarget` and allow only known SQL statements and parameter shapes.
- Use hooks for additional query checks, but do not rely on naive substring matching as a SQL firewall.
- Keep tenant identifiers and ownership rules on the server side.

### Secrets and logging

- Do not place long-lived secrets in browser-visible configuration such as Vite `VITE_*` variables.
- Prefer short-lived WebSocket credentials minted by your application server.
- Redact `Authorization`, cookies, and WebSocket auth protocol values from access logs.
- Avoid logging full SQL statements when they can contain sensitive data.

### Security checklist

- Bind the server to `127.0.0.1` or a private interface unless a proxy is enforcing TLS and access control.
- Use HTTPS/WSS for non-local traffic.
- Authenticate every HTTP database route and every WebSocket upgrade.
- Validate WebSocket `Origin` against an explicit allowlist.
- Keep SQL behind application actions or a strict allowlist.
- Keep user input in SQL parameters, not interpolated strings.
- Restrict CORS to known origins.
- Add rate limits, audit logs, and abuse monitoring at the application or edge layer for public deployments.

Further reading: [OWASP REST Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/REST_Security_Cheat_Sheet.html), [OWASP WebSocket Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html), and [MDN WebSocket constructor](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/WebSocket).

## Error handling

All errors extend `SirannonError` with a machine-readable `code` property:

| Error | Code | When |
| --- | --- | --- |
| `DatabaseNotFoundError` | `DATABASE_NOT_FOUND` | Database ID not in registry |
| `DatabaseAlreadyExistsError` | `DATABASE_ALREADY_EXISTS` | Duplicate database ID |
| `ReadOnlyError` | `READ_ONLY` | Write attempted on read-only database |
| `QueryError` | `QUERY_ERROR` | SQL execution failure |
| `TransactionError` | `TRANSACTION_ERROR` | Transaction commit/rollback failure |
| `MigrationError` | `MIGRATION_ERROR` | Migration step failure |
| `HookDeniedError` | `HOOK_DENIED` | Before-hook rejected the operation |
| `CDCError` | `CDC_ERROR` | Change tracking pipeline failure |
| `BackupError` | `BACKUP_ERROR` | Backup operation failure |
| `ConnectionPoolError` | `CONNECTION_POOL_ERROR` | Pool closed or misconfigured |
| `MaxDatabasesError` | `MAX_DATABASES` | Capacity limit reached |
| `ExtensionError` | `EXTENSION_ERROR` | SQLite extension load failure |

```ts
import { QueryError } from '@delali/sirannon-db'

try {
  await db.execute('INSERT INTO users (id) VALUES (?)', [1])
} catch (err) {
  if (err instanceof QueryError) {
    console.error(`SQL failed [${err.code}]: ${err.message}`)
    console.error(`Statement: ${err.sql}`)
  }
}
```

## Configuration reference

### `SirannonOptions`

| Option | Type | Required | Description |
| --- | --- | --- | --- |
| `driver` | `SQLiteDriver` | Yes | The SQLite driver adapter to use |
| `hooks` | `HookConfig` | No | Before/after hooks for queries, connections, subscriptions |
| `metrics` | `MetricsConfig` | No | Callbacks for query timing, connection events, CDC activity |
| `lifecycle` | `LifecycleConfig` | No | Auto-open resolver, idle timeout, max open databases |

### `DatabaseOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `readOnly` | `boolean` | `false` | Open in read-only mode |
| `readPoolSize` | `number` | `4` | Number of read connections |
| `walMode` | `boolean` | `true` | Enable WAL mode |
| `cdcPollInterval` | `number` | `50` | CDC polling interval in ms |
| `cdcRetention` | `number` | `3_600_000` | CDC retention period in ms (1 hour) |

### `ServerOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `string` | `'127.0.0.1'` | Bind address |
| `port` | `number` | `9876` | Listen port |
| `cors` | `boolean \| CorsOptions` | `false` | CORS configuration |
| `onRequest` | `OnRequestHook` | - | Middleware hook for auth, rate limiting, and request validation |

### `ClientOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `transport` | `'websocket' \| 'http'` | `'websocket'` | Transport protocol |
| `headers` | `Record<string, string>` | - | Custom HTTP headers; browser WebSocket handshakes do not use this option |
| `webSocketProtocols` | `string \| string[]` | - | WebSocket subprotocols sent during the upgrade handshake |
| `autoReconnect` | `boolean` | `true` | Reconnect on WebSocket disconnect |
| `reconnectInterval` | `number` | `1000` | Reconnect delay in ms |

## Examples

Self-contained example projects live in [`examples/`](examples/) and cover the current runnable Node.js, browser, and client-server paths:

| Example | Runtime | Driver | What it demonstrates |
| --- | --- | --- | --- |
| [`node`](examples/node/) | Node.js >= 22 | better-sqlite3 or built-in `node:sqlite` | All core features: schema, migrations, CRUD, transactions, CDC, connection pools, metrics, multi-tenant, hooks, backup, shutdown |
| [`web-wa-sqlite`](examples/web-wa-sqlite/) | Browser (Vite) | wa-sqlite + IndexedDB | CRUD, transactions, CDC subscriptions in the browser |
| [`web-client`](examples/web-client/) | Browser + Node.js | better-sqlite3 (server) | Client SDK connecting to a Sirannon server over HTTP and WebSocket |

### Running the examples

From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

Then pick an example:

```bash
# Node.js with better-sqlite3, the default driver
cd packages/ts/examples/node
pnpm start

# Node.js with built-in sqlite
cd packages/ts/examples/node
pnpm run start:node-native

# Browser with wa-sqlite (opens Vite dev server)
cd packages/ts/examples/web-wa-sqlite
pnpm run dev

# Client-server (starts both Sirannon server and Vite client)
cd packages/ts/examples/web-client
pnpm run dev
```

## Benchmarks

The benchmark suite compares Sirannon's embedded SQLite performance against Postgres 17 across micro-operations, YCSB, TPC-C, and concurrency scaling. All benchmarks support driver switching via the `BENCH_DRIVER` environment variable (`better-sqlite3` or `node`). See [`benchmarks/BENCHMARKS.md`](benchmarks/BENCHMARKS.md) for setup instructions, configuration, Docker-based fair comparisons, and statistical analysis methodology.

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
