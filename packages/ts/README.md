# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, migrations, backups, device sync, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, while Sirannon nodes replicate primary-owned changes over gRPC.

**Read the full documentation at [sirannon.sondelali.com/docs](https://sirannon.sondelali.com/docs).** This page is a quick reference: examples plus the configuration tables. Benchmarks against Postgres 17 are in [BENCHMARKS.md](../../BENCHMARKS.md).

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

## Bulk load

`bulkLoad` runs the whole batch in one transaction under relaxed durability, then restores the configured level. Use it for imports you can re-run after a crash.

```ts
const summary = await db.bulkLoad('INSERT INTO events (id, payload) VALUES (?, ?)', rows, { durability: 'off' })
```

Over the client, `loadAll` splits an iterable into batches and checkpoints the WAL once at the end:

```ts
const summary = await db.loadAll('INSERT INTO events (id, payload) VALUES (?, ?)', rowStream, {
  batchSize: 5000,
  durability: 'off',
})
```

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

## Migrations

Numbered `.up.sql` and `.down.sql` files apply once each, inside a transaction, tracked in `_sirannon_migrations` with a checksum. Versions must be integers from 1 to 2,147,483,647 so they fit `PRAGMA user_version`, which mirrors the highest applied version.

```txt
migrations/
  001_create_users.up.sql
  001_create_users.down.sql
  002_add_email_index.up.sql
```

```ts
import { loadMigrations } from '@delali/sirannon-db/file-migrations'

const migrations = loadMigrations('./migrations')
await db.migrate(migrations)

await db.rollback(migrations)      // undo the last migration
await db.rollback(migrations, 2)   // undo everything after version 2
await db.rollback(migrations, 0)   // undo everything
```

Pass migration objects directly when you do not load from disk:

```ts
await db.migrate([
  { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)', down: 'DROP TABLE users' },
])
```

Bundlers inline `.sql` files as strings, so a bundled app builds the same set without filesystem access:

```ts
import { migrationsFromFiles } from '@delali/sirannon-db'

const files = import.meta.glob('./migrations/*.sql', { query: '?raw', import: 'default', eager: true })
await db.migrate(migrationsFromFiles(files))
```

A baseline squashes history. Write one file holding the full schema and mark the highest version it supersedes; a fresh database runs the baseline and everything after it, and a database with real history keeps using that history:

```ts
const migrations = loadMigrations('./migrations', { baseline: { version: 701, through: 700 } })
```

Declare the set on the registry to migrate every database it opens, including tenants resolved lazily:

```ts
const sirannon = new Sirannon({
  driver,
  migrations: () => loadMigrations('./migrations'),
  lifecycle: { autoOpen: { resolver: id => ({ path: `/data/tenants/${id}.db` }) } },
})

const db = await sirannon.resolve('tenant-42')
```

## Backups

```ts
await db.backup('./backups/snapshot.db')

db.scheduleBackup({
  cron: '0 */6 * * *',
  destDir: './backups',
  maxFiles: 10,
  timezone: 'America/New_York',
  onError: err => console.error('Backup failed:', err),
})
```

## Hooks and metrics

Throwing from a before-hook denies the operation.

```ts
sirannon.onBeforeQuery(ctx => {
  if (!isAllowedStatement(ctx.sql)) throw new Error('Statement not allowed')
})

sirannon.onAfterQuery(ctx => console.log(`[${ctx.databaseId}] ${ctx.sql} took ${ctx.durationMs}ms`))

const withMetrics = new Sirannon({
  driver,
  metrics: {
    onQueryComplete: m => histogram.observe(m.durationMs),
    onConnectionOpen: m => gauge.inc({ db: m.databaseId }),
    onCDCEvent: m => counter.inc({ table: m.table, op: m.operation }),
  },
})
```

Global hooks: `onBeforeQuery`, `onAfterQuery`, `onBeforeConnect`, `onDatabaseOpen`, `onDatabaseClose`. Register `onBeforeSubscribe` through the `hooks` constructor option. Substring matching is no SQL firewall, so pair hooks with an allow-list of known statements.

## Multi-tenant lifecycle

```ts
const sirannon = new Sirannon({
  driver,
  lifecycle: {
    autoOpen: { resolver: id => ({ path: `/data/tenants/${id}.db` }) },
    idleTimeout: 300_000,
    maxOpen: 50,
  },
})

const db = await sirannon.resolve('tenant-42')
```

## Server

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, { port: 9876 })
await server.listen()
```

The server runs SQL sent by clients, so read [Security](#security) before you expose it. Writes come in three shapes on both transports: `transaction` for several different statements that must succeed or fail together, `batch` for one statement over many parameter sets, and `load` for a large import that trades durability for speed.

Turn on `writerWorker` to run writes, checkpoints, loads, migrations, and backups on a worker thread so a disk flush never blocks the serving thread. Full durability still holds, because a write returns only after its flush completes:

```ts
const db = await sirannon.open('app', './data/app.db', {
  synchronous: 'full',
  writerWorker: { maxPendingWrites: 1024, writeTimeoutMs: 30_000, maxRestarts: 5 },
})
```

### HTTP routes

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/db/:id/query` | Execute a SELECT, returns `{ rows }` |
| `POST` | `/db/:id/execute` | Execute a mutation, returns `{ changes, lastInsertRowId }` |
| `POST` | `/db/:id/transaction` | Execute many statements atomically, returns `{ results }` |
| `POST` | `/db/:id/batch` | Apply one statement over many parameter sets, returns `{ results }` |
| `POST` | `/db/:id/load` | Bulk-load rows with relaxed durability, returns `{ rowsLoaded, changes }` |
| `GET` | `/db/:id/cluster` | Role, replication group, current primary, primary term, read endpoints, and health |
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check with per-database status |

### WebSocket messages

Connect to `ws://host:port/db/:id`. Every message carries a `type` and a client-chosen `id`, and every reply echoes that `id`.

| Inbound `type` | Fields | Reply |
| --- | --- | --- |
| `query` | `sql`, `params?` | `{ type: 'result', data: { rows } }` |
| `execute` | `sql`, `params?` | `{ type: 'result', data: { changes, lastInsertRowId } }` |
| `transaction` | `statements`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `batch` | `sql`, `paramsBatch`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `load` | `sql`, `paramsBatch`, `durability?`, `checkpoint?` | `{ type: 'result', data: { rowsLoaded, changes } }` |
| `subscribe` | `table`, `tables?`, `filter?`, `sinceSeq?`, `epoch?` | `{ type: 'subscribed', seq?, epoch?, resync? }` then `change` events |
| `unsubscribe` | - | `{ type: 'unsubscribed' }` |

Each transaction, batch, and load runs server-side in one transaction and replies once. The server never holds the write lock across a round-trip, so it accepts no interactive `BEGIN` ... `COMMIT` across messages.

Both transports round-trip every SQLite value through JSON. A blob crosses as `{ "__sirannon_blob": "<uppercase hex>" }` and an integer beyond the safe range as `{ "__sirannon_int": "<decimal string>" }`. The client SDK encodes and decodes these for you, so `BigInt` and `Uint8Array` values need no application code. The normative definition is in [`packages/spec/05-server.md`](../spec/05-server.md).

## Client SDK

```ts
import { SirannonClient } from '@delali/sirannon-db/client'

const client = new SirannonClient('http://localhost:9876', { transport: 'websocket', autoReconnect: true })
const db = client.database('app')

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
await db.execute('INSERT INTO users (name) VALUES (?)', ['Turing'])

const sub = await db.on('users').subscribe(event => console.log('User changed:', event))

sub.unsubscribe()
client.close()
```

Transactions use the HTTP transport:

```ts
const httpDb = new SirannonClient('http://localhost:9876', { transport: 'http' }).database('app')

await httpDb.transaction([
  { sql: 'UPDATE accounts SET balance = balance - 50 WHERE id = ?', params: [1] },
  { sql: 'UPDATE accounts SET balance = balance + 50 WHERE id = ?', params: [2] },
])
```

## Device sync

Device sync keeps an end-user device's local database and a server database in step, offline-first and both ways. A device syncs the whole database and holds no primary authority. The [device sync specification](../spec/08-device-sync.md) defines the wire protocol.

The routes are built into the server, so watch the tables you want to sync and start the server as usual:

```ts
const db = await sirannon.open('app', './data/app.db')
await db.watch('tasks')

await createServer(sirannon, { port: 9876 }).listen()
```

On the device, drive the loop with a `SyncController`:

```ts
import { SyncController } from '@delali/sirannon-db/client'

const sync = new SyncController(db, {
  url: 'https://api.example.com',
  databaseId: 'app',
  tables: ['tasks'],
  onChange: event => refreshView(event.table),
  onResyncRequired: () => warnBeforeWipe(),
  onSnapshotProgress: progress => showProgress(progress),
})

await sync.start()
const status = await sync.status()
```

- `start()` checks capabilities, reconciles the migration handshake, opens the live pull, and starts the push loop. A server that announces no `sync.stream-apply` is refused with `SYNC_UNSUPPORTED`. `pause()` keeps the cursors, `resume()` restarts the loops, and `stop()` ends them.
- The controller applies what it pulls, writing each server transaction and the pull cursor in one local transaction, so a device that stops part-way resumes from the last transaction it committed. `onChange` fires after that commit.
- Conflicts run through `resolver`, which defaults to last-write-wins on the HLC and accepts a delete whatever the timestamps say.
- A device acknowledges a sequence only once it has committed it, and the server pauses delivery to a device running more than `maxUnacknowledgedChanges` past its acknowledgement.
- A fresh device, or one too far behind to resume, replaces its whole database from a server snapshot. Local reads and writes fail with `SNAPSHOT_IN_PROGRESS` while that runs.
- Schema changes arrive through the migration handshake, never the change feed. The server withholds rows a migration wrote and refuses a stale device with `MIGRATION_REQUIRED`, and the controller then fetches, verifies, and applies the missing migrations. Share one migration set across your server, web, and mobile builds.
- A device idle past the retention window, 30 days by default, is evicted and resyncs from a snapshot.

## Distributed replication

<p align="center">
  <img src="../../docs/assets/replication-topology.svg" alt="Sirannon replication topology: application clients reach the primary and read replicas, the primary replicates to replicas over gRPC with mutual TLS, and an etcd coordinator tracks authority, leases, and the in-sync set." width="820">
</p>

One primary accepts writes and pushes changes to read replicas, which serve reads and forward writes when `writeForwarding` is on. Each node has its own SQLite file; Sirannon moves checksummed batches of changes over the replication transport and never shares one file over a network filesystem.

```ts
import { PrimaryReplicaTopology, ReplicationEngine } from '@delali/sirannon-db/replication'
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
```

A replica points at the primary's endpoints:

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

With `initialSync` on (the default), a new node pulls a full snapshot before it serves reads: the source streams schema and table data in batches with per-batch checksums, then a manifest, and the joiner moves through `pending` -> `syncing` -> `catching-up` -> `ready`, which you read from `engine.status().syncState`. For a database too large to transfer, copy the file and start from a known sequence with `initialSync: false` and `resumeFromSeq`.

Write concerns control how many replicas must acknowledge a write:

```ts
await engine.execute('INSERT INTO orders (id, total) VALUES (?, ?)', [1, 4999], {
  writeConcern: { level: 'majority', timeoutMs: 5000 },
})
```

Static mode returns after the local commit when you omit `writeConcern`; coordinator mode selects `'majority'`. Coordinator-mode majority counts the configured voting nodes, including the primary's own durable commit, so such a write survives automatic failover when only the failed primary is lost.

### Coordinator-backed failover

Coordinator mode stores primary authority, node sessions, group state, and the in-sync set in a `ClusterCoordinator`. The package includes an etcd adapter:

```ts
import { createEtcdCoordinator } from '@delali/sirannon-db/replication/coordinator/etcd'

const coordinator = createEtcdCoordinator({
  hosts: ['https://etcd-1.internal:2379', 'https://etcd-2.internal:2379'],
  keyPrefix: '/sirannon/orders',
  credentials: {
    rootCertificate: readFileSync('./certs/etcd-ca.crt'),
    privateKey: readFileSync('./certs/orders-node.key'),
    certChain: readFileSync('./certs/orders-node.crt'),
  },
})

const engine = new ReplicationEngine(db, writerConn, {
  nodeId: 'orders-node-a',
  topology: new PrimaryReplicaTopology('primary'),
  transport,
  transportConfig: { endpoints: ['orders-node-b.internal:4200', 'orders-node-c.internal:4200'] },
  changeTracker: tracker,
  snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
  writeForwarding: true,
  coordinator: {
    clusterId: 'commerce-production',
    groupId: 'orders',
    endpoint: 'https://orders-node-a.internal/db/orders',
    coordinator,
    votingDataBearingNodeIds: ['orders-node-a', 'orders-node-b', 'orders-node-c'],
    controller: true,
  },
})
```

Every coordinator-mode node needs a stable, persisted `nodeId`, and automatic write failover needs at least three voting data-bearing nodes, since fewer voters cannot prove majority authority after losing one. Production access requires HTTPS and an authenticated identity; the in-memory coordinator and `allowInsecure: true` are for tests.

### Conflict resolution

A receiver that finds the target row already present passes the local and incoming versions to the configured resolver.

| Strategy | Class | Behaviour |
| --- | --- | --- |
| Last-Writer-Wins | `LWWResolver` | Accepts a remote delete whatever the timestamps say, so a delete wins over a concurrent update. Otherwise takes the higher HLC timestamp and breaks ties by node ID. |
| Field-Level Merge | `FieldMergeResolver` | Merges non-overlapping columns and uses per-column HLC metadata for overlapping ones. Falls back to whole-row LWW without column metadata. |
| Primary Wins | `PrimaryWinsResolver` | Takes the version authored by a configured primary node ID, and falls back to LWW otherwise. |

Write a custom resolver as a class with a `resolve(ctx: ConflictContext): ConflictResolution` method.

### Transports

| Transport | Import | Use case |
| --- | --- | --- |
| gRPC | `@delali/sirannon-db/transport/grpc` | Production multi-node replication over the network with TLS |
| In-Memory | `@delali/sirannon-db/transport/memory` | Testing and single-process multi-node scenarios |
| Custom | Build your own | Anything satisfying the `ReplicationTransport` interface |

The client `Transport` interface carries application queries, writes, and CDC subscriptions over HTTP or WebSocket. `ReplicationTransport` carries change batches, acknowledgements, write forwarding, and first sync between nodes. `WebSocketTransport` conforms to the first and never the second.

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

## Configuration reference

### `SirannonOptions`

| Option | Type | Required | Description |
| --- | --- | --- | --- |
| `driver` | `SQLiteDriver` | Yes | The SQLite driver adapter to use |
| `hooks` | `HookConfig` | No | Before/after hooks for queries, connections, subscriptions |
| `metrics` | `MetricsConfig` | No | Callbacks for query timing, connection events, CDC activity |
| `lifecycle` | `LifecycleConfig` | No | Auto-open resolver, idle timeout, max open databases |
| `migrations` | `MigrationSource` | No | Migration set, or a function returning it, applied to every writable database before it registers |

### `DatabaseOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `readOnly` | `boolean` | `false` | Open in read-only mode |
| `readPoolSize` | `number` | `4` | Number of read connections |
| `walMode` | `boolean` | `true` | Enable WAL mode |
| `synchronous` | `'off' \| 'normal' \| 'full' \| 'extra'` | `'normal'` | Writer durability (`PRAGMA synchronous`); a bulk load restores this level when it finishes |
| `cdcPollInterval` | `number` | `50` | CDC polling interval in ms |
| `cdcRetention` | `number` | `3_600_000` | CDC retention period in ms |
| `writerWorker` | `boolean \| WriterWorkerOptions` | `false` | Run writes on a dedicated worker thread so disk flushes never block the serving thread |

`WriterWorkerOptions` accepts `maxPendingWrites` (in-flight writes before the server sheds load), `writeTimeoutMs` (per-operation deadline), and `maxRestarts` (respawns allowed after the worker crashes).

### `ServerOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `host` | `string` | `'127.0.0.1'` | Bind address |
| `port` | `number` | `9876` | Listen port |
| `cors` | `boolean \| CorsOptions` | `false` | CORS configuration |
| `maxBodyBytes` | `number` | `1_048_576` | Maximum HTTP body and WebSocket message size; a positive integer no larger than `4_294_967_295` |
| `maxWebSocketBackpressureBytes` | `number` | larger of `16_777_216` and `maxBodyBytes` | Bytes buffered per connection before the server closes it so the client reconnects instead of losing a frame |
| `cdcRetentionMs` | `number` | `3_600_000` | How long change events are retained, bounding change-log growth and how far back `sinceSeq` can resume |
| `maxUnacknowledgedChanges` | `number` | `1_000` | How far a device may run past its acknowledged sequence before delivery pauses; a larger transaction still arrives whole |
| `onRequest` | `OnRequestHook` | - | Middleware hook for auth, rate limiting, and request validation |

### `ClientOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `transport` | `'websocket' \| 'http'` | `'websocket'` | Transport protocol |
| `headers` | `Record<string, string>` | - | Custom HTTP headers; browser WebSocket handshakes do not use this option |
| `webSocketProtocols` | `string \| string[]` | - | WebSocket subprotocols sent during the upgrade handshake |
| `autoReconnect` | `boolean` | `true` | Reconnect on WebSocket disconnect |
| `reconnectInterval` | `number` | `1000` | Reconnect delay in ms |

### `SyncControllerOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `url` | `string` | required | Server base URL |
| `databaseId` | `string` | required | Database to sync against |
| `tables` | `readonly string[]` | required | Tables the device syncs |
| `headers` | `Record<string, string>` | - | Headers sent on push, snapshot, and migration requests |
| `batchSize` | `number` | `100` | Changes per push request |
| `pushIntervalMs` | `number` | `1_000` | Push loop interval, also the base for retry backoff |
| `ackIntervalMs` | `number` | `2_000` | How often the device acknowledges applied changes |
| `maxPushRetryDelayMs` | `number` | `30_000` | Ceiling for push and pull retry backoff |
| `requestTimeout` | `number` | `30_000` | HTTP request timeout in ms |
| `autoResync` | `boolean` | `true` | Download a snapshot on start, on a server resync signal, and after a failed download |
| `snapshotRetryDelayMs` | `number` | `5_000` | First delay before retrying a failed snapshot |
| `maxSnapshotRetryDelayMs` | `number` | `300_000` | Ceiling for snapshot retry backoff |
| `snapshotPageSize` | `number` | `500` | Rows per snapshot page |
| `immediateAckAfterChanges` | `number` | half the server's window | Outstanding changes that trigger an immediate acknowledgement |
| `resolver` | `ConflictResolver \| ((table: string) => ConflictResolver)` | `LWWResolver` | Conflict resolution for pulled changes |
| `onChange` | `(event: ChangeEvent) => void` | - | Called for each pulled change after it commits locally |
| `onResyncRequired` | `() => void` | - | Called before a snapshot replaces local data |
| `onSnapshotProgress` | `(progress: SnapshotProgress) => void` | - | Table and row progress during a snapshot |

### `ReplicationOptions`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `nodeId` | `string` | auto-generated in static mode | Unique node identifier; coordinator mode requires a stable, persisted value |
| `topology` | `Topology` | required | `PrimaryReplicaTopology` |
| `transport` | `ReplicationTransport` | required | Transport for inter-node communication |
| `transportConfig` | `TransportConfig` | `{}` | Peer endpoints and transport metadata |
| `writeForwarding` | `boolean` | `false` | Forward writes from replicas to the primary |
| `defaultConflictResolver` | `ConflictResolver` | `LWWResolver` | Default conflict resolution strategy |
| `conflictResolvers` | `Record<string, ConflictResolver>` | - | Per-table conflict resolution overrides |
| `batchSize` | `number` | `100` | Changes per replication batch |
| `batchIntervalMs` | `number` | `100` | Sender loop interval in ms |
| `maxClockDriftMs` | `number` | `60000` | Maximum tolerated HLC drift before rejecting a batch |
| `maxPendingBatches` | `number` | `10` | In-flight batches per peer before backpressure |
| `maxBatchChanges` | `number` | `1000` | Maximum accepted changes in one inbound batch |
| `ackTimeoutMs` | `number` | `5000` | Replication batch ack timeout |
| `initialSync` | `boolean` | `true` | Pull a full snapshot when joining a cluster |
| `syncBatchSize` | `number` | `10000` | Rows per sync batch during first sync |
| `maxConcurrentSyncs` | `number` | `2` | Maximum simultaneous sync sessions on the source |
| `maxSyncDurationMs` | `number` | `1800000` | Source aborts sync after this duration |
| `maxSyncLagBeforeReady` | `number` | `100` | Catch-up lag threshold, in sequences, to reach ready |
| `syncAckTimeoutMs` | `number` | `30000` | Per-batch ack timeout during sync |
| `catchUpDeadlineMs` | `number` | `600000` | Maximum time in catch-up before transitioning to ready |
| `resumeFromSeq` | `bigint` | - | Start replication from a specific sequence (out-of-band sync) |
| `snapshotConnectionFactory` | `() => Promise<SQLiteConnection>` | - | Factory for read-only connections used during sync serving |
| `changeTracker` | `ChangeTracker` | - | CDC trigger manager, required for first sync |
| `flowControl` | `{ maxLagSeconds?, onLagExceeded? }` | - | Replication lag monitoring callbacks |
| `onBeforeForwardedQuery` | `(sql, params?) => void` | - | Validation hook called before the primary runs each forwarded statement |
| `coordinator` | `CoordinatorModeConfig` | - | Enables coordinator-backed authority and failover |

### `CoordinatorModeConfig`

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `clusterId` | `string` | required | Coordinator namespace for the cluster |
| `groupId` | `string` | required | Replication group containing copies of one database |
| `endpoint` | `string` | - | Application endpoint advertised for client discovery |
| `votingDataBearingNodeIds` | `string[]` | - | Voter set used to create an unregistered group and calculate write concerns |
| `coordinator` | `ClusterCoordinator` | required | Coordinator adapter, such as the etcd adapter |
| `sessionTtlMs` | `number` | `10000` | Node-session lease lifetime |
| `controller` | `boolean \| CoordinatorControllerConfig` | enabled | Enables the controller loop or configures its lease holder, TTL, and tick interval |
| `compatibility` | `CoordinatorCompatibilityMetadata` | - | Package, specification, and protocol versions checked before promotion |

`CoordinatorControllerConfig` accepts `enabled`, `holderId`, `leaseTtlMs` (default 10,000 ms), and `tickIntervalMs` (default 1,000 ms).

### `TransportConfig`

| Option | Type | Description |
| --- | --- | --- |
| `endpoints` | `string[]` | Peer addresses used to establish replication connections |
| `localRole` | `'primary' \| 'replica'` | Local topology role; `ReplicationEngine` supplies this value |
| `groupId` | `string` | Replication group carried in coordinator-mode handshakes |
| `primaryTerm` | `bigint` | Current fencing term, supplied from coordinator state |
| `protocolVersion` | `string` | Replication protocol version advertised to peers |
| `metadata` | `Record<string, unknown>` | Optional custom transport metadata |

`ReplicationEngine.start()` fills in role, group, term, and protocol version. Set them yourself only when you use a `ReplicationTransport` without the engine.

## Errors

Every error extends `SirannonError` with a machine-readable `code`.

```ts
import { QueryError } from '@delali/sirannon-db'

try {
  await db.execute('INSERT INTO users (id) VALUES (?)', [1])
} catch (err) {
  if (err instanceof QueryError) console.error(`SQL failed [${err.code}]: ${err.message}`, err.sql)
}
```

| Error | Code | When |
| --- | --- | --- |
| `DatabaseNotFoundError` | `DATABASE_NOT_FOUND` | Database ID not in registry |
| `DatabaseAlreadyExistsError` | `DATABASE_ALREADY_EXISTS` | Duplicate database ID |
| `ReadOnlyError` | `READ_ONLY` | Write attempted on a read-only database |
| `QueryError` | `QUERY_ERROR` | SQL execution failure |
| `TransactionError` | `TRANSACTION_ERROR` | Transaction commit or rollback failure |
| `MigrationError` | `MIGRATION_ERROR` | Migration step failure |
| `HookDeniedError` | `HOOK_DENIED` | Before-hook rejected the operation |
| `CDCError` | `CDC_ERROR` | Change tracking pipeline failure |
| `BackupError` | `BACKUP_ERROR` | Backup operation failure |
| `ConnectionPoolError` | `CONNECTION_POOL_ERROR` | Pool closed or misconfigured |
| `MaxDatabasesError` | `MAX_DATABASES` | Capacity limit reached |
| `ExtensionError` | `EXTENSION_ERROR` | SQLite extension load failure |

| Code | When | Retry? |
| --- | --- | --- |
| `WRITE_OVERLOADED` | More writes pending than `maxPendingWrites` allows, or a queued write was shed when an earlier deadline expired. HTTP returns 503 with `Retry-After`. | Yes; the write never applied |
| `WRITER_WORKER_TIMEOUT` | An in-flight operation missed `writeTimeoutMs` plus the grace window | Only after reconciling; the outcome is indeterminate |
| `WRITER_WORKER_UNSUPPORTED` | `writerWorker` enabled on a driver with no worker entry; the database refuses to open | No; change driver or option |
| `INVALID_WRITER_WORKER` | A `writerWorker` value is out of range | No; fix the configuration |
| `PAYLOAD_TOO_LARGE` | A request or message exceeded `maxBodyBytes` | No; send less |
| `INVALID_MAX_BODY_BYTES` | `maxBodyBytes` is not a positive integer within `4_294_967_295` | No; fix the configuration |
| `INVALID_WS_BACKPRESSURE` | `maxWebSocketBackpressureBytes` is out of range | No; fix the configuration |
| `INVALID_DURABILITY` | A load passed a `durability` other than `'off'` or `'normal'` | No; fix the call |
| `DURABILITY_RESTORE_FAILED` | The load committed, then the writer failed before durability was restored | No; the load succeeded, do not re-run it |
| `BULK_LOAD_UNSUPPORTED` | The resolved execution target implements no bulk load | No |

| Error | Code | When |
| --- | --- | --- |
| `ReplicationError` | `REPLICATION_ERROR` | Base class for replication failures |
| `SyncError` | `SYNC_ERROR` | First sync failures: node not ready, timeout, integrity mismatch |
| `ConflictError` | `CONFLICT_ERROR` | Unresolvable write conflict |
| `TransportError` | `TRANSPORT_ERROR` | Inter-node communication failure |
| `BatchValidationError` | `BATCH_VALIDATION_ERROR` | Checksum mismatch, clock drift, or oversized batch |
| `TopologyError` | `TOPOLOGY_ERROR` | Write on a read-only node without forwarding |
| `WriteConcernError` | `WRITE_CONCERN_ERROR` | Quorum not reached within the timeout |

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
