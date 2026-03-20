# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Turn any SQLite database into a networked data layer with real-time subscriptions. One library gives you connection pooling, change data capture, migrations, scheduled backups, and a client SDK that talks over HTTP or WebSocket.

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

## Security

Sirannon-db is designed to be secure by default in its core operations. This section covers what the library handles for you and what you're responsible for when deploying to production.

### Built-in protections

- **Parameterized queries** - All SQL execution uses parameter binding through the driver layer, preventing SQL injection. User input never touches query strings directly.
- **Identifier validation** - CDC table and column names are validated against a strict allowlist regex (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`), and identifiers are escaped with double-quote wrapping.
- **Path traversal prevention** - Migration and backup paths reject null bytes, `..` segments, and control characters before any filesystem access.
- **Request size limits** - HTTP bodies and WebSocket payloads are capped at 1 MB, preventing memory exhaustion from oversized requests.
- **Error isolation** - Errors returned to clients contain a machine-readable code and message. Stack traces and internal details are never leaked.
- **Connection isolation** - Read and write operations use separate connection pools. Read-only databases enforce immutability at the connection level.

### Authentication and authorization

The server accepts arbitrary SQL from clients. When you expose it beyond localhost, always use the `onRequest` hook to authenticate and authorize requests.

```ts
const server = createServer(sirannon, {
  port: 9876,
  onRequest: ({ headers, path, method, remoteAddress }) => {
    if (headers.authorization !== `Bearer ${process.env.API_TOKEN}`) {
      return { status: 401, code: 'UNAUTHORIZED', message: 'Invalid or missing token' }
    }
  },
})
```

The hook runs before every database route (HTTP and WebSocket upgrade). Return `void` to allow the request, or return a `{ status, code, message }` object to deny it. Health endpoints (`/health`, `/health/ready`) bypass this hook.

### TLS and transport security

> **Warning:** The built-in server binds plain HTTP and WebSocket without TLS. When you serve traffic outside a trusted network, terminate TLS upstream with a reverse proxy (nginx, Caddy, a cloud load balancer) or your clients' bearer tokens and query payloads will travel in cleartext.

Once TLS is in place, update your client URLs to use `https://` and `wss://`:

```ts
const client = new SirannonClient('https://db.example.com', {
  transport: 'websocket',
  headers: { Authorization: `Bearer ${token}` },
})
```

### CORS

CORS is disabled by default. Enable it only if browser clients need direct access, and restrict origins to trusted domains:

```ts
const server = createServer(sirannon, {
  port: 9876,
  cors: {
    origin: ['https://app.example.com'],
  },
})
```

Passing `cors: true` allows all origins, which is fine for local development but should be avoided in production.

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
| `headers` | `Record<string, string>` | - | Custom HTTP headers |
| `autoReconnect` | `boolean` | `true` | Reconnect on WebSocket disconnect |
| `reconnectInterval` | `number` | `1000` | Reconnect delay in ms |

## Examples

Self-contained example projects live in [`examples/`](examples/) and cover every runtime target:

| Example | Runtime | Driver | What it demonstrates |
| --- | --- | --- | --- |
| [`node-better-sqlite3`](examples/node-better-sqlite3/) | Node.js | better-sqlite3 | All core features: schema, migrations, CRUD, transactions, CDC, connection pools, metrics, multi-tenant, hooks, backup, shutdown |
| [`node-native`](examples/node-native/) | Node.js >= 22 | built-in `node:sqlite` | Same features as above using the zero-dependency Node driver |
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
# Node.js with better-sqlite3
cd packages/ts/examples/node-better-sqlite3
pnpm start

# Node.js with built-in sqlite
cd packages/ts/examples/node-native
pnpm start

# Browser with wa-sqlite (opens Vite dev server)
cd packages/ts/examples/web-wa-sqlite
pnpm dev

# Client-server (starts both Sirannon server and Vite client)
cd packages/ts/examples/web-client
pnpm start
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
