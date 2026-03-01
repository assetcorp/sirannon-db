# sirannon-db

Turn any SQLite database into a networked data layer with real-time subscriptions. One library gives you connection pooling, change data capture, migrations, scheduled backups, and a client SDK that talks over HTTP or WebSocket.

> *sirannon* means 'gate-stream' in Sindarin.

## Install

```bash
pnpm add @delali/sirannon-db
```

Requires Node.js >= 22.

## Quick start

```ts
import { Sirannon } from '@delali/sirannon-db'

const sirannon = new Sirannon()
const db = sirannon.open('app', './data/app.db')

db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = db.query<{ id: number; name: string }>('SELECT * FROM users')
```

## Three entry points

The package ships three independent exports so you only bundle what you need:

| Import | What you get |
|---|---|
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/server` | HTTP + WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Browser/Node.js client SDK with auto-reconnect and subscription restore |

## Core features

### Queries and transactions

```ts
const row = db.queryOne<{ count: number }>('SELECT count(*) as count FROM users')

const result = db.execute(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Grace', 'grace@example.com'],
)
// result.changes === 1, result.lastInsertRowId === 2

db.executeBatch('INSERT INTO tags (label) VALUES (?)', [
  ['typescript'],
  ['sqlite'],
  ['realtime'],
])

const total = db.transaction(tx => {
  tx.execute('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1])
  tx.execute('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2])
  const [row] = tx.query<{ balance: number }>('SELECT balance FROM accounts WHERE id = ?', [2])
  return row
})
```

Statements are cached in an LRU pool (capacity 128) so repeated queries skip the prepare step.

### Connection pooling

Every database opens with 1 dedicated write connection and N read connections (default 4). WAL mode is enabled by default, allowing concurrent reads during writes.

```ts
const db = sirannon.open('analytics', './data/analytics.db', {
  readPoolSize: 8,
  walMode: true,
})
```

### Change data capture (CDC)

Watch tables for INSERT, UPDATE, and DELETE events in real time. The CDC system installs SQLite triggers that record changes into a tracking table, then polls at a configurable interval.

```ts
db.watch('orders')

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
db.unwatch('orders')
```

### Migrations

Place numbered SQL files in a directory and run them in order. Each migration runs inside a transaction and is tracked in a `_sirannon_migrations` table so it only applies once.

```
migrations/
  001_create_users.sql
  002_add_email_index.sql
  003_create_orders.sql
```

```ts
const result = db.migrate('./migrations')
// result.applied: files that ran this time
// result.skipped: number of files already applied
```

### Backups

One-shot backups use `VACUUM INTO` for a consistent snapshot. Scheduled backups run on a cron expression with automatic file rotation.

```ts
db.backup('./backups/snapshot.db')

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

### Metrics

Plug in callbacks to collect query timing, connection events, and CDC activity.

```ts
const sirannon = new Sirannon({
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
  lifecycle: {
    autoOpen: {
      resolver: id => ({ path: `/data/tenants/${id}.db` }),
    },
    idleTimeout: 300_000, // close after 5 minutes of inactivity
    maxOpen: 50,          // evict least-recently-used when full
  },
})

// Databases resolve on first access:
const db = sirannon.get('tenant-42') // opens /data/tenants/tenant-42.db
```

## Server

Expose any `Sirannon` instance over HTTP and WebSocket with a single function call. The server uses uWebSockets.js for high throughput.

```ts
import { Sirannon } from '@delali/sirannon-db'
import { createServer } from '@delali/sirannon-db/server'

const sirannon = new Sirannon()
sirannon.open('app', './data/app.db')

const server = createServer(sirannon, {
  port: 9876,
  cors: true,
  auth: async ({ headers }) => {
    return headers.authorization === `Bearer ${process.env.API_TOKEN}`
  },
})

await server.listen()
```

### HTTP routes

| Method | Path | Description |
|---|---|---|
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

const sub = await db
  .on('users')
  .filter({ role: 'admin' })
  .subscribe(event => console.log('Admin changed:', event))

// Cleanup:
sub.unsubscribe()
client.close()
```

Transactions require HTTP transport:

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

## Error handling

All errors extend `SirannonError` with a machine-readable `code` property:

| Error | Code | When |
|---|---|---|
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
  db.execute('INSERT INTO users (id) VALUES (?)', [1])
} catch (err) {
  if (err instanceof QueryError) {
    console.error(`SQL failed [${err.code}]: ${err.message}`)
    console.error(`Statement: ${err.sql}`)
  }
}
```

## Configuration reference

### `DatabaseOptions`

| Option | Type | Default | Description |
|---|---|---|---|
| `readOnly` | `boolean` | `false` | Open in read-only mode |
| `readPoolSize` | `number` | `4` | Number of read connections |
| `walMode` | `boolean` | `true` | Enable WAL mode |
| `cdcPollInterval` | `number` | `50` | CDC polling interval in ms |
| `cdcRetention` | `number` | `3_600_000` | CDC retention period in ms (1 hour) |

### `SirannonOptions`

| Option | Type | Description |
|---|---|---|
| `hooks` | `HookConfig` | Before/after hooks for queries, connections, subscriptions |
| `metrics` | `MetricsConfig` | Callbacks for query timing, connection events, CDC activity |
| `lifecycle` | `LifecycleConfig` | Auto-open resolver, idle timeout, max open databases |

### `ServerOptions`

| Option | Type | Default | Description |
|---|---|---|---|
| `host` | `string` | `'0.0.0.0'` | Bind address |
| `port` | `number` | `9876` | Listen port |
| `cors` | `boolean \| CorsOptions` | `false` | CORS configuration |
| `auth` | `(req) => boolean \| Promise<boolean>` | - | Request authentication function |

### `ClientOptions`

| Option | Type | Default | Description |
|---|---|---|---|
| `transport` | `'websocket' \| 'http'` | `'websocket'` | Transport protocol |
| `headers` | `Record<string, string>` | - | Custom HTTP headers |
| `autoReconnect` | `boolean` | `true` | Reconnect on WebSocket disconnect |
| `reconnectInterval` | `number` | `1000` | Reconnect delay in ms |

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
