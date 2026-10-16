# Core engine

The core entry point, `@delali/sirannon-db`, holds the database registry, the connection pools, and every operation on a local SQLite file. The [package README](../packages/ts/README.md) covers installation, the first query, and change data capture, while this guide covers the rest.

## Bulk load

`bulkLoad` executes the whole batch in one transaction under relaxed durability, then restores the configured level. Use it for imports that you can repeat after a crash.

```ts
const summary = await db.bulkLoad('INSERT INTO events (id, payload) VALUES (?, ?)', rows, { durability: 'off' })
```

On the client, `loadAll` splits an iterable into batches and checkpoints the WAL once at the end:

```ts
const summary = await db.loadAll('INSERT INTO events (id, payload) VALUES (?, ?)', rowStream, {
  batchSize: 5000,
  durability: 'off',
})
```

## Live queries

`db.live` returns a query result that Sirannon updates from change events, so a view re-renders without polling or re-reading the table:

```ts
const orders = await db.live<{ id: number; total: number }>(
  'SELECT id, total FROM orders WHERE status = ? ORDER BY id',
  ['pending'],
)

orders.subscribe(() => render(orders.getState()))
```

The [live queries guide](live-queries.md) covers the update kinds, the three cases that trigger a second read, and the statements that a live query can maintain.

## Migrations

Sirannon applies each numbered migration file once inside a transaction and records it in `_sirannon_migrations` with a checksum. Versions must be integers from 1 to 2,147,483,647 so that they fit `PRAGMA user_version`, which Sirannon sets to the highest applied version.

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

Pass migration objects directly when your migrations are not in files:

```ts
await db.migrate([
  { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)', down: 'DROP TABLE users' },
])
```

A bundler such as Vite can inline `.sql` files as strings, so a bundled app can build the same set without filesystem access:

```ts
import { migrationsFromFiles } from '@delali/sirannon-db'

const files = import.meta.glob('./migrations/*.sql', { query: '?raw', import: 'default', eager: true })
await db.migrate(migrationsFromFiles(files))
```

A baseline replaces a long migration history with one file. Write one file that holds the full schema, and mark the highest version that it replaces. Sirannon applies the baseline and every later migration to a fresh database, while a database that already has a history goes on using that history:

```ts
const migrations = loadMigrations('./migrations', { baseline: { version: 701, through: 700 } })
```

Declare the set on the registry to migrate every database that it opens, including a tenant database that it resolves on first use:

```ts
const TENANT_ID = /^[\w-]{1,64}$/

const sirannon = new Sirannon({
  driver,
  migrations: () => loadMigrations('./migrations'),
  lifecycle: {
    autoOpen: { resolver: id => (TENANT_ID.test(id) ? { path: `/data/tenants/${id}.db` } : undefined) },
  },
})

const db = await sirannon.resolve('tenant-42')
```

## Backups

`backup()` copies a database to a file while it stays open for reads and writes, because SQLite copies the pages in steps and a write can commit in the gap between two of them:

```ts
await db.backup('./backups/snapshot.db')
```

The [backups guide](backups.md) covers the cron schedule, sending a copy to storage that you supply, the chain of changes that follows each full copy, and restoring from a moment that you name.

## Hooks and metrics

A before-hook refuses the operation by throwing. Sirannon calls an after-query hook whether the statement succeeds or fails, and when it fails, `ctx.error` holds the error that the caller receives.

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

The registry has five methods that register a global hook: `onBeforeQuery`, `onAfterQuery`, `onBeforeConnect`, `onDatabaseOpen`, and `onDatabaseClose`. Register `onBeforeSubscribe`, `onBeforeSnapshot`, and `onBeforePush` through the `hooks` constructor option. The server calls the first once for each table that a client subscribes to, the second once for each table that a snapshot reads, and the third once for each table that a pushed device batch writes to. Each of them receives the identity that the `authenticate` hook returns for the request, and throwing from it refuses the request. A substring match on SQL misses a statement that someone rewrites to avoid it, so pair hooks with an allow-list of known statements.

### Rows read and rows written

When you host many tenants on one machine, you can bill each tenant by the rows that SQLite reads and writes for its statements, because those counts stay the same however busy the machine is. Install `@delali/sirannon-sqlite` in the same project as `@delali/sirannon-db`, and pass `rowCounts: true` to the better-sqlite3 driver:

```bash
pnpm add -E @delali/sirannon-sqlite
```

```ts
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const usage = new Map<string, { rowsRead: number; rowsWritten: number }>()

const metered = new Sirannon({
  driver: betterSqlite3({ rowCounts: true }),
  metrics: {
    onQueryComplete: ({ databaseId, rowsRead = 0, rowsWritten = 0 }) => {
      const tenant = usage.get(databaseId) ?? { rowsRead: 0, rowsWritten: 0 }
      tenant.rowsRead += rowsRead
      tenant.rowsWritten += rowsWritten
      usage.set(databaseId, tenant)
    },
  },
})
```

Sirannon then sets `rowsRead` and `rowsWritten` on every statement that it reports to `onQueryComplete`, including each statement inside a transaction and each statement that fails. For a failed statement, Sirannon reports the rows that SQLite read before the failure, so a tenant pays for a scan that ends in an error. When a write fails with `WRITER_WORKER_TIMEOUT`, SQLite can still finish it, so Sirannon holds that statement's report until the writer worker answers and then reports the rows that SQLite read and wrote, with `error: true`. A tenant therefore pays for a write that it slows past the deadline on purpose. Sirannon leaves both counts out only when the writer worker exits, crashes, or closes before it answers, because Sirannon cannot then tell how much of the statement SQLite executed. Sirannon reports 5,000 rows read for a scan of 5,000 rows that returns five of them, and one row read for a lookup by primary key. Each row that a write inserts, updates, or deletes counts as one row written, and so does each index entry that the write inserts. Both counts include the work of every trigger that the statement fires, including foreign-key actions. Each count is exact up to 2^53 - 1 rows for one statement, which is the largest integer that a JavaScript number holds exactly.

Sirannon's build of SQLite leaves the work of Sirannon's own change-capture triggers out of both counts. However, SQLite takes a second pass over the changed rows of some statements on any table that has a trigger, so those statements on a watched table read more rows than they read on the same table without change capture. A multi-row `UPDATE` reads each updated row once or twice more, and a `DELETE` that can remove more than one row reads each deleted row once more. A `DELETE` with no `WHERE` clause reads each row twice, while on a table without triggers it reads none, because SQLite then empties the table without visiting its rows. An `INSERT ... SELECT` reads each inserted row once more, because SQLite first copies the selected rows into a temporary table. Rows written stay the same, and so do the rows read by a statement that changes one row through its primary key.

Sirannon also reports each read that a live query makes to `onQueryComplete` with `source: 'live'`, whether the live query reads its first rows, reads a second time, or checks the rows that a transaction changed. It reports no counts for change delivery, device sync, backups, or the changes that one node applies from another. With [replication](replication.md), the primary reports every write that it executes, including each write that a replica forwards to it, and each replica reports the reads that it serves. The server keeps both counts out of every response that it sends to a client, so only your `onQueryComplete` callback receives them.

With `rowCounts: true`, the driver opens every database through `@delali/sirannon-sqlite`. When you install that package, npm also installs the build for your platform, which is better-sqlite3 compiled against Sirannon's own build of SQLite. `open` throws `ROW_COUNTS_UNSUPPORTED` when the package or your platform's build is missing, so a metered host executes only the statements that it can bill. On a platform without a published build, compile one with `native/sqlite/build.mjs` from the Sirannon repository and pass its path as `sqliteBindingPath`. The other drivers count no rows, so Sirannon leaves both fields out of their metrics. Your callback can therefore tell a statement that read no rows, where `rowsRead` is 0, from a statement that no driver counted, where `rowsRead` is absent.

## Multi-tenant lifecycle

```ts
const TENANT_ID = /^[\w-]{1,64}$/

const sirannon = new Sirannon({
  driver,
  lifecycle: {
    autoOpen: { resolver: id => (TENANT_ID.test(id) ? { path: `/data/tenants/${id}.db` } : undefined) },
    idleTimeout: 300_000,
    maxOpen: 50,
  },
})

const db = await sirannon.resolve('tenant-42')
```

The server percent-decodes the identifier in each request path, so a caller can send an identifier that holds `/` or `..`. Check the identifier against a pattern before you build a file path from it. When the resolver returns undefined, the server answers `404 DATABASE_NOT_FOUND`.

The option tables for every constructor above are in the [configuration reference](configuration.md).
