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
const sirannon = new Sirannon({
  driver,
  migrations: () => loadMigrations('./migrations'),
  lifecycle: { autoOpen: { resolver: id => ({ path: `/data/tenants/${id}.db` }) } },
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

A before-hook refuses the operation by throwing.

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

The option tables for every constructor above are in the [configuration reference](configuration.md).
