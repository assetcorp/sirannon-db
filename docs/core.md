# Core engine

The core entry point, `@delali/sirannon-db`, holds the database registry, the connection pools, and everything that runs against a local SQLite file. The [package README](../packages/ts/README.md) covers installation, the first query, and change data capture; this guide covers the rest.

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

## Live queries

`db.live` returns a query result that change events keep current, so a view re-renders without polling and without re-reading the table:

```ts
const orders = await db.live<{ id: number; total: number }>(
  'SELECT id, total FROM orders WHERE status = ? ORDER BY id',
  ['pending'],
)

orders.subscribe(() => render(orders.getState()))
```

The [live queries guide](live-queries.md) covers the update kinds, the three cases that trigger a second read, and the statements a live query maintains.

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

The option tables for every constructor above are in the [configuration reference](configuration.md).
