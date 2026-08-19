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

`backup()` writes a copy to a local file. The database stays open for reads and writes throughout, because SQLite moves the pages in steps and a write runs in the gap between two of them.

```ts
await db.backup('./backups/snapshot.db')
```

`scheduleBackup()` repeats that on a cron schedule and keeps a bounded number of files:

```ts
db.scheduleBackup({
  cron: '0 */6 * * *',
  destDir: './backups',
  maxFiles: 10,
  timezone: 'America/New_York',
  onError: err => console.error('Backup failed:', err),
})
```

Both of those write to local disk. `backupTo()` sends the copy to storage you supply instead:

```ts
const report = await db.backupTo({ destination: s3Destination })

report.destinationName   // the name your storage now holds the copy under
report.bytesWritten
report.durationMs
```

### Supplying a destination

Sirannon carries no storage client, so you write three functions and it calls them. It splits every backup into fixed-size pieces, 16 MiB by default, and numbers them:

```ts
import type { BackupDestination } from '@delali/sirannon-db'

const s3Destination: BackupDestination = {
  async writePiece(name, index, bytes) {
    await s3.send(new PutObjectCommand({ Bucket, Key: `${name}/${index}`, Body: bytes }))
  },
  async readPiece(name, index) {
    const object = await s3.send(new GetObjectCommand({ Bucket, Key: `${name}/${index}` }))
    return new Uint8Array(await object.Body.transformToByteArray())
  },
  async listPieces(name) {
    const listed = await s3.send(new ListObjectsV2Command({ Bucket, Prefix: `${name}/` }))
    return (listed.Contents ?? []).map(object => ({
      index: Number(object.Key.split('/').pop()),
      byteLength: object.Size,
    }))
  },
}
```

Sirannon relies on three properties here. Pieces arrive in any order, since SQLite writes page one last, so nothing in your code may assume piece 0 comes first. A second write to the same name and index has to replace the piece already there, because a run that stops part-way through repeats its last write when it resumes. And `listPieces` answers for the one name it receives, returning an empty list where you hold nothing under that name.

## Incremental backups

Every backup above copies the whole database, however little of it changed since the last run. On a large database the cost of that mounts quickly.

The `backups` option copies the whole database once, then sends only what changed since the previous run. A 1 TB database that changed by 200 MB in a day uploads 200 MB.

```ts
const db = await sirannon.open('main', './data/main.db', {
  backups: {
    destination,
    intervalMs: 60_000,
    fullCopyIntervalMs: 24 * 60 * 60 * 1000,
    onError: err => pageOnCall(err),
  },
})
```

`destination` is the same object `backupTo` takes, so a destination you have already written works here unchanged. See [Supplying a destination](#supplying-a-destination).

Set `intervalMs` to how much work you can afford to lose, in milliseconds. At the default of one minute, a machine you lose takes up to a minute of writes with it.

### What gets stored

Sirannon writes one full copy, then a small file per interval holding the changes after it. Those files belong together: to rebuild the database as of some moment, you need the full copy plus every change file up to that moment, applied in order. That set is a **chain**. Sirannon records which files make up each one, so you never have to work it out from filenames.

Sirannon starts a new chain on a schedule, once a day by default, which `fullCopyIntervalMs` sets. Without that, a chain would grow all year and a restore would have to replay every file in it.

### Before you turn it on

While this option is on, Sirannon takes over one piece of SQLite housekeeping: trimming the log file SQLite keeps beside your database. Sirannon trims it immediately after each capture instead, which keeps it small.

Plan for what that costs when it stops. A cycle that fails while your app keeps writing lets the log grow until it fills the disk. A destination that stops accepting writes does that, and so do credentials that expire overnight. `onError` is your only warning, so treat anything arriving there as urgent.

Sirannon also stages each capture beside your database file before sending it, so leave a little headroom on that volume. `stagingDir` moves it elsewhere.

### Checking on the cycle

```ts
await db.captureBackupChanges()   // run one now instead of waiting for the interval
await db.backupChain()            // every chain at the destination, newest first
```

### Working out what a restore needs

Sirannon tells you which files to fetch and in what order:

```ts
const plan = await db.backupRestorePlan(Date.parse('2026-08-18T09:00:00Z'))

plan.base.name    // the full copy to start from
plan.changes      // the change files to apply, in order
plan.restoresTo   // the moment you would actually reach
```

`restoresTo` is the last capture at or before the time you asked for, so at a one-minute interval you reach within a minute of it. A moment older than every full copy you still hold fails the call, rather than handing back a plan that could not work.

You write the code that fetches those files and rebuilds the database from them.

### Deleting old backups safely

The newest full copy is not enough on its own, because restoring to any moment after it also needs the change files in between. So ask rather than working it out by hand:

```ts
const stale = await db.backupPiecesSafeToDelete({
  restorableFrom: Date.now() - 30 * 24 * 60 * 60 * 1000,
})

for (const record of stale) await myStorage.deleteEveryPieceOf(record.name)
```

That call asks for a 30 day window, and returns everything no restore inside it needs. With no argument you get only what is already useless: a chain whose full copy someone removed, and change files stranded after a missing one.

Each record names one file. Your destination holds that file as the numbered pieces you stored under its name, so deleting a record means deleting every one of them.

Sirannon deletes nothing from your destination; it only tells you what is safe to remove.

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
