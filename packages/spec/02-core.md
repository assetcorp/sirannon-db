# Sirannon Core Specification

The core layer manages named databases, connection pooling, write serialisation,
query execution, change data capture, hooks, lifecycle, migrations, bulk loading,
backups, and metrics. Every Sirannon implementation must follow these contracts.

---

## Registry (Sirannon)

The registry is the top-level object that manages many named databases, each
identified by a unique string ID.

```text
Sirannon {
  constructor(options: SirannonOptions)

  open(id: string, path: string, options?: DatabaseOptions): async -> Database
  close(id: string): async -> void
  get(id: string): Database or null
  resolve(id: string): async -> Database or null
  has(id: string): boolean
  databases(): Map<string, Database>
  registryMigrations(): async -> List<Migration>
  shutdown(): async -> void

  onBeforeQuery(hook): DisposeFn
  onAfterQuery(hook): DisposeFn
  onBeforeConnect(hook): DisposeFn
  onDatabaseOpen(hook): DisposeFn
  onDatabaseClose(hook): DisposeFn
}

SirannonOptions {
  driver:        SQLiteDriver          (required)
  hooks?:        HookConfig
  metrics?:      MetricsConfig
  lifecycle?:    LifecycleConfig
  migrations?:   List<Migration> or (() -> List<Migration>, sync or async)
  writerWorker?: boolean or WriterWorkerOptions
}
```

A `writerWorker` on the registry is the default for every database it opens; a
`writerWorker` in `DatabaseOptions` overrides it for that database.

- **open** opens the file at `path` and registers it under `id`. A duplicate
  `id` (registered or opening) fails with `DATABASE_ALREADY_EXISTS`; a
  shut-down registry fails with `SHUTDOWN`. `open` creates the connection pool,
  fires `beforeConnect` then `databaseOpen`, and, when a registry `migrations`
  set is declared, applies every pending migration before registering the
  database (see [Registry Migrations](#registry-migrations)). No caller may
  observe a database through `get`, `resolve`, or `databases()` before its
  migrations complete.
- **close** closes the database under `id` and fires `databaseClose`. An unknown
  `id` fails with `DATABASE_NOT_FOUND`.
- **get** returns the registered database, or null. It is synchronous and has no
  side effect beyond marking the database recently used.
- **resolve** returns the registered database, or auto-opens it through the
  lifecycle resolver when one is configured, or returns nothing. Concurrent
  `resolve` calls for the same unregistered `id` share one auto-open: the first
  performs the open and its migrations, and every concurrent call receives the
  same result or error.
- **has** reports whether `id` is registered.
- **databases** returns a copy of the registered map.
- **registryMigrations** returns the resolved registry migration set.
- **shutdown** closes every database and marks the registry shut down; it is
  idempotent and fails with `SHUTDOWN_ERROR` when a close fails. After shutdown,
  `open` and `close` fail with `SHUTDOWN`, while `get`, `has`, and `resolve`
  report no database.

---

## Database

A single database backed by a SQLite file or an in-memory database.

```text
Database {
  readonly id: string
  readonly path: string
  readonly readOnly: boolean
  readonly closed: boolean
  readonly readerCount: number

  query<T>(sql, params?, options?): async -> List<T>
  queryOne<T>(sql, params?, options?): async -> T or null
  execute(sql, params?, options?): async -> ExecuteResult
  executeBatch(sql, paramsBatch, options?): async -> List<ExecuteResult>
  executeTransaction(statements, options?): async -> List<ExecuteResult>
  transaction<T>(fn: (tx: Transaction) -> async T): async -> T
  bulkLoad(sql, paramsBatch, options?): async -> BulkLoadResult

  applyChanges(batch, resolver?): async -> ApplyResult     -- see 08-device-sync.md
  deviceSync(): DeviceSyncPort                              -- see 08-device-sync.md

  watch(table): async -> void
  unwatch(table): async -> void
  on(table): SubscriptionBuilder

  migrate(migrations): async -> MigrationResult
  rollback(migrations, version?): async -> RollbackResult
  appliedMigrations(): async -> List<AppliedMigration>

  backup(destPath): async -> void
  scheduleBackup(options): void
  loadExtension(extensionPath): async -> void

  onBeforeQuery(hook): DisposeFn
  onAfterQuery(hook): DisposeFn
  addCloseListener(fn): void
  close(): async -> void
}

DatabaseOptions {
  readOnly?:        boolean          (default: false)
  readPoolSize?:    number           (default: 4, recommended)
  walMode?:         boolean          (default: true)
  synchronous?:     SynchronousLevel (default: 'normal')
  cdcPollInterval?: number           (default: 50 ms, recommended)
  cdcRetention?:    number           (default: 3_600_000 ms, recommended)
  writerWorker?:    boolean or WriterWorkerOptions (default: off)
}

ExecuteResult   { changes: number, lastInsertRowId: number or bigint }
Params          = Map<string, any> or List<any>
```

- **query / queryOne** run a read on a reader connection and fire query hooks.
  `queryOne` returns the first row or null.
- **execute** runs one write on the writer connection. A read-only database fails
  with `READ_ONLY`. Writes are coalesced by [group commit](#group-commit).
- **executeBatch** runs `sql` once per parameter set in one writer transaction,
  returning one result each; the batch is atomic.
- **executeTransaction** runs a fixed list of statements atomically, sharing a
  group commit when every statement is groupable.
- **transaction** runs `fn` inside one writer transaction, committing on success
  and rolling back on failure.
- **bulkLoad** loads many rows at relaxed durability; see [Bulk Load](#bulk-load).
- **watch** installs CDC triggers on the table and starts the poll loop.
  **unwatch** removes them and stops polling once no table is watched. **on**
  returns a subscription builder; a subscription receives events only for tables
  that are watched.
- **query options** carry `writeConcern` and `readConcern`. The core layer passes
  them to hooks and, for a replication execution target, to the replication
  engine, which enforces their meaning (see [03-replication.md](03-replication.md)).
  Plain core execution does not otherwise act on them.
- **close** stops CDC polling, cancels scheduled backups, drains pending grouped
  writes, closes the pool, and runs close listeners. Afterwards every method
  fails with `DATABASE_CLOSED`. While a device-sync snapshot load is in progress,
  reads and writes fail with `SNAPSHOT_IN_PROGRESS` (see [08-device-sync.md](08-device-sync.md)).

---

## Group Commit

Writes submitted concurrently are coalesced so one `fsync` commits many. The
writer forms a group from the statements waiting when a commit finishes; the
accumulation window is the previous commit's own duration, with no timer. Only
data-modifying statements (`INSERT`, `UPDATE`, `DELETE`, `REPLACE`) are grouped;
DDL, PRAGMA, and other statements run alone. A group holds at most 1000
statements (recommended). The group runs as one transaction; a statement that
fails before commit is isolated with a savepoint so only that unit fails, while
a failure at commit fails every unit in the group and is not retried, because the
commit may already have reached disk.

---

## Connection Pool

The pool holds one writer connection and a set of reader connections.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `readPoolSize` | 4 (recommended) | Reader connection count. |
| `readOnly` | `false` | Skip the writer connection. |
| `walMode` | `true` | WAL mode on the writer. |

Creation rules:

1. When not read-only, create one writer with WAL mode.
2. When the driver reports `multipleConnections`, create `max(readPoolSize, 1)`
   readers, each opened read-only. Otherwise create no readers and route reads
   through the writer under the writer lock.

`acquireReader` returns the next reader by round-robin, or the writer when no
readers exist, and fails with `CONNECTION_POOL_ERROR` when the pool is closed.
`acquireWriter` returns the writer and fails with `CONNECTION_POOL_ERROR` when
the pool is closed or read-only.

Write work on the writer connection is serialised by a writer lock so grouped
writes, transactions, migrations, backups, and extension loads never overlap.

---

## Writer Worker

SQLite's write path is synchronous: a commit waiting on `fsync`, a checkpoint,
or a long DDL statement blocks its thread. The writer worker moves writer
execution off the caller's thread. Reads are unaffected. The isolation mechanism
is implementation-defined; the option shape, the queue bound, the deadline
outcomes, and the error codes below are normative.

```text
WriterWorkerOptions {
  maxPendingWrites?: number  (default: 1024)
  writeTimeoutMs?:   number  (default: 30_000, 0 disables)
  maxRestarts?:      number  (default: 5)
}
```

`maxPendingWrites` must be at least 1, `writeTimeoutMs` at least 0,
`maxRestarts` at least 0; a value that fails validation fails with
`INVALID_WRITER_WORKER`. Enabling the worker with a driver that cannot run the
writer this way fails at open with `WRITER_WORKER_UNSUPPORTED`.

- **Queue bound.** A write arriving while `maxPendingWrites` writes are in flight
  is rejected with `WRITE_OVERLOADED` before it reaches the worker. The rejection
  is definite (the write never started, so a retry is safe) and carries a
  retry-after hint (recommended 1000 ms).
- **Deadline.** A synchronous native call cannot be interrupted, so the deadline
  never terminates the worker. When it expires, exactly one outcome follows: the
  work had not started, so it is skipped and the caller is rejected with
  `WRITE_OVERLOADED` (definite, retryable); or the result arrives within one
  further deadline (within twice `writeTimeoutMs`) and is delivered as a normal
  completion; or the work is still unresolved and the caller is rejected with
  `WRITER_WORKER_TIMEOUT` (indeterminate, so a non-idempotent write must be
  reconciled before retry). A deadline on open or close rejects with
  `WRITER_WORKER_TIMEOUT`.
- **Crash and restart.** A crash or exit rejects every in-flight write with
  `WRITER_WORKER_EXIT` and respawns the worker; a completed write resets the
  fault count; past `maxRestarts` faults, writes fail with `WRITER_WORKER_FATAL`.
  A write with no worker available fails with `WRITER_WORKER_UNAVAILABLE`, after
  close with `WRITER_WORKER_CLOSED`, and a failed handoff with
  `WRITER_WORKER_POST_FAILED`.

---

## Query Execution

### Statement Cache

Each connection caches prepared statements. The recommended capacity is 128 with
oldest-first eviction; a failed preparation removes the entry. The eviction
strategy is implementation-defined but must return correct results for repeated
queries.

### Parameter Normalisation

Before binding: omitted parameters become an empty list; a list passes through;
a named-parameter object is wrapped in a single-element list for engines that
bind named-parameter objects positionally.

### Reserved Identifiers

The query API refuses any statement that reaches Sirannon's internal tables.
An identifier beginning with `_sirannon` is reserved, so a read or write against
it fails with `FORBIDDEN_SQL`. The `sqlite_` catalogue is readable, but a
statement that modifies it fails with the same code, as do `PRAGMA
writable_schema`, `ATTACH`, and `DETACH`. Write verbs recognised for the
catalogue rule are `insert`, `update`, `delete`, `replace`, `create`, `alter`,
`drop`, `vacuum`, and `reindex`. The engine maintains its own tables through
internal connections that bypass this guard, so change tracking, migrations, and
replication keep working.

---

## Tagged Value Encoding (Normative)

JSON cannot carry two SQLite value types without loss: integers outside the safe
range -(2^53 - 1) to 2^53 - 1 lose precision as IEEE 754 doubles, and JSON has
no binary type. Wherever a column value crosses the wire or is stored in the
change log as JSON, these two values take tagged envelopes:

```text
IntegerEnvelope { "__sirannon_int":  string }   -- exact decimal, 1 to 19 digits, optional leading '-'
BlobEnvelope    { "__sirannon_blob": string }   -- uppercase hexadecimal, two digits per byte
```

An integer inside the safe range is a plain JSON number; an integer outside it
takes an `IntegerEnvelope`. Every BLOB takes a `BlobEnvelope`; an empty string
encodes an empty BLOB. All other value types keep their natural JSON form. A
consumer treats a JSON object with exactly one key, `__sirannon_int` or
`__sirannon_blob` and a string value, as an envelope and decodes it; envelopes
appear only where a column value is expected, so a stored TEXT value that
resembles one serialises as a JSON string and cannot collide. A malformed
envelope payload must be rejected rather than bound or decoded. This encoding is
used by [query result rows and bind parameters](05-server.md#value-encoding),
by change events, and by device-sync change batches.

---

## Change Data Capture (CDC)

CDC records row-level changes with SQLite triggers that write to a tracking
table, which a poll loop reads and dispatches to subscribers.

### Change Log

The tracking table is `_sirannon_changes`:

```sql
CREATE TABLE _sirannon_changes (
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name  TEXT NOT NULL,
  operation   TEXT NOT NULL,
  row_id      TEXT NOT NULL,
  changed_at  REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data    TEXT,
  new_data    TEXT,
  node_id     TEXT NOT NULL DEFAULT '',
  tx_id       TEXT NOT NULL DEFAULT '',
  hlc         TEXT NOT NULL DEFAULT ''
)
```

The `node_id`, `tx_id`, and `hlc` columns are always present and carry the sync
metadata described in [08-device-sync.md](08-device-sync.md); an unstamped local
change has all three empty. Implementations create indexes on `changed_at`,
`node_id`, and `hlc`.

### Triggers

For each watched table, three `AFTER` triggers are installed, named
`_sirannon_trg_{table}_insert`, `_sirannon_trg_{table}_update`, and
`_sirannon_trg_{table}_delete`. Each inserts a change row with:

- `operation`: `'INSERT'`, `'UPDATE'`, or `'DELETE'` (stored upper case).
- `row_id`: the affected row's primary key. With one key column it is that
  value; with several it is the values joined by `-`; with no primary key it is
  the SQLite `rowid`.
- `new_data`, `old_data`: a JSON object of the row's column values, each value
  encoded by the [Tagged Value Encoding](#tagged-value-encoding-normative). The
  column list is fixed when the trigger is created, so a table altered with
  `ADD COLUMN` needs its triggers reinstalled.
- `node_id`, `tx_id`, `hlc`: written empty; local writes are stamped afterwards
  (see [08-device-sync.md](08-device-sync.md)).

Table and column names used in trigger SQL must match `^[a-zA-Z_][a-zA-Z0-9_]*$`;
names that do not match must be rejected.

### CDC Epoch

Each database file holds a random epoch string in `_sirannon_meta` under
`cdc_epoch`, minted once and stable for the file's lifetime. It identifies the
file's `seq` space so a resume cursor carried from another file is recognised as
foreign and forces a resync rather than replaying unrelated rows.

### Polling and Cleanup

The poll loop reads rows where `seq > lastSeq`, ordered by `seq`, up to a
recommended 1000 rows per poll at a recommended 50 ms interval, and skips the
query when no subscriber is active. Old rows are pruned periodically (recommended
every 100 poll ticks) by deleting rows older than the retention window
(recommended 3,600,000 ms); when a prune boundary is set, deletion is also bounded
by `seq` so unacknowledged changes are retained.

### Change Events and Subscriptions

```text
ChangeEvent<T> {
  type:      'insert' | 'update' | 'delete'
  table:     string
  row:       T
  oldRow?:   T
  seq:       bigint
  timestamp: number
  hlc?:      string
  origin?:   string
}

SubscriptionBuilder {
  filter(conditions: Map<string, any>): SubscriptionBuilder
  subscribe(callback: (event) -> void): Subscription
}
```

`oldRow` is present for updates and deletes. `origin` carries the change's
`node_id` and `hlc` its timestamp when stamped. A filter matches when every
key-value pair equals the event row's value (matched against `oldRow` for a
delete). An error thrown by one subscription callback must not stop delivery to
others.

---

## System Catalogue

Sirannon keeps its own tables under the `_sirannon_` prefix: `_sirannon_changes`
(above), `_sirannon_meta` (a `key TEXT PRIMARY KEY, value TEXT NOT NULL` store),
`_sirannon_migrations` (below), and the replication and device-sync tables
defined in [03-replication.md](03-replication.md) and
[08-device-sync.md](08-device-sync.md). The meta table holds `cdc_epoch`,
`node_id`, `hlc_clock`, and the device-sync cursor keys.

---

## Hooks

Hooks registered on the registry apply to every database and run before
database-level hooks for the same event, in registration order.

| Event | Context | When | Can deny |
|-------|---------|------|----------|
| `beforeQuery` | `{ databaseId, sql, params?, writeConcern?, readConcern? }` | Before a query | Yes |
| `afterQuery` | `{ databaseId, sql, params?, durationMs }` | After a query | No |
| `beforeConnect` | `{ databaseId, path }` | Before a connection opens | Yes |
| `databaseOpen` | `{ databaseId, path }` | After a database opens | No |
| `databaseClose` | `{ databaseId, path }` | After a database closes | No |

A before-hook that throws aborts the operation, and its error propagates to the
caller. Query and connection hooks run synchronously; a hook that returns a
promise fails. Hooks are registered through the dedicated methods or a
`HookConfig` object that accepts one function or a list per event. Each `on…`
registrar returns a `DisposeFn` (a `() -> void`) that removes the hook;
disposing more than once is a no-op.

---

## Lifecycle Management

```text
LifecycleConfig {
  autoOpen?:   { resolver: (id) -> { path, options? } or null }
  idleTimeout?: number   (0 disables)
  maxOpen?:     number   (0 = unlimited)
}
```

When `resolve(id)` finds no registered database and a resolver is configured, the
resolver is called and any returned path is auto-opened and registered. When
`idleTimeout` is above zero, databases idle past the window are closed on a
recurring check (recommended interval `min(max(floor(timeout / 2), 100), 60000)`
ms). When `maxOpen` is reached, the least-recently-used database is evicted to
make room, or the open fails with `MAX_DATABASES` when nothing is evictable.

---

## Migrations

### Tracking Table

```sql
CREATE TABLE _sirannon_migrations (
  version    INTEGER PRIMARY KEY,
  name       TEXT NOT NULL,
  applied_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
  checksum   TEXT
)
```

The highest applied `version` is mirrored into `PRAGMA user_version`.

### Migration Definition

```text
Migration {
  version:   number    (positive integer, at most 2_147_483_647)
  name:      string    (matches ^\w+$)
  up:        string or (tx: Transaction) -> async void
  down?:     string or (tx: Transaction) -> async void
  baseline?: { through: number }
}
```

The version cap is 2,147,483,647 because it mirrors to `PRAGMA user_version`, a
signed 32-bit value.

### Checksum (Normative)

A string `up` migration carries a checksum: the 64-bit FNV-1a hash of the up SQL
with line endings normalised to `\n` and surrounding whitespace trimmed,
rendered as 16 lowercase hexadecimal digits. A function migration has no
checksum. When a migration runs, a stored checksum that differs from the
recomputed value fails with `MIGRATION_CHECKSUM_MISMATCH`; a null stored checksum
is backfilled. The checksum is normative because the device-sync migration
handshake serves and verifies `up` SQL by it (see [08-device-sync.md](08-device-sync.md)).

### Execution

Migrations are validated (positive integer version within the cap, name matching
`^\w+$`, no duplicate versions, non-empty SQL), sorted ascending, and each
pending migration runs in its own transaction that executes the `up` and inserts
a tracking row with the content checksum. A failure rolls that migration back and
fails with `MIGRATION_ERROR` carrying the version; validation failures fail with
`MIGRATION_VALIDATION_ERROR` or `MIGRATION_DUPLICATE_VERSION`. A concurrent
attempt is retried once and otherwise fails with `MIGRATION_CONCURRENT`.

### Baseline

A migration marked `baseline: { through }` squashes history up to `through`.
`through` must be at least 1 and below the migration's own version, and no
non-baseline migration may fall inside `(through, version)`. On an empty history
the baseline plus every migration above `through` applies; on a history already
at or above `through` the baseline is skipped and only migrations above `through`
apply; a history below `through` with the bridging migrations absent fails with
`MIGRATION_BASELINE_GAP`.

### Registry Migrations

A registry may declare one migration set in `SirannonOptions.migrations`, so an
operator hosting many databases (for example one file per tenant) rolls out
schema changes without opening each file. The rollout is pull-based: each
database applies the pending set the next time it opens, through a direct `open`
or the lifecycle resolver.

The set is a list or a function returning a list. The registry calls the function
at most once, on the first open that needs it, and caches the result. A function
that throws fails that open with its own error; a non-list result fails with
`MIGRATION_SOURCE_INVALID`; either way the database is left unregistered and the
next open retries. `open` applies every pending migration after creating the
connections and before registering the database. A migration failure closes the
database, leaves it unregistered, and rethrows the migration error unchanged; any
other failure of the step throws `DATABASE_OPEN_FAILED`. A read-only open skips
the set, because a read-only connection cannot create the tracking table. When no
set is declared, `open` runs no migration step and creates no tracking table.

### Rollback

Rollback reverses applied migrations in descending version order. With no target
it reverses only the latest; with a target it reverses every version above the
target. Each runs the migration's `down` in a transaction, then removes the
tracking row and re-mirrors `user_version`. A missing `down` fails with
`MIGRATION_NO_DOWN`.

### File Migration Sources

An implementation should provide `migrationsFromFiles(files)`, a pure function
turning a map of filename to SQL text into a sorted migration list, so an
application whose bundler inlines `.sql` files can build its set without run-time
filesystem access. Only the final path segment is parsed, matching
`<version>_<name>.up.sql` or `<version>_<name>.down.sql`. A segment that does not
match, a non-string value, empty SQL, or a version with no up file fails with
`MIGRATION_VALIDATION_ERROR`; a version collision fails with
`MIGRATION_DUPLICATE_VERSION`. A directory loader that reads SQL from disk is
also provided; it rejects control characters and `..` path segments.

---

## Bulk Load

```text
BulkLoadOptions {
  durability?: 'off' | 'normal'  (default: 'off')
  checkpoint?: boolean           (default: true)
}

BulkLoadResult { rowsLoaded: number, changes: number }
```

`bulkLoad` relaxes `PRAGMA synchronous` to the chosen durability, loads the rows
in one transaction, then always restores the configured durability level, on
success and on failure. An invalid durability fails with `INVALID_DURABILITY`; a
failure to restore after a committed load fails with `DURABILITY_RESTORE_FAILED`.
When `checkpoint` is set, WAL mode is active, and the load changed rows, a WAL
checkpoint runs afterwards, retrying a few times and deferring rather than failing
when a reader holds pages.

---

## Backups

`backup(destPath)` creates a point-in-time snapshot with `VACUUM INTO`. Paths
containing null bytes, control characters, or `..` segments, and destinations
that already exist, are rejected; the parent directory is created recursively; a
failure makes a best-effort cleanup of the partial file and then fails with
`BACKUP_ERROR`. A driver with no backup engine fails with `BACKUP_UNSUPPORTED`.
The recommended filename is `backup-{ISO timestamp}.db` with colons and periods
replaced by hyphens.

```text
BackupScheduleOptions {
  cron:      string
  destDir:   string
  maxFiles?: number   (default: 5, recommended)
  timezone?: string   (IANA name; default: host time zone)
  onError?:  (error) -> void
}
```

`scheduleBackup` runs on the cron schedule, backs up into `destDir`, and rotates
files matching `backup-*.db` beyond `maxFiles` by modification time. The cron
expression is evaluated in `timezone` when supplied, otherwise the host zone. The
scheduler checks the time on a recurring tick and does not backfill: a scheduled
time skipped while the host sleeps or the clock jumps forward is not run late,
and a backward clock step repeats nothing until real time passes the last
completed backup. Across a daylight-saving forward transition the missing hour is
skipped; across a backward transition a time in the repeated hour runs once.

---

## Metrics

```text
MetricsConfig {
  onQueryComplete?:   (metrics: QueryMetrics) -> void
  onConnectionOpen?:  (metrics: ConnectionMetrics) -> void
  onConnectionClose?: (metrics: ConnectionMetrics) -> void
}

QueryMetrics       { databaseId: string, sql: string, durationMs: number, error?: boolean }
ConnectionMetrics  { databaseId: string, path: string, readerCount: number, event: 'open' | 'close' }
```

Metrics callbacks are optional and configured only when `metrics` is supplied.
`onConnectionClose` reports `readerCount` as 0. An error thrown by a metrics
callback must not affect database operations.
