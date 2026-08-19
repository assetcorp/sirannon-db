# Sirannon Core Specification

The core layer manages named databases, connection pooling, write serialisation, query execution, change data capture, live queries, hooks, lifecycle, migrations, bulk loading, backups, and metrics. Every Sirannon implementation must follow these contracts.

---

## Registry (Sirannon)

The registry is the top-level object that manages many named databases, each identified by a unique string ID.

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

A `writerWorker` on the registry is the default for every database it opens; a `writerWorker` in `DatabaseOptions` overrides it for that database.

- **open** opens the file at `path` and registers it under `id`. A duplicate `id` (registered or opening) fails with `DATABASE_ALREADY_EXISTS`; a shut-down registry fails with `SHUTDOWN`. `open` creates the connection pool, fires `beforeConnect` then `databaseOpen`, and, when a registry `migrations` set is declared, applies every pending migration before registering the database (see [Registry Migrations](#registry-migrations)). No caller may observe a database through `get`, `resolve`, or `databases()` before its migrations complete.
- **close** closes the database under `id` and fires `databaseClose`. An unknown `id` fails with `DATABASE_NOT_FOUND`.
- **get** returns the registered database, or null. It is synchronous and has no side effect beyond marking the database recently used.
- **resolve** returns the registered database, or auto-opens it through the lifecycle resolver when one is configured, or returns nothing. Concurrent `resolve` calls for the same unregistered `id` share one auto-open: the first performs the open and its migrations, and every concurrent call receives the same result or error.
- **has** reports whether `id` is registered.
- **databases** returns a copy of the registered map.
- **registryMigrations** returns the resolved registry migration set.
- **shutdown** closes every database and marks the registry shut down; it is idempotent and fails with `SHUTDOWN_ERROR` when a close fails. After shutdown, `open` and `close` fail with `SHUTDOWN`, while `get`, `has`, and `resolve` report no database.

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
  live<T>(sql, params?, options?): async -> LiveQuery<T>

  migrate(migrations): async -> MigrationResult
  rollback(migrations, version?): async -> RollbackResult
  appliedMigrations(): async -> List<AppliedMigration>

  backup(destPath): async -> void
  backupTo(options): async -> BackupRunReport
  backupCapabilities(): BackupCapabilities
  scheduleBackup(options): void
  captureBackupChanges(): async -> BackupRunReport or null
  backupChain(): async -> List<BackupChain>
  backupRestorePlan(moment): async -> BackupRestorePlan
  backupPiecesSafeToDelete(options?): async -> List<BackupChainRecord>
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
  backups?:         BackupCycleOptions (default: off)
}

ExecuteResult { changes: number, lastInsertRowId: number or bigint }
Params        = Map<string, any> or List<any>
```

- **query / queryOne** run a read on a reader connection and fire query hooks. `queryOne` returns the first row or null.
- **execute** runs one write on the writer connection. A read-only database fails with `READ_ONLY`. Writes are coalesced by [group commit](#group-commit).
- **executeBatch** runs `sql` once per parameter set in one writer transaction, returning one result each; the batch is atomic.
- **executeTransaction** runs a fixed list of statements atomically, sharing a group commit when every statement is groupable.
- **transaction** runs `fn` inside one writer transaction, committing on success and rolling back on failure.
- **bulkLoad** loads many rows at relaxed durability; see [Bulk Load](#bulk-load).
- **watch** installs CDC triggers on the table and starts the poll loop. **unwatch** removes them and stops polling once no table is watched. **on** returns a subscription builder; a subscription receives events only for tables that are watched.
- **live** returns a query result that change events keep current; see [Live Queries](#live-queries).
- **query options** carry `writeConcern` and `readConcern`. The core layer passes them to hooks and, for a replication execution target, to the replication engine, which enforces their meaning (see [03-replication.md](03-replication.md)). Plain core execution does not otherwise act on them.
- **close** stops CDC polling, cancels scheduled backups, drains pending grouped writes, captures the change log a final time, closes the pool, and runs close listeners. Afterwards every method fails with `DATABASE_CLOSED`. While a device-sync snapshot load is in progress, reads and writes fail with `SNAPSHOT_IN_PROGRESS` (see [08-device-sync.md](08-device-sync.md)).

---

## Group Commit

Writes submitted concurrently are coalesced so one `fsync` commits many. The writer forms a group from the statements waiting when a commit finishes; the accumulation window is the previous commit's own duration, with no timer. Only data-modifying statements (`INSERT`, `UPDATE`, `DELETE`, `REPLACE`) are grouped; DDL, PRAGMA, and other statements run alone. A group holds at most 1000 statements (recommended). The group runs as one transaction; a statement that fails before commit is isolated with a savepoint so only that unit fails, while a failure at commit fails every unit in the group and is not retried, because the commit may already have reached disk.

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
2. When the driver reports `multipleConnections`, create `max(readPoolSize, 1)` readers, each opened read-only. Otherwise create no readers and route reads through the writer under the writer lock.

`acquireReader` returns the next reader by round-robin, or the writer when no readers exist, and fails with `CONNECTION_POOL_ERROR` when the pool is closed. `acquireWriter` returns the writer and fails with `CONNECTION_POOL_ERROR` when the pool is closed or read-only. `connections` returns the writer and every reader, for work that must apply to the whole pool, and fails with `CONNECTION_POOL_ERROR` when the pool is closed.

Write work on the writer connection is serialised by a writer lock so grouped writes, transactions, migrations, and extension loads never overlap. A backup takes that lock to start its copy and releases it once the copy's first step completes, so writes made after that step run in the gaps between the steps that follow.

---

## Writer Worker

SQLite's write path is synchronous: a commit waiting on `fsync`, a checkpoint, or a long DDL statement blocks its thread. The writer worker moves writer execution off the caller's thread. Reads are unaffected. The isolation mechanism is implementation-defined; the option shape, the queue bound, the deadline outcomes, and the error codes below are normative.

```text
WriterWorkerOptions {
  maxPendingWrites?: number  (default: 1024)
  writeTimeoutMs?:   number  (default: 30_000, 0 disables)
  maxRestarts?:      number  (default: 5)
}
```

`maxPendingWrites` must be at least 1, `writeTimeoutMs` at least 0, `maxRestarts` at least 0; a value that fails validation fails with `INVALID_WRITER_WORKER`. Enabling the worker with a driver that cannot run the writer this way fails at open with `WRITER_WORKER_UNSUPPORTED`.

- **Queue bound.** A write arriving while `maxPendingWrites` writes are in flight is rejected with `WRITE_OVERLOADED` before it reaches the worker. The rejection is definite (the write never started, so a retry is safe) and carries a retry-after hint (recommended 1000 ms).
- **Deadline.** A synchronous native call cannot be interrupted, so the deadline never terminates the worker. When it expires, exactly one outcome follows: the work had not started, so it is skipped and the caller is rejected with `WRITE_OVERLOADED` (definite, retryable); or the result arrives within one further deadline (within twice `writeTimeoutMs`) and is delivered as a normal completion; or the work is still unresolved and the caller is rejected with `WRITER_WORKER_TIMEOUT` (indeterminate, so a non-idempotent write must be reconciled before retry). A deadline on open or close rejects with `WRITER_WORKER_TIMEOUT`.
- **Crash and restart.** A crash or exit rejects every in-flight write with `WRITER_WORKER_EXIT` and respawns the worker; a completed write resets the fault count; past `maxRestarts` faults, writes fail with `WRITER_WORKER_FATAL`. A write with no worker available fails with `WRITER_WORKER_UNAVAILABLE`, after close with `WRITER_WORKER_CLOSED`, and a failed handoff with `WRITER_WORKER_POST_FAILED`.

---

## Query Execution

### Statement Cache

Each connection caches prepared statements. The recommended capacity is 128 with oldest-first eviction; a failed preparation removes the entry. The eviction strategy is implementation-defined but must return correct results for repeated queries.

### Parameter Normalisation

Before binding: omitted parameters become an empty list; a list passes through; a named-parameter object is wrapped in a single-element list for engines that bind named-parameter objects positionally.

### Reserved Identifiers

The query API must reject any statement that reaches Sirannon's internal tables. An identifier beginning with `_sirannon` is reserved, and a read or write against it must fail with `FORBIDDEN_SQL`. The `sqlite_` catalogue is readable, and a statement that modifies it must fail with the same code, as must `PRAGMA writable_schema`, `ATTACH`, and `DETACH`. The write verbs for the catalogue rule are `insert`, `update`, `delete`, `replace`, `create`, `alter`, `drop`, `vacuum`, and `reindex`. Internal connections bypass this guard, so that change tracking, migrations, and replication continue to maintain their own tables.

---

## Tagged Value Encoding (Normative)

JSON cannot carry two SQLite value types without loss: integers outside the safe range -(2^53 - 1) to 2^53 - 1 lose precision as IEEE 754 doubles, and JSON has no binary type. Wherever a column value crosses the wire or is stored in the change log as JSON, these two values take tagged envelopes:

```text
IntegerEnvelope { "__sirannon_int":  string }   -- exact decimal, 1 to 19 digits, optional leading '-'
BlobEnvelope    { "__sirannon_blob": string }   -- uppercase hexadecimal, two digits per byte
```

An integer inside the safe range is a plain JSON number; an integer outside it takes an `IntegerEnvelope`. Every BLOB takes a `BlobEnvelope`; an empty string encodes an empty BLOB. All other value types keep their natural JSON form. A consumer treats a JSON object with exactly one key, `__sirannon_int` or `__sirannon_blob` and a string value, as an envelope and decodes it; envelopes appear only where a column value is expected, so a stored TEXT value that resembles one serialises as a JSON string and cannot collide. A malformed envelope payload must be rejected rather than bound or decoded. This encoding is used by [query result rows and bind parameters](05-server.md#value-encoding), by change events, and by device-sync change batches.

---

## Change Data Capture (CDC)

CDC records row-level changes with SQLite triggers that write to a tracking table, which a poll loop reads and dispatches to subscribers.

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

The `node_id`, `tx_id`, and `hlc` columns are always present and carry the sync metadata described in [08-device-sync.md](08-device-sync.md); an unstamped local change has all three empty. Implementations create indexes on `changed_at`, `node_id`, and `hlc`.

### Triggers

For each watched table, three `AFTER` triggers are installed, named `_sirannon_trg_{table}_insert`, `_sirannon_trg_{table}_update`, and `_sirannon_trg_{table}_delete`. Each inserts a change row with:

- `operation`: `'INSERT'`, `'UPDATE'`, or `'DELETE'` (stored upper case).
- `row_id`: the affected row's primary key. With one key column it is that value; with several it is the values joined by `-`; with no primary key it is the SQLite `rowid`.
- `new_data`, `old_data`: a JSON object of the row's column values, each value encoded by the [Tagged Value Encoding](#tagged-value-encoding-normative). The column list is fixed when the trigger is created, so a table altered with `ADD COLUMN` needs its triggers reinstalled.
- `node_id`, `tx_id`, `hlc`: written empty; local writes are stamped afterwards (see [08-device-sync.md](08-device-sync.md)).

Table and column names used in trigger SQL must match `^[a-zA-Z_][a-zA-Z0-9_]*$`; names that do not match must be rejected.

### CDC Epoch

Each database file holds a random epoch string in `_sirannon_meta` under `cdc_epoch`, minted once and stable for the file's lifetime. It identifies the file's `seq` space so a resume cursor carried from another file is recognised as foreign and forces a resync rather than replaying unrelated rows.

### Read Positions

A read position names the change-log point a read's rows already include. A reader that subscribes from that position misses no change and receives none twice. The capability is internal: a live query opens with a positioned read, and no other surface takes or returns a position.

A positioned read runs the read and reads the change log's highest `seq` in one transaction, so the rows and the position come from one snapshot. Capturing the position separately is wrong: a write that commits between the two makes them disagree, and re-applying a change is unsafe for a table with no declared primary key.

The read takes a connection of its own and closes it afterwards, whether the read succeeds or fails. One pooled reader serves several concurrent reads, so a transaction opened on it would capture their statements and end their reads on commit. A driver that opens one connection per file runs the read on the writer under the writer lock instead. Minting the epoch is a write, so a read-only database serves no positioned read. `query` and `queryOne` keep their single-statement path and open no transaction.

The position is an opaque token holding the file's epoch and the sequence:

```text
position = hex(utf8("1:" + epoch + ":" + seq))
```

A sequence means nothing in another file's sequence space, so the token carries the epoch with it. The code holding a token passes it back rather than reading it, so the encoding stays free to change. A token that fails to decode, carries another version, or holds a malformed epoch or sequence is refused rather than interpreted.

### Polling and Cleanup

The poll loop reads rows where `seq > lastSeq`, ordered by `seq`, up to a recommended 1000 rows per poll at a recommended 50 ms interval, and skips the query when no subscriber is active. Old rows are pruned periodically (recommended every 100 poll ticks) by deleting rows older than the retention window (recommended 3,600,000 ms); when a prune boundary is set, deletion is also bounded by `seq` so unacknowledged changes are retained.

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
  rowId?:    string
  txId?:     string
  txEnd?:    boolean
}

SubscriptionBuilder {
  filter(conditions: Map<string, any>): SubscriptionBuilder
  subscribe(callback: (event) -> void): Subscription
}
```

`oldRow` is present for updates and deletes. `rowId` carries the change row's `row_id` and is present on every change read from the log, so a subscriber identifies the affected row without reading a key column. When the change is stamped, `origin` carries its `node_id`, `hlc` its timestamp, and `txId` the transaction that made it. `txEnd` marks the last change of a transaction; the core subscription delivers each change as the poll reads it and leaves `txEnd` unset, while the WebSocket subscription marks it (see [05-server.md](05-server.md#transaction-boundaries)). An error thrown by one subscription callback must not stop delivery to others.

A filter holds key-value pairs, and a row matches when it holds the filter's value under every key. The subscription evaluates the filter against the row before the change and against the row after it. What the subscriber receives follows from the two results:

| Before | After | Delivered |
|--------|-------|-----------|
| matches | matches | the change unchanged |
| matches | no match | a delete carrying the old row |
| no match | matches | an insert carrying the new row |
| no match | no match | nothing |

An insert has no row before the change and a delete none after, so only an update crosses the boundary; the subscription delivers an insert or a delete unchanged, or drops it. A delete built from an update carries the old row in `oldRow` and an empty `row`, and an insert built from an update carries no `oldRow`; both keep every other field of the change. A subscription carrying no filter receives every change for its table. An implementation must build a new change rather than alter the polled one, because every subscriber on a table receives the same change.

---

## Live Queries

`live` returns a query result the implementation must keep current from change events. Each change updates the rows the result already holds. The read runs a second time only in the cases listed below.

```text
LiveQuery<T> {
  getState(): LiveQueryState<T>
  subscribe(listener: (update: LiveUpdate<T>) -> void): DisposeFn
  close(): async -> void
}

LiveQueryState<T> = { status: 'pending' }
  | { status: 'ready', rows: List<T>, revalidating: boolean }
  | { status: 'error', error: Error }

LiveUpdate<T> = { kind: 'rows' }
  | { kind: 'ops', ops: List<ResultOp<T>> }
  | { kind: 'revalidating' }
  | { kind: 'error' }

ResultOp<T> = { op: 'insert', index, row: T }
  | { op: 'update', index, row: T }
  | { op: 'delete', index }

LiveQueryOptions {
  rereadJitterMs?:        number  (default: 25 ms, recommended)
  maxTransactionChanges?: number  (default: 10_000, recommended)
}
```

`ops` carries the splices that produced the new rows, in order. `rows` follows a second read that replaced them. An implementation that maintains its own copy of the result, such as a server serving a remote live query, applies the operations in order to hold the same rows; one that only renders the result reads `getState`.

Opening a live query must watch the statement's table, read once, and subscribe from that read's position, so that every change reaches the result exactly once. `live` on a read-only database must fail with `READ_ONLY`, because `watch` installs triggers.

Each live query owns a temporary probe table whose columns match the declared types and collations of the base table. For each transaction, the implementation must write the row before and after every change into that table, then run the statement's own `WHERE` clause and select list over those rows. Affinity, collation, and `ORDER BY` therefore match a read of the base table.

The implementation must run the read a second time in three cases: a transaction carries more changes than the result has rows, a `LIMIT` window loses a row the held rows cannot replace, or buffered changes exceed `maxTransactionChanges` or an implementation-defined byte bound. `revalidating` is true for the duration of that read, and the previous rows remain readable. `rereadJitterMs` bounds a random delay before it.

A live query maintains the result of a single-table statement. `live` must fail with `CDC_ERROR` for a join, an aggregate, `GROUP BY`, `HAVING`, `DISTINCT`, a compound `SELECT`, a window function, a subquery, or `LIMIT` without `ORDER BY`.

---

## System Catalogue

Sirannon keeps its own tables under the `_sirannon_` prefix: `_sirannon_changes` (above), `_sirannon_meta` (a `key TEXT PRIMARY KEY, value TEXT NOT NULL` store), `_sirannon_migrations` (below), and the replication and device-sync tables defined in [03-replication.md](03-replication.md) and [08-device-sync.md](08-device-sync.md). The meta table holds `cdc_epoch`, `node_id`, `hlc_clock`, and the device-sync cursor keys. A live query adds one temporary table under the same prefix and drops it when the query closes.

---

## Hooks

Hooks registered on the registry apply to every database and run before database-level hooks for the same event, in registration order.

| Event | Context | When | Can deny |
|-------|---------|------|----------|
| `beforeQuery` | `{ databaseId, sql, params?, writeConcern?, readConcern? }` | Before a query | Yes |
| `afterQuery` | `{ databaseId, sql, params?, durationMs }` | After a query | No |
| `beforeConnect` | `{ databaseId, path }` | Before a connection opens | Yes |
| `databaseOpen` | `{ databaseId, path }` | After a database opens | No |
| `databaseClose` | `{ databaseId, path }` | After a database closes | No |

A before-hook that throws aborts the operation, and its error propagates to the caller. Query and connection hooks run synchronously; a hook that returns a promise fails. Hooks are registered through the dedicated methods or a `HookConfig` object that accepts one function or a list per event. Each `on…` registrar returns a `DisposeFn` (a `() -> void`) that removes the hook; disposing more than once is a no-op.

---

## Lifecycle Management

```text
LifecycleConfig {
  autoOpen?:   { resolver: (id) -> { path, options? } or null }
  idleTimeout?: number   (0 disables)
  maxOpen?:     number   (0 = unlimited)
}
```

When `resolve(id)` finds no registered database and a resolver is configured, the resolver is called and any returned path is auto-opened and registered. When `idleTimeout` is above zero, databases idle past the window are closed on a recurring check (recommended interval `min(max(floor(timeout / 2), 100), 60000)` ms). When `maxOpen` is reached, the least-recently-used database is evicted to make room, or the open fails with `MAX_DATABASES` when nothing is evictable.

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

The version cap is 2,147,483,647 because it mirrors to `PRAGMA user_version`, a signed 32-bit value.

### Checksum (Normative)

A string `up` migration carries a checksum: the 64-bit FNV-1a hash of the up SQL with line endings normalised to `\n` and surrounding whitespace trimmed, rendered as 16 lowercase hexadecimal digits. A function migration has no checksum. When a migration runs, a stored checksum that differs from the recomputed value fails with `MIGRATION_CHECKSUM_MISMATCH`; a null stored checksum is backfilled. The checksum is normative because the device-sync migration handshake uses it to serve and verify `up` SQL (see [08-device-sync.md](08-device-sync.md)).

### Execution

Migrations are validated (positive integer version within the cap, name matching `^\w+$`, no duplicate versions, non-empty SQL), sorted ascending, and each pending migration runs in its own transaction that executes the `up` and inserts a tracking row with the content checksum. A failure rolls that migration back and fails with `MIGRATION_ERROR` carrying the version; validation failures fail with `MIGRATION_VALIDATION_ERROR` or `MIGRATION_DUPLICATE_VERSION`. A concurrent attempt is retried once and otherwise fails with `MIGRATION_CONCURRENT`.

### Baseline

A migration marked `baseline: { through }` squashes history up to `through`. `through` must be at least 1 and below the migration's own version, and no non-baseline migration may fall inside `(through, version)`. On an empty history the baseline plus every migration above `through` applies; on a history already at or above `through` the baseline is skipped and only migrations above `through` apply; a history below `through` with the bridging migrations absent fails with `MIGRATION_BASELINE_GAP`.

### Registry Migrations

A registry may declare one migration set in `SirannonOptions.migrations`, so an operator hosting many databases (for example one file per tenant) rolls out schema changes without opening each file. The rollout is pull-based: each database applies the pending set the next time it opens, through a direct `open` or the lifecycle resolver.

The set is a list or a function returning a list. The registry calls the function at most once, on the first open that needs it, and caches the result. A function that throws fails that open with its own error; a non-list result fails with `MIGRATION_SOURCE_INVALID`; either way the database is left unregistered and the next open retries. `open` applies every pending migration after creating the connections and before registering the database. A migration failure closes the database, leaves it unregistered, and rethrows the migration error unchanged; any other failure of the step throws `DATABASE_OPEN_FAILED`. A read-only open skips the set, because a read-only connection cannot create the tracking table. When no set is declared, `open` runs no migration step and creates no tracking table.

### Rollback

Rollback reverses applied migrations in descending version order. With no target it reverses only the latest; with a target it reverses every version above the target. Each runs the migration's `down` in a transaction, then removes the tracking row and re-mirrors `user_version`. A missing `down` fails with `MIGRATION_NO_DOWN`.

### File Migration Sources

An implementation should provide `migrationsFromFiles(files)`, a pure function turning a map of filename to SQL text into a sorted migration list, so an application whose bundler inlines `.sql` files can build its set without run-time filesystem access. Only the final path segment is parsed, matching `<version>_<name>.up.sql` or `<version>_<name>.down.sql`. A segment that does not match, a non-string value, empty SQL, or a version with no up file fails with `MIGRATION_VALIDATION_ERROR`; a version collision fails with `MIGRATION_DUPLICATE_VERSION`. A directory loader that reads SQL from disk is also provided; it rejects control characters and `..` path segments.

---

## Bulk Load

```text
BulkLoadOptions {
  durability?: 'off' | 'normal'  (default: 'off')
  checkpoint?: boolean           (default: true)
}

BulkLoadResult { rowsLoaded: number, changes: number }
```

`bulkLoad` relaxes `PRAGMA synchronous` to the chosen durability, loads the rows in one transaction, then always restores the configured durability level, on success and on failure. An invalid durability fails with `INVALID_DURABILITY`; a failure to restore after a committed load fails with `DURABILITY_RESTORE_FAILED`. When `checkpoint` is set, WAL mode is active, and the load changed rows, a WAL checkpoint runs afterwards, retrying a few times and deferring rather than failing when a reader holds pages. A database opened with `backups` runs no checkpoint here, whatever the caller set.

---

## Backups

A backup copies a database while writes continue. The copy runs through SQLite's stepped backup interface on the connection that writes, because SQLite returns a copy to page one whenever any other connection writes to the source or runs a `RESTART` or `TRUNCATE` checkpoint on it. No backup operation may hold the writer lock for longer than its first step.

### Full copy to a path

`backup(destPath)` copies the database to a local file. Paths containing null bytes, control characters, or `..` segments, and destinations that already exist, are rejected; the parent directory is created recursively; a failure makes a best-effort cleanup of the partial file and then fails with `BACKUP_ERROR`. A driver with no backup engine fails with `BACKUP_UNSUPPORTED`. The recommended filename is `backup-{ISO timestamp}.db` with colons and periods replaced by hyphens.

### Restarts and stalls

An implementation compares the pages copied, which is `totalPages - remainingPages`, against the previous step's. A fall in that count is a restart, because SQLite has returned the copy to page one; a rise in both counters is the source growing under the copy and is not a restart. An implementation counts restarts, stops after `restartLimit` of them, and fails with `BACKUP_RESTARTED`, naming what restarts a copy and what the operator does about it. A restart is never retried silently and never retried forever.

A copy restarted on every step reports the same pages copied at every step, so the count of restarts alone leaves it running for ever. An implementation therefore tracks the furthest the copy has reached and fails with `BACKUP_RESTARTED` after `noProgressStepLimit` steps that reach no further, which also catches a source growing faster than the copy moves it.

An implementation restarts a stall deadline on every step and fails with `BACKUP_STALLED` when no step arrives inside `stallTimeoutMs`, because a runtime whose event loop never reaches the copy's continuation holds the copy still without ending it.

### Destination

`backupTo(options)` copies the database to a destination the caller supplies. Sirannon carries no storage client, so the caller supplies three operations and connects object storage or anything else that moves bytes.

```text
BackupDestination {
  writePiece(name: string, index: number, bytes: Bytes): async -> void
  readPiece(name: string, index: number): async -> Bytes
  listPieces(name: string): async -> List<BackupPiece>
}

BackupPiece {
  index:      number
  byteLength: number
}
```

Every piece holds `pieceBytes` bytes except the last. A destination must accept pieces in any order, because SQLite writes page one last. A destination must let a second write to the same name and index replace the piece already stored, because a run resumed after an interruption repeats its last write. A destination must hold more than one name, because a chain stores its full copy, each change piece, and its chain log under a name of its own. A run whose destination refuses an operation fails with `BACKUP_DESTINATION_ERROR`, naming the piece and the name it belongs to.

Reassembly writes each piece at `index * pieceBytes`, so a gap SQLite leaves unwritten reads back as zeros rather than moving every later byte. Reassembly checks the pieces it finds against the run report that wrote them, and fails with `BACKUP_DESTINATION_ERROR` on a missing index, on a piece beyond the run's `pieceCount`, on a byte total other than `bytesWritten`, and on a fingerprint other than the one recorded, because a name reused by a later, smaller run leaves the earlier run's trailing pieces in place.

```text
BackupToDestinationOptions {
  destination:          BackupDestination
  name?:                string  (default: backup-{ISO timestamp}.db)
  chainId?:             string  (default: an identifier the run mints)
  pieceBytes?:          number  (default: 16 MiB, recommended)
  pagesPerStep?:        number  (default: 256, recommended)
  restartLimit?:        number  (default: 3, recommended)
  stallTimeoutMs?:      number  (default: 30000, recommended)
  noProgressStepLimit?: number  (default: 256, recommended)
  stagingDir?:          string  (default: the host temporary directory)
  fingerprint?:         boolean (default: true)
  onProgress?:          (progress: BackupProgress) -> void
}
```

An implementation that cannot deliver the copy to the destination as SQLite writes it takes the staged route, which writes one local file and sends that file on in pieces. The staged route needs local disk equal to the backup, and the capability report states that requirement. The journal SQLite opens beside a copy stays with the implementation that writes it and reaches no destination, so every run reports one name.

### Reports

`onProgress` is called at step resolution while the copy runs and once per piece while the pieces travel.

```text
BackupProgress {
  runId:          string
  phase:          'copy' | 'transfer'
  totalPages:     number
  remainingPages: number
  restarts:       number
  piecesWritten:  number
  bytesWritten:   number
}

BackupRunReport {
  runId:           string
  databaseId:      string
  sourcePath:      string
  kind:            'full' | 'change'
  chainId:         string
  route:           'staged' | 'streamed'
  destinationName: string
  startedAt:       number
  finishedAt:      number
  durationMs:      number
  copyMs:          number
  transferMs:      number
  pageCount:       number
  pageSize:        number
  bytesWritten:    number
  pieceCount:      number
  pieceBytes:      number
  restarts:        number
  position?:       BackupChainPosition
  fingerprint?:    string
}
```

The fingerprint is the SHA-256 of what the run wrote, and a caller turns it off where the read it costs is not worth its price.

`chainId` names the chain the run belongs to; a full copy begins one and every change piece extends it. A change capture reports `kind: 'change'`, sets `position` to the frames it took, and counts them in `pageCount`. A full copy carries no `position`.

### Change capture

A database opened with `backups` captures its write-ahead log on an interval, then checkpoints it. Those backups form a chain: one full copy, then one change piece per capture.

```text
BackupCycleOptions {
  destination:          BackupDestination
  intervalMs?:          number  (default: 60000, recommended)
  fullCopyIntervalMs?:  number  (default: 86400000, recommended)
  chainName?:           string  (default: sirannon-backup-chain)
  namePrefix?:          string  (default: sirannon-backup)
  pieceBytes?:          number  (default: 16 MiB, recommended)
  fingerprint?:         boolean (default: true)
  stagingDir?:          string  (default: a directory beside the database file)
  pagesPerStep?:        number
  restartLimit?:        number
  stallTimeoutMs?:      number
  noProgressStepLimit?: number
  onRun?:               (report: BackupRunReport) -> void
  onError?:             (error) -> void
}
```

Such a database opens its writer with `walAutoCheckpoint` at zero. An in-memory database and one outside WAL mode both fail with `BACKUP_UNSUPPORTED`.

Each turn of the cycle runs in this order:

1. Send any capture still waiting.
2. Under the writer lock, stage the log frames written since the previous capture.
3. Checkpoint the log under that same lock.
4. Send the staged capture and append its chain record.

A capture that fails stops the checkpoint behind it. A staged capture reaches the destination before the next capture runs. The first turn after a chain passes `fullCopyIntervalMs` starts a fresh chain.

The first capture of a chain starts at frame one and carries the log header. Pieces covering one run of the log hold contiguous frames, so they concatenate into a log SQLite recovers from.

Every capture compares the log's salts against the ones the chain last recorded. Where they differ and the implementation ran no checkpoint, the capture fails with `BACKUP_LOG_REWOUND` and the implementation starts a fresh chain.

A database captures its log a final time during close, after its writes drain and before its pool closes.

### Chain records

An implementation stores the chain at the destination, never inside the database, and never changes a record it has stored.

```text
BackupChainPosition {
  logSequence: number
  salt1:       number
  salt2:       number
  firstFrame:  number
  lastFrame:   number
}

BackupChainBase {
  kind:         'full'
  chainId:      string
  name:         string
  runId:        string
  finishedAt:   number
  pieceCount:   number
  pieceBytes:   number
  bytesWritten: number
  fingerprint?: string
}

BackupChainChange {
  kind:         'change'
  chainId:      string
  name:         string
  runId:        string
  sequence:     number
  position:     BackupChainPosition
  capturedAt:   number
  frameCount:   number
  pieceCount:   number
  pieceBytes:   number
  bytesWritten: number
  checkpointed: boolean
  fingerprint?: string
}

BackupChainRecord = BackupChainBase or BackupChainChange

BackupChain {
  chainId:          string
  startedAt:        number
  previousChainId?: string
  base?:            BackupChainBase
  changes:          List<BackupChainChange>
}
```

The list of chains holds one record per chain under `chainName`, oldest at index zero. Each chain holds its own records under `{chainName}.{chainId}`, its full copy at index zero and each change piece at its `sequence`. A full copy's record reaches the destination before its chain joins the list.

`backupChain` returns the chains newest first, each with its own records oldest first. A chain whose full copy the destination no longer holds carries no `base`.

### Restore selection

```text
BackupRestorePlan {
  chainId:    string
  base:       BackupChainBase
  changes:    List<BackupChainChange>
  restoresTo: number
}

BackupSafeToDeleteOptions {
  restorableFrom?: number
}
```

`backupRestorePlan(moment)` selects the newest full copy finished at or before `moment`, then every change piece of that chain captured at or before it, in `sequence` order. `restoresTo` is the last selected piece's `capturedAt`, or the full copy's `finishedAt` where the plan selects none. A moment no full copy reaches, and a gap in the selected sequence, both fail with `BACKUP_CHAIN_BROKEN` naming the missing piece.

`backupPiecesSafeToDelete(options?)` reports every record of a chain whose full copy is absent, and every change piece after a gap. Given `restorableFrom`, it also reports every record of a chain older than the newest full copy finished at or before that moment. It deletes nothing.

### Capability report

`backupCapabilities()` states which backup operations the runtime supports, so a caller learns before a run rather than when one fails.

```text
BackupCapabilities {
  fullCopy:          boolean
  streamedCopy:      boolean
  stagedCopy:        boolean
  localDiskRequired: 'none' | 'equal-to-backup'
  schedule:          boolean
}
```

A runtime that hands over whole databases only reports `fullCopy: false`, a runtime taking the staged route reports `localDiskRequired: 'equal-to-backup'`, and `schedule` follows `fullCopy`, because a scheduled run makes a full copy.

### Schedule

```text
BackupScheduleOptions {
  cron:      string
  destDir:   string
  maxFiles?: number   (default: 5, recommended)
  timezone?: string   (IANA name; default: host time zone)
  onError?:  (error) -> void
}
```

`scheduleBackup` runs on the cron schedule, backs up into `destDir`, and rotates files matching `backup-*.db` beyond `maxFiles` by modification time. The cron expression is evaluated in `timezone` when supplied, otherwise the host zone. The scheduler checks the time on a recurring tick and does not backfill: a scheduled time skipped while the host sleeps or the clock jumps forward is not run late, and a backward clock step repeats nothing until real time passes the last completed backup. Across a daylight-saving forward transition the missing hour is skipped; across a backward transition a time in the repeated hour runs once.

---

## Extensions

`loadExtension(extensionPath)` loads a compiled SQLite extension into a database under the writer lock. A load that cannot complete fails with `EXTENSION_ERROR`.

The call proceeds in this order:

1. Reject an empty path, a path holding a null byte or a control character, and a path holding a `..` segment.
2. Reject the call when any connection exposes no `loadExtension`.
3. Reject the call when the driver reports `extensions: true` and supplies no `resolveExtensionPath`.
4. Resolve the path through `resolveExtensionPath` where the driver supplies one.
5. Reject a relative path from that resolver.
6. Call each connection's own `loadExtension` with the resolved path, never the SQL `load_extension` function.

A driver reporting `extensions: false` refuses at step 6, and the message must state which runtime cannot load an extension. A missing file and a runtime that cannot load an extension must produce different messages.

The load must apply to the writer, every reader, and every further connection the database has open. A connection the database opens afterwards must load every extension already loaded. A writer worker that restarts must load them onto its new connection. Loading and opening must not interleave.

SQLite cannot unload an extension, so a load that fails part-way leaves it on the connections loaded before the failure.

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

Metrics callbacks are optional and configured only when `metrics` is supplied. `onConnectionClose` reports `readerCount` as 0. An error thrown by a metrics callback must not affect database operations.
