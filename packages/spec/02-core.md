# Sirannon Core Specification

This document defines the core database management layer: the
registry that manages multiple named databases, the connection
pool, query execution with statement caching, change data capture,
hooks, lifecycle management, migrations, backups, and metrics
collection. All Sirannon implementations must follow the contracts
defined here.

---

## Registry (Sirannon)

The Sirannon registry is the top-level object that manages
multiple named databases. Each database is identified by a unique
string ID.

```text
Sirannon {
  constructor(options: SirannonOptions)

  open(id: string, path: string, options?: DatabaseOptions): async -> Database
  close(id: string): async -> void
  get(id: string): Database or null
  resolve(id: string): async -> Database or null
  has(id: string): boolean
  databases(): Map<string, Database>
  shutdown(): async -> void

  onBeforeQuery(hook: BeforeQueryHook): void
  onAfterQuery(hook: AfterQueryHook): void
  onBeforeConnect(hook: BeforeConnectHook): void
  onDatabaseOpen(hook: DatabaseOpenHook): void
  onDatabaseClose(hook: DatabaseCloseHook): void
}
```

### SirannonOptions

```text
SirannonOptions {
  driver:     SQLiteDriver
  hooks?:     HookConfig
  metrics?:   MetricsConfig
  lifecycle?: LifecycleConfig
}
```

The `driver` field is required. All other fields are optional.

### open(id, path, options?)

Opens a database file at `path` and registers it under `id`. If
`id` is already registered, throw with error code
`DATABASE_ALREADY_EXISTS`. If the registry has been shut down,
throw with error code `SHUTDOWN`.

The method creates a connection pool for the database, configures
CDC if needed, and invokes any registered `beforeConnect` and
`databaseOpen` hooks.

### close(id)

Closes the database registered under `id` and removes it from the
registry. If `id` is not found, throw with error code
`DATABASE_NOT_FOUND`. Invokes any registered `databaseClose` hooks.

### get(id)

Returns the database registered under `id`, or `undefined` if not
found. This is a synchronous lookup with no side effects.

### resolve(id)

Returns the database registered under `id`. If the database is not
found and a lifecycle resolver is configured, attempts to auto-open
the database using the resolver. Returns `undefined` if the
database cannot be resolved.

### shutdown()

Closes all open databases and marks the registry as shut down.
After shutdown, all methods except `databases()` must throw with
error code `SHUTDOWN`.

---

## Database

A single database instance backed by a SQLite file (or in-memory
database). Provides query execution, CDC subscriptions, migrations,
and backups.

```text
Database {
  readonly id: string
  readonly path: string
  readonly readOnly: boolean
  readonly closed: boolean
  readonly readerCount: number

  query<T>(sql: string, params?: Params, options?: QueryOptions): async -> List<T>
  queryOne<T>(sql: string, params?: Params, options?: QueryOptions): async -> T or null
  execute(sql: string, params?: Params, options?: QueryOptions): async -> ExecuteResult
  executeBatch(sql: string, paramsBatch: List<Params>, options?: QueryOptions): async -> List<ExecuteResult>
  transaction<T>(fn: (tx: Transaction) -> async T): async -> T

  watch(table: string): async -> void
  unwatch(table: string): async -> void
  on(table: string): SubscriptionBuilder

  migrate(migrations: List<Migration>): async -> MigrationResult
  rollback(migrations: List<Migration>, version?: number): async -> RollbackResult

  backup(destPath: string): async -> void
  scheduleBackup(options: BackupScheduleOptions): void

  loadExtension(extensionPath: string): async -> void

  onBeforeQuery(hook: BeforeQueryHook): void
  onAfterQuery(hook: AfterQueryHook): void
  close(): async -> void
}
```

### DatabaseOptions

```text
DatabaseOptions {
  readOnly?:        boolean     (default: false)
  readPoolSize?:    number      (default: 4, recommended)
  walMode?:         boolean     (default: true)
  cdcPollInterval?: number      (default: 50, milliseconds, recommended)
  cdcRetention?:    number      (default: 3_600_000, milliseconds, recommended)
}
```

### Query Parameters

```text
Params = Map<string, any> or List<any>
```

Named parameters (object) or positional parameters (array). See
[01-driver.md](01-driver.md#parameter-binding) for binding rules.

### QueryOptions

```text
QueryOptions {
  writeConcern?: WriteConcern
  readConcern?:  ReadConcern
}

WriteConcern {
  level:      'local' | 'majority' | 'all'
  timeoutMs?: number
}

ReadConcern {
  level: 'local' | 'majority' | 'linearizable'
}
```

Write concern levels control replication durability guarantees.
Read concern levels are reserved for future use.

### ExecuteResult

```text
ExecuteResult {
  changes:          number
  lastInsertRowId:  number or bigint
}
```

### query(sql, params?, options?)

Executes a read query and returns all matching rows. Uses a reader
connection from the pool. Fires `beforeQuery` hooks before
execution and `afterQuery` hooks after execution.

### execute(sql, params?, options?)

Executes a write statement (INSERT, UPDATE, DELETE). Uses the
writer connection. Throws with error code `READ_ONLY` on read-only
databases. Fires query hooks.

### executeBatch(sql, paramsBatch, options?)

Executes the same SQL statement multiple times, once per entry in
`paramsBatch`. Uses the writer connection. Returns one
`ExecuteResult` per parameter set.

### transaction(fn)

Runs `fn` inside a SQLite transaction on the writer connection.
Commits on success, rolls back on failure. Throws with error code
`READ_ONLY` on read-only databases.

### watch(table)

Installs CDC triggers on the named table and starts polling for
changes. See the [CDC](#change-data-capture-cdc) section.

### on(table)

Returns a `SubscriptionBuilder` for the named table. Calling
`on(table)` does not install triggers; triggers are installed when
the first subscription is created.

### close()

Stops CDC polling, cancels scheduled backups, closes the connection
pool, and invokes close listeners. After calling `close()`, all
methods must throw with error code `DATABASE_CLOSED`.

---

## Connection Pool

The connection pool maintains a dedicated writer connection and a
set of reader connections for concurrent read access.

```text
ConnectionPool {
  static create(options: ConnectionPoolOptions): async -> ConnectionPool

  acquireReader(): SQLiteConnection
  acquireWriter(): SQLiteConnection
  close(): async -> void

  readonly readerCount: number
  readonly isReadOnly: boolean
}
```

### Pool Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `readPoolSize` | 4 (recommended) | Number of reader connections. |
| `readOnly` | `false` | Skip creating a writer connection. |
| `walMode` | `true` | Enable WAL mode on the writer. |

### Pool Creation Rules

1. If `readOnly` is `false`, create one writer connection with WAL
   mode enabled.
2. If the driver reports `capabilities.multipleConnections = true`,
   create `max(readPoolSize, 1)` reader connections. Each reader is
   opened with `readonly = true`.
3. If the driver does not support multiple connections, create zero
   readers. All reads go through the writer.

### acquireReader()

Returns the next reader connection using round-robin selection. If
no readers exist, returns the writer connection. Throws with error
code `CONNECTION_POOL_ERROR` if the pool is closed.

### acquireWriter()

Returns the writer connection. Throws with error code
`CONNECTION_POOL_ERROR` if the pool is closed or if the pool is
read-only.

---

## Query Execution and Statement Caching

Sirannon caches prepared statements to avoid repeated parsing.

### Cache Behavior

- Each connection maintains its own statement cache.
- The recommended cache capacity is 128 statements.
- When the cache exceeds capacity, the oldest entry is evicted.
- Failed statement preparation removes the entry from the cache.
- The cache is implementation-defined in its eviction strategy, but
  must produce correct results for repeated queries.

### Parameter Normalisation

Before passing parameters to a prepared statement, normalise them:

- `undefined` or omitted: empty array.
- Array: pass as-is.
- Object (named parameters): wrap in a single-element array
  `[params]` for engines that expect positional binding of named
  parameter objects.

---

## Change Data Capture (CDC)

CDC records row-level changes using SQLite triggers that write to
a tracking table. A polling loop reads new changes and dispatches
them to subscribers.

### Changes Table

The tracking table is named `_sirannon_changes`. When replication
is active, the table includes additional columns for node tracking:

```sql
CREATE TABLE IF NOT EXISTS _sirannon_changes (
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

The `node_id`, `tx_id`, and `hlc` columns are present when
replication is enabled. Without replication, implementations may
omit these columns.

Implementations must create these indexes:

```sql
CREATE INDEX IF NOT EXISTS idx__sirannon_changes_changed_at
  ON _sirannon_changes (changed_at)
```

### Trigger Installation

For each watched table, three `AFTER` triggers must be installed:

- `_sirannon_trg_{table}_insert` (AFTER INSERT)
- `_sirannon_trg_{table}_update` (AFTER UPDATE)
- `_sirannon_trg_{table}_delete` (AFTER DELETE)

Each trigger inserts a row into `_sirannon_changes` with:

- `table_name`: the watched table name.
- `operation`: `'INSERT'`, `'UPDATE'`, or `'DELETE'`.
- `row_id`: the primary key value(s) of the affected row,
  serialised as a JSON string when composite.
- `new_data`: a JSON object of all column values from `NEW`
  (INSERT, UPDATE).
- `old_data`: a JSON object of all column values from `OLD`
  (UPDATE, DELETE).

### Identifier Validation

Table and column names used in trigger SQL must match the pattern
`/^[a-zA-Z_][a-zA-Z0-9_]*$/`. Implementations must reject names
that do not match, to prevent SQL injection through dynamic
identifier construction.

### Polling

The polling loop reads new changes from `_sirannon_changes` where
`seq > lastSeq`. The recommended poll interval is 50 milliseconds.
The recommended batch size per poll is 1000 rows.

If no subscribers are active, the polling loop should skip the
query to avoid unnecessary I/O.

### Cleanup

Old change records must be cleaned up periodically. The
recommended retention period is 1 hour (3,600,000 milliseconds).
Cleanup deletes rows where `changed_at` is older than the
retention threshold.

The recommended cleanup frequency is every 100 poll ticks.

### Change Events

```text
ChangeEvent<T> {
  type:       'insert' | 'update' | 'delete'
  table:      string
  row:        T
  oldRow?:    T
  seq:        bigint
  timestamp:  number
}
```

The `oldRow` field is present for update and delete events.

---

## Subscriptions

Subscriptions allow callers to receive CDC events for a specific
table with optional filtering.

```text
SubscriptionBuilder {
  filter(conditions: Map<string, any>): SubscriptionBuilder
  subscribe(callback: (event: ChangeEvent) -> void): Subscription
}

Subscription {
  unsubscribe(): void
}
```

### Filter Matching

When a filter is provided, events are matched by comparing each
filter key-value pair against the event's row data. For delete
events, the filter is matched against `oldRow` instead of `row`.
All filter conditions must match for the event to be delivered.

### Error Isolation

Errors thrown by subscription callbacks must not prevent delivery
to other subscribers. Implementations must catch and suppress
callback errors.

---

## Hook System

Hooks provide event-driven extensibility points for database
operations. Hooks registered on the Sirannon registry apply to all
databases. Hooks registered on a specific database apply only to
that database.

### Hook Events

| Event | Context | Invocation | Can Deny? |
|-------|---------|------------|-----------|
| `beforeQuery` | `{ databaseId, sql, params?, writeConcern?, readConcern? }` | Before query execution | Yes (throw to deny) |
| `afterQuery` | `{ databaseId, sql, params?, durationMs }` | After query execution | No |
| `beforeConnect` | `{ databaseId, path }` | Before connection opens | Yes (throw to deny) |
| `databaseOpen` | `{ databaseId, path }` | After database opens | No |
| `databaseClose` | `{ databaseId, path }` | After database closes | No |
| `beforeSubscribe` | `{ databaseId, table, filter? }` | Before subscription creates | Yes (throw to deny) |

### Before-Hook Denial

Before-hooks (beforeQuery, beforeConnect, beforeSubscribe) can
deny an operation by throwing. The thrown error propagates to the
caller. When a before-hook throws, implementations must throw with
error code `HOOK_DENIED`.

### Hook Registration

Hooks are registered via dedicated methods (e.g., `onBeforeQuery`)
or through a `HookConfig` object at construction time. The config
object accepts either a single hook function or an array of hook
functions per event.

Registration returns a dispose function that removes the hook.
Calling dispose more than once is a no-op.

### Invocation Order

Hooks for the same event are invoked in registration order.
Registry-level hooks run before database-level hooks.

---

## Lifecycle Management

The lifecycle manager provides auto-open, idle timeout, and LRU
eviction for databases.

### LifecycleConfig

```text
LifecycleConfig {
  autoOpen?: {
    resolver: (id: string) -> { path: string, options?: DatabaseOptions } or null
  }
  idleTimeout?: number    (0 = disabled, milliseconds)
  maxOpen?:     number    (0 = unlimited)
}
```

### Auto-Open

When a `resolve(id)` call finds no registered database and a
resolver is configured, the lifecycle manager calls the resolver.
If the resolver returns a path and options, the database is
auto-opened and registered.

### Idle Timeout

When `idleTimeout` is greater than zero, the lifecycle manager
periodically checks for databases that have not been accessed
within the timeout window. Idle databases are closed automatically.

The recommended check interval is `min(max(floor(timeout / 2), 100), 60000)` milliseconds.

### LRU Eviction

When `maxOpen` is reached and a new database needs to open, the
lifecycle manager evicts the least-recently-used database to make
room. If no evictable database exists, throw with error code
`MAX_DATABASES`.

---

## Migrations

The migration system provides schema versioning with transactional
execution and optional rollback.

### Tracking Table

```sql
CREATE TABLE IF NOT EXISTS _sirannon_migrations (
  version    INTEGER PRIMARY KEY,
  name       TEXT NOT NULL,
  applied_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
)
```

### Migration Definition

```text
Migration {
  version: number    (positive integer)
  name:    string    (alphanumeric + underscores)
  up:      string | function(tx: Transaction) -> async void
  down?:   string | function(tx: Transaction) -> async void
}
```

### Migration Execution

1. Validate all migrations: version must be a positive safe
   integer, name must match `/^\w+$/`, no duplicate versions.
2. Query the tracking table for already-applied versions.
3. Filter to pending migrations, sorted by version ascending.
4. For each pending migration, execute inside a transaction:
   - If `up` is a string, execute it as SQL.
   - If `up` is a function, call it with a transaction handle.
   - Insert a tracking record.
5. Return the count of applied and skipped migrations.

If any migration fails, the transaction rolls back. Throw with
error code `MIGRATION_ERROR` and include the version number.

### Rollback

Rollback reverses applied migrations in descending version order.
If no target version is specified, only the latest migration is
rolled back. Each rollback executes the migration's `down` field
inside a transaction. If `down` is undefined, throw with error code
`MIGRATION_NO_DOWN`.

---

## Backups

Backups create point-in-time snapshots of a database file.

### backup(destPath)

Creates a backup using SQLite's `VACUUM INTO` command:

```sql
VACUUM INTO '{escaped_path}'
```

Path validation rules:

- Reject paths containing null bytes.
- Reject paths containing control characters (code points <= 0x1F).
- Reject paths containing `..` path traversal segments.
- Reject paths where the destination file already exists.

The parent directory is created recursively if it does not exist.
On failure, the implementation should make a best-effort attempt to
clean up the partial file before throwing with error code
`BACKUP_ERROR`.

### File Naming

The recommended filename format is `backup-{ISO timestamp}.db`,
with colons and periods replaced by hyphens.

### Rotation

The recommended rotation behaviour: given a directory and a
`maxFiles` count, list all files matching `backup-*.db`, sort by
modification time descending, and delete files beyond `maxFiles`.

### Scheduled Backups

```text
BackupScheduleOptions {
  cron:      string       (cron expression)
  destDir:   string
  maxFiles?: number       (default: 5, recommended)
  onError?:  (error: Error) -> void
}
```

Scheduled backups execute on the cron schedule, create a backup
in `destDir`, and rotate old files.

---

## Metrics

The metrics system provides optional telemetry callbacks for query
performance, connection events, and CDC activity.

### MetricsConfig

```text
MetricsConfig {
  onQueryComplete?:    (metrics: QueryMetrics) -> void
  onConnectionOpen?:   (metrics: ConnectionMetrics) -> void
  onConnectionClose?:  (metrics: ConnectionMetrics) -> void
  onCDCEvent?:         (metrics: CDCMetrics) -> void
}
```

### Metric Types

```text
QueryMetrics {
  databaseId:    string
  sql:           string
  durationMs:    number
  rowsReturned?: number
  changes?:      number
  error?:        boolean
}

ConnectionMetrics {
  databaseId:  string
  path:        string
  readerCount: number
  event:       'open' | 'close'
}

CDCMetrics {
  databaseId:      string
  table:           string
  operation:       'insert' | 'update' | 'delete'
  subscriberCount: number
}
```

### Error Isolation

Errors thrown by metrics callbacks must not affect database
operations. Implementations must catch and suppress callback errors.
