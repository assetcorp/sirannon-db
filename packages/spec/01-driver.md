# Sirannon Driver Specification

The driver abstraction wraps a SQLite engine (native binding, WebAssembly module, or runtime built-in) behind a uniform asynchronous interface. Every Sirannon implementation must support this contract, and community drivers must conform to it. All driver methods are asynchronous.

---

## SQLiteDriver

The top-level entry point. A driver must expose a `capabilities` descriptor and an `open` method. The remaining members are optional and enable features the runtime supports.

```text
SQLiteDriver {
  readonly capabilities: DriverCapabilities
  open(path: string, options?: OpenOptions): async -> SQLiteConnection

  readonly worker?: DriverWorkerEntry
  startWriterHost?(path: string, options: OpenOptions, hostOptions?: WorkerHostOptions): async -> SQLiteConnection
  createWriterContext?(): WriterContext
  createBackupEngine?(): BackupEngine
  resolveExtensionPath?(extensionPath: string): string
}
```

| Member | Purpose |
|--------|---------|
| `worker` | Points a worker thread at a module that rebuilds the driver, since `open` cannot cross a thread boundary. |
| `startWriterHost` | Opens the writer connection on a worker thread so slow disk work does not block the caller. Present only when the runtime has threads. See [Writer Worker](02-core.md#writer-worker). |
| `createWriterContext` | Produces a [WriterContext](#writercontext) that distinguishes the caller holding the writer from one waiting on it. |
| `createBackupEngine` | Produces a backup engine: it copies a database, schedules that copy, and builds the checkpoint cycle. See [Backups](02-core.md#backups). |
| `resolveExtensionPath` | Resolves an extension path to an absolute path, so the dynamic linker cannot search its own paths; required when `capabilities.extensions` is `true`. |

### DriverCapabilities

```text
DriverCapabilities {
  multipleConnections: boolean
  extensions: boolean
  steppedCopy: boolean
  rowCounts: boolean
  encryption: boolean
}
```

| Field | Description |
|-------|-------------|
| `multipleConnections` | The engine can open several independent connections to one database file. When `true`, Sirannon creates a pool with dedicated reader connections; when `false`, all reads and writes share one connection. |
| `extensions` | The engine can load native SQLite extensions; a driver reporting `true` must supply `resolveExtensionPath` and must return connections that expose `loadExtension`. |
| `steppedCopy` | The engine can copy an open database through SQLite's stepped backup interface; a driver reporting `true` must return connections that expose `copyDatabase`. |
| `rowCounts` | A driver reporting `true` must expose the counted statement calls, set `rowsRead` and `rowsWritten` by the rules in [Row Counts](02-core.md#row-counts) on every `RunResult`, `BatchSummary`, and statement error, and fail `open` with `ROW_COUNTS_UNSUPPORTED` when its engine has no row counters. |
| `encryption` | The engine encrypts every page of the database file and its log under a 32-byte key. A driver reporting `true` must accept `OpenOptions.encryption`, and a driver reporting `false` must fail an open that carries a key with `ENCRYPTION_UNSUPPORTED`. |

### OpenOptions

```text
OpenOptions {
  readonly?:           boolean          (default: false)
  walMode?:            boolean          (default: true)
  synchronous?:        SynchronousLevel (default: 'normal')
  walAutoCheckpoint?:  number           (default: unset, leaving SQLite's own threshold in force)
  encryption?:         { key: Bytes }   (default: unset, a plaintext database)
}

SynchronousLevel = 'off' | 'normal' | 'full' | 'extra'
```

| Field | Description |
|-------|-------------|
| `readonly` | Opens the database read-only. A write on a read-only connection must fail. |
| `walMode` | Enables WAL journal mode. |
| `synchronous` | Selects the `PRAGMA synchronous` durability level. |
| `walAutoCheckpoint` | Sets the frame count at which SQLite checkpoints the log on its own; `0` disables that. A driver applies it on every connection it opens, including one a restarted writer worker opens. |
| `encryption` | Carries the 32-byte key that opens an encrypted database. Core supplies it on every open, and the driver must keep no reference to the key once `open` returns. |

### open(path, options?)

Opens a connection to the database at `path`, or the special string `':memory:'` for an in-memory database. Every driver must configure each new connection:

```sql
PRAGMA journal_mode = WAL      -- when walMode is not false
PRAGMA synchronous = NORMAL    -- the level chosen by synchronous, NORMAL by default
PRAGMA foreign_keys = ON
PRAGMA wal_autocheckpoint = N  -- only when walAutoCheckpoint is set
```

The `synchronous` value maps to its pragma argument through a fixed allowlist (`off`, `normal`, `full`, `extra`); a value outside the allowlist must fail with error code `INVALID_SYNCHRONOUS`. A driver may set further pragmas the runtime needs; the reference drivers on native engines also set `PRAGMA busy_timeout = 5000`. On failure, the driver must throw an error carrying enough context to tell a missing file, a permission failure, and a corrupt database apart.

When `encryption` is set, the driver must apply the key before any other statement on the connection, set `PRAGMA temp_store = MEMORY`, and confirm that the engine encrypts, failing with `ENCRYPTION_UNSUPPORTED` when it reports no version. A SQLite3 Multiple Ciphers engine reports its version through `sqlite3mc_version()`, and a SQLCipher engine reports it through `PRAGMA cipher_version`. Each driver encrypts under one scheme with the raw 32-byte key: a driver built on SQLite3 Multiple Ciphers must use AEGIS-256, and a driver built on SQLCipher must use SQLCipher 4's default scheme. A file written under the other scheme must fail to open with `ENCRYPTION_KEY_REJECTED`, and the error must state that the file can also come from another scheme. Before the keyed open, the driver must find out whether the file is new, plaintext, or encrypted. A file that does not exist or holds no pages is new, and the driver must create it encrypted when a key is set. A plaintext file starts with the SQLite header string, and an encrypted file starts with its salt. A driver that cannot read the file's bytes must instead open a separate connection without a key, read the schema, and close that connection before the keyed open, treating a read that fails as not a database as an encrypted file. A key given for a plaintext file must fail with `DATABASE_NOT_ENCRYPTED`, an open without a key of an encrypted file must fail with `ENCRYPTION_KEY_MISSING`, and a key that fails the authentication check of the first page must fail with `ENCRYPTION_KEY_REJECTED`. A statement that reads any later page that fails its authentication check must fail with `ENCRYPTED_PAGE_CORRUPT`. An in-memory database must fail an open that carries a key with `ENCRYPTION_UNSUPPORTED`.

---

## SQLiteConnection

```text
SQLiteConnection {
  exec(sql: string): async -> void
  prepare(sql: string): async -> SQLiteStatement
  transaction<T>(fn: (conn: SQLiteConnection) -> async T): async -> T
  close(): async -> void
  loadExtension?(extensionPath: string): async -> void
  copyDatabase?(request: DatabaseCopyRequest): async -> DatabaseCopyStep

  runBatch?(sql: string, paramsBatch: List<List<any>>): async -> List<RunResult>
  runBatchSummary?(sql: string, paramsBatch: List<List<any>>): async -> BatchSummary
  runGroup?(units: List<{ statements: List<{ sql, params?, trusted? }> }>): async -> List<GroupRunOutcome>
}
```

- **exec** runs one or more statements that return no rows. Support for several semicolon-separated statements in one call is implementation-defined.
- **prepare** compiles a statement that can run many times. Callers should cache prepared statements to avoid reparsing.
- **transaction** runs `fn` inside a transaction, commits on normal return, and rolls back if `fn` throws. The connection passed to `fn` is the one that holds the transaction; a caller must not use the outer connection during it.
- **close** releases all resources. Every other method must throw afterwards. Closing an already-closed connection is a no-op.
- **loadExtension** loads the compiled extension at the absolute path `extensionPath` into this connection through the engine's own loading call, never through the SQL `load_extension` function. A connection whose runtime cannot load an extension must fail with `EXTENSION_ERROR`, and the message must state which runtime refuses.
- **copyDatabase** copies this connection's database to `destPath` through SQLite's stepped backup interface, moving `pagesPerStep` pages per step and calling `onStep` after each one. The implementation must yield to the runtime's event loop between steps so that other work on this connection runs in the gaps. A driver that steps the copy on another thread must apply `stallTimeoutMs` as the deadline for the next step from that thread, in place of any deadline of its own. SQLite copies no pages and reports success when a transaction is already open on the connection, so an implementation must reject that call with `BACKUP_ERROR` before starting. A connection whose runtime carries no stepped backup interface must fail with `BACKUP_UNSUPPORTED`. When the connection is encrypted, `copyDatabase` must write the destination under the connection's key.

```text
DatabaseCopyRequest {
  destPath:        string
  pagesPerStep:    number
  onStep?:         (step: DatabaseCopyStep) -> void
  stallTimeoutMs?: number
}

DatabaseCopyStep {
  totalPages:     number
  remainingPages: number
}
```

The optional methods let a driver accelerate bulk work:

| Method | Contract |
|--------|----------|
| `runBatch` | Runs `sql` once per parameter set, returning one `RunResult` each. |
| `runBatchSummary` | Runs `sql` once per parameter set, returning a `BatchSummary` rather than per-row results, to bound memory. |
| `runGroup` | Runs several independent units in one transaction, returning one outcome per unit in order. A unit that fails must not disturb the others. A unit statement marked `trusted` bypasses the reserved-identifier guard. |

```text
RunResult      { changes: number, lastInsertRowId: number or bigint, rowsRead?: number, rowsWritten?: number }
BatchSummary   { rowsLoaded: number, changes: number, rowsRead?: number, rowsWritten?: number }
GroupRunOutcome = { ok: true, results: List<RunResult> }
                or { ok: false, error: { message: string, name?: string, code?: string } }
```

`lastInsertRowId` is whatever the engine reports for the connection; engines with 64-bit rowids return a bigint for values beyond the safe integer range (see [Value Mapping](#value-mapping)).

---

## SQLiteStatement

```text
SQLiteStatement {
  all<T>(...params: any): async -> List<T>
  get<T>(...params: any): async -> T or null
  run(...params: any): async -> RunResult
  allRaw?<T>(...params: any): async -> List<T>

  allCounted?<T>(...params: any): async -> Counted<List<T>>
  getCounted?<T>(...params: any): async -> Counted<T or null>
  allRawCounted?<T>(...params: any): async -> Counted<List<T>>
}

Counted<V> { value: V, rowsRead: number, rowsWritten: number }
```

Parameters are passed as trailing arguments, not a single list.

- **all** runs the statement and returns every matching row as an object keyed by column name; an empty list when nothing matches.
- **get** returns the first matching row, or null (absent) when nothing matches.
- **run** runs the statement for its side effects and returns a `RunResult`.
- **allRaw** behaves like `all` but skips safe-range bigint narrowing, leaving every integer as a bigint. A driver may omit it; callers then fall back to `all`.
- **allCounted**, **getCounted**, and **allRawCounted** must return the value of `all`, `get`, or `allRaw` together with the row counts of that call alone; a driver must expose `allRawCounted` whenever it exposes `allRaw`.

---

## Value Mapping

A driver reads SQLite integers as a 64-bit type so values cross the boundary without rounding, then narrows any integer within the safe range [-(2^53 - 1), 2^53 - 1] back to a floating-point number. Only integers beyond that range surface as a bigint. This convention is normative: a conforming driver must return an exact 64-bit integer for a value outside the safe range, and a `RunResult.lastInsertRowId` follows the same rule.

The remaining mappings are: REAL to a floating-point number, TEXT to a string, SQL NULL to null, and BLOB to a byte array that round-trips exactly. SQLite has no boolean type; booleans are stored and read as the integers 0 and 1.

---

## Parameter Binding

Query and execution methods bind values safely. A driver must support positional parameters (values bound by position to `?` placeholders). Named-parameter binding (`:name`, `@name`, `$name`) is supported where the engine allows it. When parameters are omitted, the statement runs with no bound values. A driver must never interpolate parameter values into SQL; all binding goes through the engine's native mechanism.

---

## Driver Validation

An implementation should provide a `defineDriver` factory that validates a driver at construction time. It must verify that `capabilities` is present, that `open` is callable, and, when a `worker` entry is present, that its `specifier` is a string. On failure it throws with error code `INVALID_DRIVER`. The returned driver should be frozen against later mutation.

---

## Driver Conformance

A conforming driver satisfies these invariants:

1. **Lifecycle.** `open` returns a usable connection; `close` releases every resource; methods called after `close` throw.
2. **Statement correctness.** A prepared statement runs many times and reflects current database state. A statement prepared before a schema change may throw on execution; this is acceptable.
3. **Transaction atomicity.** `transaction` commits on success and rolls back on failure, with no partial state visible to other connections.
4. **WAL mode.** With `walMode`, the connection operates in WAL journal mode and concurrent reads do not block during writes.
5. **Read-only enforcement.** With `readonly`, any write fails.
6. **Integer fidelity.** An integer stored outside the safe range reads back as an exact 64-bit value, and one inside the range reads back as a number.
7. **BLOB fidelity.** A stored BLOB reads back byte-for-byte.
8. **Parameter safety.** All binding uses the engine's native mechanism; SQL string interpolation is forbidden.
