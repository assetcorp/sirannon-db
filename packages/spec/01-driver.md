# Sirannon Driver Specification

The driver abstraction wraps a SQLite engine (native binding, WebAssembly
module, or runtime built-in) behind a uniform asynchronous interface. Every
Sirannon implementation must support this contract, and community drivers must
conform to it. All driver methods are asynchronous.

---

## SQLiteDriver

The top-level entry point. A driver must expose a `capabilities` descriptor and
an `open` method. The remaining members are optional and enable features the
runtime supports.

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
| `createBackupEngine` | Produces a backup engine. See [Backups](02-core.md#backups). |
| `resolveExtensionPath` | Resolves an extension path to an absolute path, so `load_extension` cannot search the dynamic linker's own paths. |

### DriverCapabilities

```text
DriverCapabilities {
  multipleConnections: boolean
  extensions: boolean
}
```

| Field | Description |
|-------|-------------|
| `multipleConnections` | The engine can open several independent connections to one database file. When `true`, Sirannon creates a pool with dedicated reader connections; when `false`, all reads and writes share one connection. |
| `extensions` | The engine can load native SQLite extensions through `load_extension`. |

### OpenOptions

```text
OpenOptions {
  readonly?:     boolean          (default: false)
  walMode?:      boolean          (default: true)
  synchronous?:  SynchronousLevel (default: 'normal')
}

SynchronousLevel = 'off' | 'normal' | 'full' | 'extra'
```

| Field | Description |
|-------|-------------|
| `readonly` | Opens the database read-only. A write on a read-only connection must fail. |
| `walMode` | Enables WAL journal mode. |
| `synchronous` | Selects the `PRAGMA synchronous` durability level. |

### open(path, options?)

Opens a connection to the database at `path`, or the special string `':memory:'`
for an in-memory database. Every driver must configure each new connection:

```sql
PRAGMA journal_mode = WAL      -- when walMode is not false
PRAGMA synchronous = NORMAL    -- the level chosen by synchronous, NORMAL by default
PRAGMA foreign_keys = ON
```

The `synchronous` value maps to its pragma argument through a fixed allowlist
(`off`, `normal`, `full`, `extra`); a value outside the allowlist must fail with
error code `INVALID_SYNCHRONOUS`. A driver may set further pragmas the runtime
needs; the reference drivers on native engines also set `PRAGMA busy_timeout =
5000`. On failure, the driver must throw an error carrying enough context to
tell a missing file, a permission failure, and a corrupt database apart.

---

## SQLiteConnection

```text
SQLiteConnection {
  exec(sql: string): async -> void
  prepare(sql: string): async -> SQLiteStatement
  transaction<T>(fn: (conn: SQLiteConnection) -> async T): async -> T
  close(): async -> void

  runBatch?(sql: string, paramsBatch: List<List<any>>): async -> List<RunResult>
  runBatchSummary?(sql: string, paramsBatch: List<List<any>>): async -> BatchSummary
  runGroup?(units: List<{ statements: List<{ sql, params?, trusted? }> }>): async -> List<GroupRunOutcome>
}
```

- **exec** runs one or more statements that return no rows. Support for several
  semicolon-separated statements in one call is implementation-defined.
- **prepare** compiles a statement that can run many times. Callers should cache
  prepared statements to avoid reparsing.
- **transaction** runs `fn` inside a transaction, commits on normal return, and
  rolls back if `fn` throws. The connection passed to `fn` is the one that holds
  the transaction; a caller must not use the outer connection during it.
- **close** releases all resources. Every other method must throw afterwards.
  Closing an already-closed connection is a no-op.

The optional methods let a driver accelerate bulk work:

| Method | Contract |
|--------|----------|
| `runBatch` | Runs `sql` once per parameter set, returning one `RunResult` each. |
| `runBatchSummary` | Runs `sql` once per parameter set, returning a `BatchSummary` rather than per-row results, to bound memory. |
| `runGroup` | Runs several independent units in one transaction, returning one outcome per unit in order. A unit that fails must not disturb the others. A unit statement marked `trusted` bypasses the reserved-identifier guard. |

```text
RunResult      { changes: number, lastInsertRowId: number or bigint }
BatchSummary   { rowsLoaded: number, changes: number }
GroupRunOutcome = { ok: true, results: List<RunResult> }
                or { ok: false, error: { message: string, name?: string, code?: string } }
```

`lastInsertRowId` is whatever the engine reports for the connection; engines
with 64-bit rowids return a bigint for values beyond the safe integer range
(see [Value Mapping](#value-mapping)).

---

## SQLiteStatement

```text
SQLiteStatement {
  all<T>(...params: any): async -> List<T>
  get<T>(...params: any): async -> T or null
  run(...params: any): async -> RunResult
  allRaw?<T>(...params: any): async -> List<T>
}
```

Parameters are passed as trailing arguments, not a single list.

- **all** runs the statement and returns every matching row as an object keyed
  by column name; an empty list when nothing matches.
- **get** returns the first matching row, or null (absent) when nothing matches.
- **run** runs the statement for its side effects and returns a `RunResult`.
- **allRaw** behaves like `all` but skips safe-range bigint narrowing, leaving
  every integer as a bigint. A driver may omit it; callers then fall back to
  `all`.

---

## Value Mapping

A driver reads SQLite integers as a 64-bit type so values cross the boundary
without rounding, then narrows any integer within the safe range
[-(2^53 - 1), 2^53 - 1] back to a floating-point number. Only integers beyond
that range surface as a bigint. This convention is normative: a conforming
driver must return an exact 64-bit integer for a value outside the safe range,
and a `RunResult.lastInsertRowId` follows the same rule.

The remaining mappings are: REAL to a floating-point number, TEXT to a string,
SQL NULL to null, and BLOB to a byte array that round-trips exactly. SQLite has
no boolean type; booleans are stored and read as the integers 0 and 1.

---

## Parameter Binding

Query and execution methods bind values safely. A driver must support positional
parameters (values bound by position to `?` placeholders). Named-parameter
binding (`:name`, `@name`, `$name`) is supported where the engine allows it.
When parameters are omitted, the statement runs with no bound values. A driver
must never interpolate parameter values into SQL; all binding goes through the
engine's native mechanism.

---

## Driver Validation

An implementation should provide a `defineDriver` factory that validates a driver
at construction time. It must verify that `capabilities` is present, that `open`
is callable, and, when a `worker` entry is present, that its `specifier` is a
string. On failure it throws with error code `INVALID_DRIVER`. The returned
driver should be frozen against later mutation.

---

## Driver Conformance

A conforming driver satisfies these invariants:

1. **Lifecycle.** `open` returns a usable connection; `close` releases every
   resource; methods called after `close` throw.
2. **Statement correctness.** A prepared statement runs many times and reflects
   current database state. A statement prepared before a schema change may throw
   on execution; this is acceptable.
3. **Transaction atomicity.** `transaction` commits on success and rolls back on
   failure, with no partial state visible to other connections.
4. **WAL mode.** With `walMode`, the connection operates in WAL journal mode and
   concurrent reads do not block during writes.
5. **Read-only enforcement.** With `readonly`, any write fails.
6. **Integer fidelity.** An integer stored outside the safe range reads back as
   an exact 64-bit value, and one inside the range reads back as a number.
7. **BLOB fidelity.** A stored BLOB reads back byte-for-byte.
8. **Parameter safety.** All binding uses the engine's native mechanism; SQL
   string interpolation is forbidden.
