# Sirannon Driver Specification

This document defines the driver abstraction that Sirannon uses to
interact with SQLite engines. A driver wraps a specific SQLite
binding (native C library, WebAssembly module, runtime built-in)
behind a uniform async interface. All Sirannon implementations must
support this contract. Community drivers must conform to the same
interface.

---

## SQLiteDriver

The top-level entry point for a driver. Implementations must expose
a `capabilities` descriptor and an `open` method.

```text
SQLiteDriver {
  readonly capabilities: DriverCapabilities
  open(path: string, options?: OpenOptions): async -> SQLiteConnection
}
```

### DriverCapabilities

```text
DriverCapabilities {
  multipleConnections: boolean
  extensions: boolean
}
```

| Field | Description |
|-------|-------------|
| `multipleConnections` | Whether the engine supports opening multiple independent connections to the same database file. When `true`, Sirannon creates a separate connection pool with dedicated readers. When `false`, all reads and writes share a single connection. |
| `extensions` | Whether the engine supports loading native SQLite extensions via `load_extension`. |

### OpenOptions

```text
OpenOptions {
  readonly?: boolean
  walMode?: boolean
}
```

| Field | Default | Description |
|-------|---------|-------------|
| `readonly` | `false` | Open the database in read-only mode. Write operations on read-only connections must fail. |
| `walMode` | `true` | Enable WAL (Write-Ahead Logging) journal mode on the connection. |

### open(path, options?)

Opens a connection to the SQLite database at `path`. The path is
either a filesystem path or the special string `':memory:'` for an
in-memory database.

When `walMode` is `true`, the driver must execute:

```sql
PRAGMA journal_mode = WAL
```

All drivers must also configure these pragmas on every new
connection:

```sql
PRAGMA synchronous = NORMAL
PRAGMA foreign_keys = ON
PRAGMA busy_timeout = 5000
```

Drivers may set additional pragmas appropriate for the runtime, but
the four above are required.

On failure, the driver must throw an error. The error should carry
enough context for the caller to distinguish between a missing
file, a permission failure, and a corrupt database.

---

## SQLiteConnection

A single connection to a SQLite database. All methods are async.

```text
SQLiteConnection {
  exec(sql: string): async -> void
  prepare(sql: string): async -> SQLiteStatement
  transaction<T>(fn: (conn: SQLiteConnection) -> async T): async -> T
  close(): async -> void
}
```

### exec(sql)

Executes one or more SQL statements that do not return rows (e.g.,
`CREATE TABLE`, `PRAGMA`). Implementations may support multiple
semicolon-separated statements in a single call, but this behaviour
is implementation-defined. On failure, throw with the SQL string
and the engine's error message.

### prepare(sql)

Creates a prepared statement for the given SQL string. The returned
`SQLiteStatement` can be executed multiple times with different
parameters. Implementations should cache prepared statements where
possible to avoid repeated parsing.

### transaction(fn)

Executes `fn` inside a SQLite transaction. If `fn` returns
normally, the transaction is committed. If `fn` throws, the
transaction is rolled back and the error propagates to the caller.

The connection passed to `fn` is the same connection that holds the
transaction. Callers must not use the outer connection during the
transaction; doing so produces undefined behaviour in engines that
do not support nested connections.

### close()

Closes the connection and releases all resources. After calling
`close()`, all other methods on this connection must throw.
Calling `close()` on an already-closed connection is a no-op.

---

## SQLiteStatement

A prepared statement bound to a specific SQL string.

```text
SQLiteStatement {
  all<T>(...params: unknown[]): async -> Array<T>
  get<T>(...params: unknown[]): async -> T | undefined
  run(...params: unknown[]): async -> RunResult
}
```

### all(...params)

Executes the statement and returns all matching rows as an array
of objects. Column names become object keys. Returns an empty array
if no rows match.

### get(...params)

Executes the statement and returns the first matching row, or
`undefined` if no rows match.

### run(...params)

Executes the statement for its side effects (INSERT, UPDATE,
DELETE). Returns a `RunResult`.

### RunResult

```text
RunResult {
  changes: number
  lastInsertRowId: number | bigint
}
```

| Field | Description |
|-------|-------------|
| `changes` | Number of rows modified by the statement. |
| `lastInsertRowId` | The rowid of the last inserted row, or 0 if the statement did not insert. Engines that use 64-bit rowids may return a bigint. |

---

## Parameter Binding

All query and execution methods accept parameters for safe value
binding. Implementations must support two parameter styles:

**Positional parameters.** An array of values bound by position to
`?` placeholders in the SQL string.

**Named parameters.** An object whose keys map to `:name`, `@name`,
or `$name` placeholders in the SQL string.

When parameters are `undefined` or omitted, the statement executes
with no bound values.

Implementations must never interpolate parameter values into the
SQL string. All binding must go through the engine's native
parameter binding mechanism to prevent SQL injection.

---

## Driver Validation

Implementations should provide a `defineDriver` factory function
that validates a driver configuration at construction time.

```text
defineDriver(config: SQLiteDriver): SQLiteDriver
```

The function must verify:

- `config.capabilities` exists and contains both boolean fields.
- `config.open` is callable.

On validation failure, throw with error code `INVALID_DRIVER`.

The returned driver object should be frozen (immutable) to prevent
accidental modification after construction.

---

## Built-in Driver Requirements

The specification does not prescribe which drivers an
implementation must ship. Drivers are language-specific and runtime-
specific. However, every implementation must ship at least one
driver that passes the conformance checks described below.

---

## Driver Conformance

A conforming driver must satisfy these invariants:

1. **Connection lifecycle.** `open()` returns a usable connection.
   `close()` releases all resources. Methods called after `close()`
   must throw.

2. **Statement correctness.** `prepare()` returns a statement that
   can be executed multiple times. Each execution reflects the
   current database state. Stale prepared statements (those created
   before a schema change) may throw on execution; this is
   acceptable.

3. **Transaction atomicity.** `transaction()` commits on success and
   rolls back on failure. No partial state is visible to other
   connections.

4. **WAL mode.** When `walMode` is `true`, the connection must
   operate in WAL journal mode. Concurrent reads during writes must
   not block.

5. **Read-only enforcement.** When `readonly` is `true`, any write
   operation must fail with an error.

6. **Parameter safety.** All parameter binding must use the engine's
   native binding mechanism. String interpolation into SQL is
   forbidden.
