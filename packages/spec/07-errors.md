# Sirannon Error Specification

This document defines the error taxonomy across all Sirannon
modules. Error codes on the wire protocol (server responses,
transport messages) are normative; all implementations must use
the same codes. Internal error handling (exception classes,
sentinel values, error enums) follows language idioms but should
map to the same code strings.

---

## Error Format

### Wire Protocol Errors

All server HTTP responses and WebSocket error messages use this
format:

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "Human-readable description"
  }
}
```

The `code` field is a stable, machine-readable string. The
`message` field is informational and may vary across
implementations.

### Internal Errors

Implementations should provide a base error type that carries a
`code` string property. Language-specific patterns apply:

- TypeScript/JavaScript: `class SirannonError extends Error`
  with a `code` property.
- Go: sentinel errors or typed errors with a `Code()` method.
- Rust: an enum with variants per error category.
- Python: exception classes inheriting from `SirannonError`.

The `code` values must match the normative codes listed below
when errors cross module boundaries or reach the wire.

---

## Core Errors

Errors originating from the database registry, connection pool,
query execution, and related subsystems.

| Code | Name | Description |
|------|------|-------------|
| `DATABASE_NOT_FOUND` | DatabaseNotFoundError | Database ID is not registered and cannot be resolved. |
| `DATABASE_ALREADY_EXISTS` | DatabaseAlreadyExistsError | Attempted to register a database with an ID already in use. |
| `DATABASE_CLOSED` | SirannonError | Operation attempted on a closed database. |
| `DATABASE_OPEN_FAILED` | SirannonError | Database could not be opened (pool creation failure). |
| `READ_ONLY` | ReadOnlyError | Write operation attempted on a read-only database. |
| `QUERY_ERROR` | QueryError | SQLite failed to prepare or execute a statement. |
| `TRANSACTION_ERROR` | TransactionError | Transaction could not be committed or was forcibly rolled back. |
| `MIGRATION_ERROR` | MigrationError | A migration step failed during execution. |
| `MIGRATION_VALIDATION_ERROR` | MigrationError | Migration definitions failed validation (bad version, duplicate, invalid name). |
| `MIGRATION_DUPLICATE_VERSION` | MigrationError | Two migrations share the same version number. |
| `MIGRATION_NO_DOWN` | MigrationError | Rollback requested but the migration has no `down` definition. |
| `HOOK_DENIED` | HookDeniedError | A before-hook rejected the operation. |
| `CDC_ERROR` | CDCError | The change-data-capture pipeline encountered an unrecoverable error. |
| `BACKUP_ERROR` | BackupError | A backup operation failed (path validation, VACUUM, rotation). |
| `CONNECTION_POOL_ERROR` | ConnectionPoolError | Pool is closed, exhausted, or misconfigured. |
| `MAX_DATABASES` | MaxDatabasesError | Opening a new database would exceed the configured cap. |
| `EXTENSION_ERROR` | ExtensionError | A native SQLite extension could not be loaded. |
| `INVALID_DRIVER` | SirannonError | Driver configuration failed validation. |
| `SHUTDOWN` | SirannonError | Operation attempted after the registry was shut down. |
| `SHUTDOWN_ERROR` | SirannonError | One or more errors occurred during registry shutdown. |

---

## Replication Errors

Errors originating from the replication engine, conflict
resolution, and sync protocol.

| Code | Name | Description |
|------|------|-------------|
| `REPLICATION_ERROR` | ReplicationError | General replication failure (base code). |
| `CONFLICT_ERROR` | ConflictError | Conflict resolution failed for a specific table and row. |
| `TRANSPORT_ERROR` | TransportError | Transport-level failure (peer unreachable, send failed). |
| `BATCH_VALIDATION_ERROR` | BatchValidationError | Batch checksum mismatch, schema violation, clock drift exceeded, or unsafe DDL. |
| `WRITE_CONCERN_ERROR` | WriteConcernError | Write concern quorum not met within the timeout. |
| `TOPOLOGY_ERROR` | TopologyError | Write on a replica without forwarding, no primary available, or unauthorised peer. |
| `SYNC_ERROR` | SyncError | First sync failure (timeout, manifest mismatch, batch ordering). |

---

## Server Errors

Errors originating from the HTTP and WebSocket server.

| Code | Name | Description |
|------|------|-------------|
| `INVALID_REQUEST` | Server error | Request body structure is invalid. |
| `INVALID_JSON` | Server error | Request body or WebSocket message is not valid JSON. |
| `EMPTY_BODY` | Server error | Request body is empty. |
| `PAYLOAD_TOO_LARGE` | Server error | Request body exceeds the maximum size. |
| `INTERNAL_ERROR` | Server error | An unexpected error occurred during request handling. |
| `INVALID_MESSAGE` | WebSocket error | WebSocket message is missing required fields or has wrong types. |
| `UNKNOWN_TYPE` | WebSocket error | WebSocket message has an unrecognised type. |
| `HANDLER_CLOSED` | WebSocket error | The WebSocket handler is shutting down. |
| `DUPLICATE_SUBSCRIPTION` | WebSocket error | A subscription with the same ID already exists on this connection. |
| `SUBSCRIPTION_NOT_FOUND` | WebSocket error | Unsubscribe target does not exist on this connection. |
| `CDC_UNSUPPORTED` | WebSocket error | Subscriptions require a file-based database, not `:memory:`. |

---

## Client Errors

Errors originating from the client SDK.

| Code | Name | Description |
|------|------|-------------|
| `CONNECTION_ERROR` | RemoteError | Failed to connect to the server. |
| `TIMEOUT` | RemoteError | Request exceeded the configured timeout. |
| `TRANSPORT_ERROR` | RemoteError | Operation not supported by the current transport (e.g., subscriptions over HTTP). |
| `INVALID_RESPONSE` | RemoteError | Server returned a response that could not be parsed as JSON. |

---

## Error Code Conventions

- Codes use `UPPER_SNAKE_CASE`.
- Codes are stable across versions. Removing or renaming a code is
  a breaking change to the specification.
- New codes may be added in minor spec versions.
- The `message` field is not stable; client code must not parse or
  match against message strings.
- When an error carries additional context (e.g., `table` and
  `rowId` on `CONFLICT_ERROR`, `version` on `MIGRATION_ERROR`,
  `sql` on `QUERY_ERROR`), implementations should attach these as
  properties on the error object in addition to the code and
  message.

---

## HTTP Status Code Assignment

When mapping error codes to HTTP status codes, use the table in
[05-server.md](05-server.md#http-status-code-mapping). If an error
code is not listed, default to status 500.
