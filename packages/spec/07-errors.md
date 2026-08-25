# Sirannon Error Specification

Error codes that cross the wire (server responses, transport messages) are normative: all implementations must use the same code strings. Internal error handling follows language idioms (exception classes, sentinel errors, enums, variants) but should map to these code strings when an error crosses a module boundary or reaches the wire.

---

## Error Format

A server HTTP response and a WebSocket error message use:

```json
{ "error": { "code": "ERROR_CODE", "message": "Human-readable description", "details": {} } }
```

`code` is a stable machine-readable string. `message` is informational and may vary across implementations; client code must not match against it. `details` is optional and carries machine-readable context.

An implementation provides a base error type carrying a `code` string. When an error carries extra context, it should attach it as properties: `sql` on `QUERY_ERROR`, `table` and `rowId` on `CONFLICT_ERROR`, `version` on a migration error, `limit` and `retryAfterMs` on `WRITE_OVERLOADED`, `requestId` on `SYNC_ERROR`, and `serverVersion` in `details` on `MIGRATION_REQUIRED` and `SCHEMA_AHEAD`.

---

## Core

| Code | Description |
|------|-------------|
| `DATABASE_NOT_FOUND` | Database ID is not registered and cannot be resolved. |
| `DATABASE_ALREADY_EXISTS` | An ID already in use was registered again. |
| `DATABASE_CLOSED` | An operation was attempted on a closed database. |
| `DATABASE_OPEN_FAILED` | A database could not be opened. |
| `READ_ONLY` | A write was attempted on a read-only database. |
| `QUERY_ERROR` | SQLite failed to prepare or execute a statement. |
| `FORBIDDEN_SQL` | A statement reached a reserved `_sirannon` table, modified the `sqlite_` catalogue, or used `ATTACH`, `DETACH`, or `PRAGMA writable_schema`. |
| `TRANSACTION_ERROR` | A transaction could not be committed or was rolled back. |
| `HOOK_DENIED` | A hook rejected the operation. |
| `CDC_ERROR` | The change-data-capture pipeline hit an unrecoverable error. |
| `BACKUP_ERROR` | A backup operation failed. |
| `BACKUP_UNSUPPORTED` | The driver provides no backup engine, the runtime carries no stepped backup interface, or a database asked to capture its change log holds none. |
| `BACKUP_RESTARTED` | SQLite returned the copy to page one more often than the restart limit allows, or the copy reached no further across the no-progress step limit. |
| `BACKUP_STALLED` | The copy moved no pages inside the stall deadline. |
| `BACKUP_DESTINATION_ERROR` | The caller's destination refused a piece, did not answer inside the deadline, or holds pieces that do not match the run that wrote them. |
| `BACKUP_LOG_REWOUND` | The write-ahead log restarted before a capture read it, so those writes are in no backup. |
| `BACKUP_CHAIN_BROKEN` | No full copy goes back as far as the moment asked for, the chain is missing a piece the restore needs, or no chain record states the name the caller asked to verify. |
| `BACKUP_RESTORE_NOT_ACCEPTED` | The server was configured with `acceptBackupRestore` false, so it restores no database over the wire. |
| `BACKUP_RESTORE_IN_PROGRESS` | A restore of that database is already under way and keeps its file to itself. |
| `ENCRYPTION_UNSUPPORTED` | The driver reports no encryption capability for the runtime it wraps. |
| `INVALID_ENCRYPTION` | The master key is shorter than 32 bytes, or an encrypted database reports a journal mode other than `wal`. |
| `ENCRYPTION_KEY_UNAVAILABLE` | The key provider threw, or it returned fewer than 32 bytes. |
| `ENCRYPTION_KEY_MISSING` | A copy of an encrypted database named a destination holding no data key. |
| `ENCRYPTION_REQUIRED` | A group configured with `requireEncryption` reached a node whose database for that group is in plaintext. |
| `REENCRYPTION_IN_PROGRESS` | A re-encryption job for that database is already under way. |
| `ENCRYPTION_CONTROL_NOT_ACCEPTED` | The server was configured with `acceptEncryptionControl` false, so it changes no database's encryption over the wire. |
| `INVALID_ENCRYPTION_CONTROL` | `acceptEncryptionControl` is true and the server has no `authenticate` hook. |
| `CONNECTION_POOL_ERROR` | The pool is closed, exhausted, or misconfigured. |
| `MAX_DATABASES` | Opening a database would exceed the configured cap. |
| `EXTENSION_ERROR` | A native SQLite extension could not be loaded. |
| `INVALID_DRIVER` | Driver configuration failed validation. |
| `INVALID_SYNCHRONOUS` | An unknown `synchronous` level was supplied. |
| `INVALID_DURABILITY` | An unknown bulk-load durability was supplied. |
| `DURABILITY_RESTORE_FAILED` | The writer durability could not be restored after a bulk load. |
| `SNAPSHOT_IN_PROGRESS` | A read or write was attempted while a device-sync snapshot load runs. |
| `SHUTDOWN` | An operation was attempted after registry shutdown. |
| `SHUTDOWN_ERROR` | One or more databases failed to close during shutdown. |
| `LIFECYCLE_DISPOSED` | A resolve was attempted after the lifecycle manager was disposed. |
| `INTERNAL_SCHEMA_ERROR` | An identifier, column type, or default for an internal table failed validation, or a schema version fell outside `PRAGMA user_version`. |

### Writer Worker

| Code | Description |
|------|-------------|
| `INVALID_WRITER_WORKER` | Writer-worker configuration failed validation. |
| `WRITER_WORKER_UNSUPPORTED` | The writer worker was enabled with a driver that cannot run the writer this way. |
| `WRITE_OVERLOADED` | A write was shed before starting; definite outcome, safe to retry, carries a retry-after hint. |
| `WRITER_WORKER_TIMEOUT` | The writer gave no outcome within twice the write deadline; indeterminate, reconcile before retrying a non-idempotent write. |
| `WRITER_WORKER_EXIT` | The writer crashed or exited and the in-flight write was rejected. |
| `WRITER_WORKER_FATAL` | The writer exceeded its restart budget; writes fail permanently. |
| `WRITER_WORKER_UNAVAILABLE` | A write was sent while no writer was available. |
| `WRITER_WORKER_CLOSED` | A write was sent after the writer was closed. |
| `WRITER_WORKER_POST_FAILED` | The host could not hand the operation to the writer. |
| `WRITER_WORKER_NO_PORT` | The writer entry point started outside a worker thread. |

A runtime whose writer isolation differs may raise further internal codes for its own mechanism; the outcome codes above are the normative surface.

### Migrations

| Code | Description |
|------|-------------|
| `MIGRATION_ERROR` | A migration step failed during execution. |
| `MIGRATION_VALIDATION_ERROR` | A migration definition failed validation. |
| `MIGRATION_DUPLICATE_VERSION` | Two migrations share a version. |
| `MIGRATION_NO_DOWN` | A rollback was requested for a migration with no `down`. |
| `MIGRATION_SOURCE_INVALID` | The registry migration source returned a non-list value. |
| `MIGRATION_CHECKSUM_MISMATCH` | An applied migration's stored checksum no longer matches its SQL. |
| `MIGRATION_BASELINE_GAP` | A history below a baseline lacks the bridging migrations. |
| `MIGRATION_CONCURRENT` | A concurrent migration attempt could not be resolved. |
| `MIGRATION_ROLLBACK_ERROR` | A rollback step failed. |

---

## Replication

| Code | Description |
|------|-------------|
| `REPLICATION_ERROR` | General replication failure (base code). |
| `CONFLICT_ERROR` | Conflict resolution failed for a table and row. |
| `TRANSPORT_ERROR` | Transport-level failure (peer unreachable, send failed). |
| `BATCH_VALIDATION_ERROR` | Batch checksum mismatch, schema violation, clock drift exceeded, or unsafe DDL. |
| `WRITE_CONCERN_ERROR` | Write concern not met within the timeout. |
| `READ_CONCERN_ERROR` | The requested read concern cannot be satisfied. |
| `TOPOLOGY_ERROR` | A write on a replica without forwarding, no primary available, or an unauthorised peer. |
| `SYNC_ERROR` | First-sync failure (timeout, manifest mismatch, batch ordering). |
| `COORDINATOR_UNAVAILABLE` | The coordinator cannot be reached or prove quorum authority. |
| `AUTHORITY_LOST` | A node lost primary or controller authority while handling work. |
| `STALE_PRIMARY` | A request, batch, sync message, or forwarded write used a stale primary term. |
| `NO_SAFE_PRIMARY` | No eligible in-sync replica can be promoted safely. |
| `NODE_NOT_IN_SYNC` | The node is alive but not in the group's in-sync set. |
| `NODE_DRAINING` | The node is in maintenance drain mode. |
| `PROTOCOL_VERSION_MISMATCH` | Node compatibility metadata is incompatible with the cluster. |
| `UNSAFE_RECOVERY_REQUIRED` | Automatic recovery needs explicit operator action. |

---

## Server

| Code | Description |
|------|-------------|
| `INVALID_REQUEST` | The request body structure is invalid. |
| `INVALID_JSON` | The request body or WebSocket message is not valid JSON. |
| `EMPTY_BODY` | The request body is empty. |
| `PAYLOAD_TOO_LARGE` | The request body or message exceeds the maximum size. |
| `INTERNAL_ERROR` | An unexpected error occurred during request handling. |
| `HOOK_ERROR` | The `authenticate` hook or `authorizeClusterStatus` threw. |
| `NOT_FOUND` | The route does not exist, or cluster status is absent or refused. |
| `INVALID_MAX_BODY_BYTES` | `maxBodyBytes` is not a positive integer the transport can enforce exactly. |
| `INVALID_WS_BACKPRESSURE` | `maxWebSocketBackpressureBytes` fails validation or is below `maxBodyBytes`. |
| `INVALID_BACKUP_RESTORE` | `acceptBackupRestore` is true and the server has no `authenticate` hook. |
| `BULK_LOAD_UNSUPPORTED` | The execution target provides no bulk load. |
| `UNKNOWN_QUERY` | No operation of that name is registered for the database. |
| `MISSING_ARGUMENT` | A declared argument was absent from the request. |
| `ARGUMENT_NOT_ALLOWED` | The caller supplied an argument the operation does not declare, or one the server fills from identity. |
| `IDENTITY_REQUIRED` | An operation fills an argument from identity and the request carries none. |
| `REGISTRY_MISMATCH` | A subscription echoed a registry digest this server does not serve. |
| `SQL_NOT_ACCEPTED` | The server does not accept SQL statements over the wire. |
| `UNSUPPORTED_SUBPROTOCOL` | A WebSocket upgrade offered no subprotocol the server supports. |
| `SYNC_UNSUPPORTED` | The execution target provides no change application, or the server predates device sync. |
| `INVALID_MESSAGE` | A WebSocket message is missing required fields or has wrong types. |
| `UNKNOWN_TYPE` | A WebSocket message has an unrecognised type. |
| `HANDLER_CLOSED` | The WebSocket handler is shutting down. |
| `DUPLICATE_SUBSCRIPTION` | A subscription with the same ID already exists on the connection. |
| `SUBSCRIPTION_NOT_FOUND` | An unsubscribe target does not exist. |
| `CDC_UNSUPPORTED` | Subscriptions require a file-based database, not an in-memory one. |

---

## Device Sync

| Code | Description |
|------|-------------|
| `MIGRATION_REQUIRED` | The device schema version is behind the server; it must migrate before syncing. |
| `SCHEMA_AHEAD` | The device schema version is ahead of the server; the server must migrate first. |
| `SNAPSHOT_UNSUPPORTED` | A snapshot was requested for an in-memory database. |
| `SNAPSHOT_CHECKSUM_MISMATCH` | A downloaded snapshot page failed checksum verification. |

---

## Client

| Code | Description |
|------|-------------|
| `CONNECTION_ERROR` | The client failed to connect to the server. |
| `UNAUTHORIZED` | The server refused the WebSocket upgrade as unauthenticated and closed with 4401. |
| `FORBIDDEN` | The server refused the WebSocket upgrade as not permitted and closed with 4403. |
| `TIMEOUT` | A request exceeded the configured timeout. |
| `TRANSPORT_ERROR` | The operation is not supported by the current transport. |
| `INVALID_RESPONSE` | The server returned a response that could not be parsed. |
| `ROUTING_ERROR` | The client could not discover a usable primary or read endpoint. |
| `NO_SAFE_PRIMARY` | Topology routing found no current primary for a write. |
| `INVALID_ARGUMENT` | A client argument failed validation. |
| `UNKNOWN_ERROR` | An error response carried no recognisable code. |

---

## Conventions

- Codes use `UPPER_SNAKE_CASE` and are stable across versions; removing or renaming one is a breaking change. New codes may be added in a minor version.
- `message` is not stable and must not be parsed.
- Mapping a code to an HTTP status uses the table in [05-server.md](05-server.md#http-status-codes); a code not listed defaults to 500.
