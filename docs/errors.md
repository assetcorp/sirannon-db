# Errors

Every error extends `SirannonError` with a machine-readable `code`. Match on the code; the message is informational and changes between releases.

```ts
import { QueryError } from '@delali/sirannon-db'

try {
  await db.execute('INSERT INTO users (id) VALUES (?)', [1])
} catch (err) {
  if (err instanceof QueryError) console.error(`SQL failed [${err.code}]: ${err.message}`, err.sql)
}
```

Some errors carry extra context: `sql` on `QUERY_ERROR`, `table` and `rowId` on `CONFLICT_ERROR`, `version` on a migration error, `limit` and `retryAfterMs` on `WRITE_OVERLOADED`, `requestId` on `SYNC_ERROR`, and `serverVersion` on `MIGRATION_REQUIRED` and `SCHEMA_AHEAD`.

Over the network, an HTTP response and a WebSocket error message carry the same shape:

```json
{ "error": { "code": "ERROR_CODE", "message": "Human-readable description", "details": {} } }
```

## Core engine

| Error | Code | When |
| --- | --- | --- |
| `DatabaseNotFoundError` | `DATABASE_NOT_FOUND` | The database ID is not registered and cannot be resolved |
| `DatabaseAlreadyExistsError` | `DATABASE_ALREADY_EXISTS` | An ID already in use was registered again |
| - | `DATABASE_CLOSED` | An operation ran against a closed database |
| - | `DATABASE_OPEN_FAILED` | A database could not be opened |
| `ReadOnlyError` | `READ_ONLY` | A write, or `live`, ran against a read-only database |
| `QueryError` | `QUERY_ERROR` | SQLite failed to prepare or execute a statement |
| `ForbiddenSqlError` | `FORBIDDEN_SQL` | A statement reached a `_sirannon` table, modified the `sqlite_` catalogue, or used `ATTACH`, `DETACH`, or `PRAGMA writable_schema` |
| `TransactionError` | `TRANSACTION_ERROR` | A transaction could not commit, or it rolled back |
| `HookDeniedError` | `HOOK_DENIED` | A before-hook rejected the operation |
| `RequestDeniedError` | the code you supply | The `authenticate` hook refused the request with a status and code of its own |
| `CDCError` | `CDC_ERROR` | The change-data-capture pipeline failed, or a statement cannot back a live query |
| `BackupError` | `BACKUP_ERROR` | A backup failed |
| - | `BACKUP_UNSUPPORTED` | The driver has no backup engine, or the database has no write-ahead log to capture |
| - | `BACKUP_RESTARTED` | Another connection sent the copy back to page one more often than the limit allows |
| - | `BACKUP_STALLED` | The copy moved no pages inside the stall deadline |
| - | `BACKUP_DESTINATION_ERROR` | Your destination refused a piece, or holds pieces that do not match the run that wrote them |
| - | `BACKUP_LOG_REWOUND` | The log restarted before the capture reached it, so those writes are in no backup |
| - | `BACKUP_CHAIN_BROKEN` | No full copy goes back as far as the moment you asked for, a piece the chain needs is missing, or no chain record states the name you asked to check |
| - | `BACKUP_RESTORE_NOT_ACCEPTED` | The server runs with `acceptBackupRestore` off, so it restores no database over the wire |
| - | `BACKUP_RESTORE_IN_PROGRESS` | A restore of that database is already under way and keeps its file to itself |
| `ConnectionPoolError` | `CONNECTION_POOL_ERROR` | The pool is closed, exhausted, or misconfigured |
| `MaxDatabasesError` | `MAX_DATABASES` | Opening a database would pass the configured cap |
| `ExtensionError` | `EXTENSION_ERROR` | A native SQLite extension could not be loaded |
| - | `INVALID_DRIVER` | Driver configuration failed validation |
| - | `INVALID_SYNCHRONOUS` | An unknown `synchronous` level was supplied |
| - | `INVALID_DURABILITY` | A load passed a `durability` other than `'off'` or `'normal'` |
| - | `DURABILITY_RESTORE_FAILED` | The load committed, then the writer failed before durability was restored |
| - | `SNAPSHOT_IN_PROGRESS` | A read or write ran while a device-sync snapshot load was replacing the database |
| - | `SHUTDOWN` | An operation ran after registry shutdown |
| - | `SHUTDOWN_ERROR` | One or more databases failed to close during shutdown |
| - | `LIFECYCLE_DISPOSED` | A resolve ran after the lifecycle manager was disposed |
| - | `INTERNAL_SCHEMA_ERROR` | An internal-table identifier, column type, or default failed validation, or a schema version fell outside `PRAGMA user_version` |

A dash in the class column means Sirannon raises the base `SirannonError` carrying that code, so match on `err.code` there.

## Writer worker

| Code | When | Retry? |
| --- | --- | --- |
| `WRITE_OVERLOADED` | More writes were pending than `maxPendingWrites` allows, or a queued write was shed when an earlier deadline expired. HTTP returns 503 with `Retry-After`. | Yes. The write never applied |
| `WRITER_WORKER_TIMEOUT` | The writer gave no outcome within twice `writeTimeoutMs` | Only after reconciling. The outcome is indeterminate |
| `WRITER_WORKER_EXIT` | The writer crashed or exited, and every write in flight was rejected | Only after reconciling. A write in flight may have committed before the crash |
| `WRITER_WORKER_FATAL` | The writer passed its restart budget, so writes now fail permanently | No. Restart the process |
| `WRITER_WORKER_UNAVAILABLE` | A write arrived while no writer was available | Yes. The write never reached the writer |
| `WRITER_WORKER_CLOSED` | A write arrived after the writer closed, or the writer closed while it was in flight | Reconcile first when the write was already in flight |
| `WRITER_WORKER_POST_FAILED` | The host could not hand the operation to the writer | Yes. The write never reached the writer |
| `WRITER_WORKER_NO_PORT` | The writer entry point started outside a worker thread | No. Fix the configuration |
| `WRITER_WORKER_UNSUPPORTED` | `writerWorker` was enabled on a driver with no worker entry, so the database refuses to open | No. Change the driver or the option |
| `INVALID_WRITER_WORKER` | A `writerWorker` value is out of range | No. Fix the configuration |

## Migrations

| Code | When |
| --- | --- |
| `MIGRATION_ERROR` | A migration step failed while running |
| `MIGRATION_VALIDATION_ERROR` | A migration definition failed validation |
| `MIGRATION_DUPLICATE_VERSION` | Two migrations share a version |
| `MIGRATION_NO_DOWN` | A rollback was requested for a migration carrying no `down` |
| `MIGRATION_SOURCE_INVALID` | The registry migration source returned something other than a list |
| `MIGRATION_CHECKSUM_MISMATCH` | An applied migration's stored checksum no longer matches its SQL |
| `MIGRATION_BASELINE_GAP` | A history below a baseline lacks the bridging migrations |
| `MIGRATION_CONCURRENT` | A concurrent migration attempt could not be resolved |
| `MIGRATION_ROLLBACK_ERROR` | A rollback step failed |

## Server and requests

| Code | When |
| --- | --- |
| `INVALID_REQUEST` | The request body structure is invalid |
| `INVALID_JSON` | The body or WebSocket message is not valid JSON |
| `EMPTY_BODY` | The request body is empty |
| `PAYLOAD_TOO_LARGE` | The body or message passed `maxBodyBytes` |
| `INTERNAL_ERROR` | An unexpected error occurred while handling the request |
| `HOOK_ERROR` | The `authenticate` hook or `authorizeClusterStatus` threw, or `authenticate` returned a refusal object rather than an identity |
| `NOT_FOUND` | The route does not exist, or cluster status is absent or refused |
| `INVALID_MAX_BODY_BYTES` | `maxBodyBytes` is not a positive integer the transport can enforce exactly |
| `INVALID_WS_BACKPRESSURE` | `maxWebSocketBackpressureBytes` failed validation or fell below `maxBodyBytes` |
| `INVALID_BACKUP_RESTORE` | `acceptBackupRestore` is on and the server has no `authenticate` hook |
| `BULK_LOAD_UNSUPPORTED` | The execution target provides no bulk load |
| `INVALID_MESSAGE` | A WebSocket message lacks a required field or carries a wrong type |
| `UNKNOWN_TYPE` | A WebSocket message carries an unrecognised type |
| `HANDLER_CLOSED` | The WebSocket handler is shutting down |
| `DUPLICATE_SUBSCRIPTION` | A subscription with the same ID already exists on the connection |
| `SUBSCRIPTION_NOT_FOUND` | An unsubscribe named a subscription that does not exist |
| `CDC_UNSUPPORTED` | Subscriptions need a file-based database, and this one is in memory |

## Registered operations

| Code | When |
| --- | --- |
| `UNKNOWN_QUERY` | No operation of that name is registered for the database |
| `MISSING_ARGUMENT` | A declared argument was absent from the request |
| `ARGUMENT_NOT_ALLOWED` | The caller supplied an undeclared argument, or one the server fills from identity |
| `IDENTITY_REQUIRED` | An operation fills an argument from identity and the request carries none |
| `REGISTRY_MISMATCH` | A live query echoed a registry digest this server does not serve |
| `SQL_NOT_ACCEPTED` | The server accepts no SQL over the network |
| `UNSUPPORTED_SUBPROTOCOL` | A WebSocket upgrade offered no subprotocol the server supports |

## Replication

| Error | Code | When |
| --- | --- | --- |
| `ReplicationError` | `REPLICATION_ERROR` | Base class for replication failures |
| `SyncError` | `SYNC_ERROR` | First sync failed: the node was not ready, the transfer timed out, or a manifest or batch order did not match |
| `ConflictError` | `CONFLICT_ERROR` | Conflict resolution failed for a table and row |
| `TransportError` | `TRANSPORT_ERROR` | A peer was unreachable or a send failed |
| `BatchValidationError` | `BATCH_VALIDATION_ERROR` | A batch failed its checksum, broke the schema allowlist, passed `maxClockDriftMs`, or carried unsafe DDL |
| `WriteConcernError` | `WRITE_CONCERN_ERROR` | The write concern was not met within the timeout |
| `ReadConcernError` | `READ_CONCERN_ERROR` | The requested read concern cannot be satisfied |
| `TopologyError` | `TOPOLOGY_ERROR` | A write reached a replica without forwarding, no primary was available, or a peer was unauthorised |
| `CoordinatorError` | `COORDINATOR_UNAVAILABLE` | The coordinator cannot be reached or cannot prove quorum authority |
| `AuthorityError` | `AUTHORITY_LOST` | A node lost primary or controller authority while handling work |
| `StalePrimaryError` | `STALE_PRIMARY` | A request, batch, sync message, or forwarded write used a stale primary term |
| `NoSafePrimaryError` | `NO_SAFE_PRIMARY` | No eligible in-sync replica can be promoted safely |
| `NodeNotInSyncError` | `NODE_NOT_IN_SYNC` | The node is alive but outside the group's in-sync set |
| `NodeDrainingError` | `NODE_DRAINING` | The node is in maintenance drain mode |
| `ProtocolVersionMismatchError` | `PROTOCOL_VERSION_MISMATCH` | Node compatibility metadata is incompatible with the cluster |
| `UnsafeRecoveryRequiredError` | `UNSAFE_RECOVERY_REQUIRED` | Automatic recovery needs explicit operator action |

Every class above is exported from `@delali/sirannon-db/replication`, and `FailoverError` is the shared base of `NoSafePrimaryError` and `UnsafeRecoveryRequiredError`.

## Device sync

| Code | When |
| --- | --- |
| `MIGRATION_REQUIRED` | The device schema version is behind the server, so the device migrates before it syncs |
| `SCHEMA_AHEAD` | The device schema version is ahead of the server, so the server migrates first |
| `SYNC_UNSUPPORTED` | The execution target applies no changes, or the server predates device sync |
| `SNAPSHOT_UNSUPPORTED` | A snapshot was requested for an in-memory database |
| `SNAPSHOT_CHECKSUM_MISMATCH` | A downloaded snapshot page failed checksum verification |

## Client

| Code | When |
| --- | --- |
| `CONNECTION_ERROR` | The client failed to connect to the server |
| `UNAUTHORIZED` | The server refused the WebSocket upgrade as unauthenticated and closed with 4401 |
| `FORBIDDEN` | The server refused the WebSocket upgrade as not permitted and closed with 4403 |
| `TIMEOUT` | A request passed the configured timeout |
| `TRANSPORT_ERROR` | The current transport does not carry this operation, such as a live query over HTTP |
| `INVALID_RESPONSE` | The server returned a response the client could not parse |
| `ROUTING_ERROR` | The client discovered no usable primary or read endpoint |
| `NO_SAFE_PRIMARY` | Topology routing found no current primary for a write |
| `INVALID_ARGUMENT` | A client argument failed validation, such as a per-call read concern on the topology transport |
| `UNKNOWN_ERROR` | An error response carried no recognisable code |

## Retrying

`WRITE_OVERLOADED` is definite load shedding: the write never ran, the response carries `Retry-After`, and the same request is safe to send again. `WRITER_WORKER_TIMEOUT` is indeterminate, so reconcile the state before you retry anything that is not idempotent.

`STALE_PRIMARY`, `AUTHORITY_LOST`, `COORDINATOR_UNAVAILABLE`, `NO_SAFE_PRIMARY`, and `CONNECTION_ERROR` mean the client's view of the cluster is out of date. The topology client refreshes its routing metadata and retries a read once; it raises a write instead, so you decide whether to send it again.

A validation code such as `INVALID_WRITER_WORKER`, `INVALID_MAX_BODY_BYTES`, or `INVALID_DRIVER` reports a configuration mistake, so fix the configuration rather than retrying.

`UNAUTHORIZED` and `FORBIDDEN` report a refused WebSocket upgrade. The client leaves that connection closed, so issue a fresh credential and build a new client rather than retrying the request.

## HTTP status codes

| Status | Codes |
| --- | --- |
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR`, `INVALID_DURABILITY`, `INVALID_SYNCHRONOUS`, `BATCH_VALIDATION_ERROR`, `MISSING_ARGUMENT`, `ARGUMENT_NOT_ALLOWED`, `UNSUPPORTED_SUBPROTOCOL` |
| 401 | `IDENTITY_REQUIRED` |
| 403 | `READ_ONLY`, `FORBIDDEN_SQL`, `HOOK_DENIED`, `SQL_NOT_ACCEPTED`, `BACKUP_RESTORE_NOT_ACCEPTED` |
| 404 | `DATABASE_NOT_FOUND`, `NOT_FOUND`, `UNKNOWN_QUERY` |
| 409 | `STALE_PRIMARY`, `PROTOCOL_VERSION_MISMATCH`, `MIGRATION_REQUIRED`, `SCHEMA_AHEAD`, `REGISTRY_MISMATCH`, `BACKUP_CHAIN_BROKEN`, `BACKUP_RESTORE_IN_PROGRESS` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR`, `HOOK_ERROR`, `WRITER_WORKER_TIMEOUT` |
| 501 | `BULK_LOAD_UNSUPPORTED`, `SYNC_UNSUPPORTED`, `BACKUP_UNSUPPORTED` |
| 502 | `BACKUP_DESTINATION_ERROR` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN`, `READ_CONCERN_ERROR`, `COORDINATOR_UNAVAILABLE`, `AUTHORITY_LOST`, `NO_SAFE_PRIMARY`, `NODE_NOT_IN_SYNC`, `NODE_DRAINING`, `UNSAFE_RECOVERY_REQUIRED`, `WRITE_OVERLOADED` |

A code outside the table maps to 500, and a `RequestDeniedError` uses the status you gave it.

The normative code list every implementation shares is in [`packages/spec/07-errors.md`](../packages/spec/07-errors.md).
