# Errors

Every error extends `SirannonError` with a machine-readable `code`. Match on the code, because the message is for people to read and can change between releases.

```ts
import { QueryError } from '@delali/sirannon-db'

try {
  await db.execute('INSERT INTO users (id) VALUES (?)', [1])
} catch (err) {
  if (err instanceof QueryError) console.error(`SQL failed [${err.code}]: ${err.message}`, err.sql)
}
```

Some errors include extra fields: `sql` on `QUERY_ERROR`, `table` and `rowId` on `CONFLICT_ERROR`, `version` on a migration error, `limit` and `retryAfterMs` on `WRITE_OVERLOADED`, `requestId` on `SYNC_ERROR`, and `serverVersion` on `MIGRATION_REQUIRED` and `SCHEMA_AHEAD`.

Over the network, an HTTP response and a WebSocket error message use the same shape:

```json
{ "error": { "code": "ERROR_CODE", "message": "Human-readable description", "details": {} } }
```

## Core engine

| Error | Code | When |
| --- | --- | --- |
| `DatabaseNotFoundError` | `DATABASE_NOT_FOUND` | The registry has no database with that ID and cannot resolve one |
| `DatabaseAlreadyExistsError` | `DATABASE_ALREADY_EXISTS` | A caller registers an ID that is already in use |
| - | `DATABASE_CLOSED` | A caller uses a closed database |
| - | `DATABASE_OPEN_FAILED` | Sirannon cannot open a database |
| `ReadOnlyError` | `READ_ONLY` | A caller writes to a read-only database or calls `live` on one |
| `QueryError` | `QUERY_ERROR` | SQLite fails to prepare or execute a statement |
| `ForbiddenSqlError` | `FORBIDDEN_SQL` | A statement names a `_sirannon` table, modifies the `sqlite_` catalogue, or uses `ATTACH`, `DETACH`, or `PRAGMA writable_schema` |
| `TransactionError` | `TRANSACTION_ERROR` | SQLite refuses to commit a transaction whose statements all succeed, such as on a deferred foreign key |
| `HookDeniedError` | `HOOK_DENIED` | A before-hook throws to refuse the operation |
| `RequestDeniedError` | the code you supply | Your `authenticate` hook throws it to refuse a request with a status and code of your own |
| `CDCError` | `CDC_ERROR` | Change data capture fails, or Sirannon cannot maintain a live query for the statement |
| `BackupError` | `BACKUP_ERROR` | A backup fails |
| - | `BACKUP_UNSUPPORTED` | The driver has no backup engine, or the database has no write-ahead log to capture |
| - | `BACKUP_RESTARTED` | Writes from another connection restart the copy from page one more often than the limit allows |
| - | `BACKUP_STALLED` | The copy moves no pages within the stall deadline |
| - | `BACKUP_DESTINATION_ERROR` | Your destination fails to store a piece, or holds pieces that do not match the backup that wrote them |
| - | `BACKUP_LOG_REWOUND` | SQLite restarts the log before the capture copies it, so no backup holds those writes |
| - | `BACKUP_CHAIN_BROKEN` | No full copy is as old as the moment that you request, a piece that the chain needs is missing, or no chain record has the name that you ask to check |
| - | `BACKUP_RESTORE_NOT_ACCEPTED` | The server has `acceptBackupRestore` off, so it restores no database over the network |
| - | `BACKUP_RESTORE_IN_PROGRESS` | A restore of that database is already under way |
| `ConnectionPoolError` | `CONNECTION_POOL_ERROR` | The pool is closed, exhausted, or misconfigured |
| `MaxDatabasesError` | `MAX_DATABASES` | Opening a database would exceed the configured cap |
| `ExtensionError` | `EXTENSION_ERROR` | Sirannon cannot load a native SQLite extension |
| - | `INVALID_DRIVER` | The driver configuration fails validation |
| - | `INVALID_SYNCHRONOUS` | The caller supplies an unknown `synchronous` level |
| - | `INVALID_DURABILITY` | A load names a `durability` other than `'off'` or `'normal'` |
| - | `DURABILITY_RESTORE_FAILED` | The load commits, but the writer fails before it restores durability |
| - | `SNAPSHOT_IN_PROGRESS` | A caller reads or writes while a device-sync snapshot load replaces the database |
| - | `SHUTDOWN` | A caller uses the registry after it shuts down |
| - | `SHUTDOWN_ERROR` | One or more databases fail to close during shutdown |
| - | `LIFECYCLE_DISPOSED` | A caller resolves a database after the lifecycle manager shuts down |
| - | `INTERNAL_SCHEMA_ERROR` | An internal-table identifier, column type, or default fails validation, or a schema version is outside the `PRAGMA user_version` range |

A dash in the class column means that Sirannon raises the base `SirannonError` with that code, so match on `err.code` there.

## Writer worker

| Code | When | Retry? |
| --- | --- | --- |
| `WRITE_OVERLOADED` | More writes are pending than `maxPendingWrites` allows, or the writer sheds a queued write when an earlier deadline expires. The server answers 503 with `Retry-After`. | Yes, because the write never executes |
| `WRITER_WORKER_TIMEOUT` | The writer gives no outcome within twice `writeTimeoutMs` | Only after reconciling, because the outcome is indeterminate |
| `WRITER_WORKER_EXIT` | The writer crashes or exits, so the host rejects every write in flight | Only after reconciling, because a write in flight can commit before the crash |
| `WRITER_WORKER_FATAL` | The writer exhausts its restart budget, so every later write fails | No. Restart the process |
| `WRITER_WORKER_UNAVAILABLE` | The host receives a write while no writer is available | Yes, because the write never reaches the writer |
| `WRITER_WORKER_CLOSED` | The host receives a write after the writer closes, or the writer closes while the write is in flight | Reconcile first when the write is already in flight |
| `WRITER_WORKER_POST_FAILED` | The host cannot post the operation to the writer | Yes, because the write never reaches the writer |
| `WRITER_WORKER_NO_PORT` | The writer entry point starts outside a worker thread | No. Fix the configuration |
| `WRITER_WORKER_UNSUPPORTED` | You enable `writerWorker` on a driver with no worker entry, so the database refuses to open | No. Change the driver or the option |
| `INVALID_WRITER_WORKER` | A `writerWorker` value is out of range | No. Fix the configuration |

## Migrations

| Code | When |
| --- | --- |
| `MIGRATION_ERROR` | A migration step fails while Sirannon applies it |
| `MIGRATION_VALIDATION_ERROR` | A migration definition fails validation |
| `MIGRATION_DUPLICATE_VERSION` | Two migrations share a version |
| `MIGRATION_NO_DOWN` | A caller requests a rollback of a migration that has no `down` |
| `MIGRATION_SOURCE_INVALID` | The registry migration source returns something other than a list |
| `MIGRATION_CHECKSUM_MISMATCH` | An applied migration's stored checksum no longer matches its SQL |
| `MIGRATION_BASELINE_GAP` | A history below a baseline lacks the bridging migrations |
| `MIGRATION_CONCURRENT` | Sirannon cannot resolve a concurrent migration attempt |
| `MIGRATION_ROLLBACK_ERROR` | A rollback step fails |

## Server and requests

| Code | When |
| --- | --- |
| `INVALID_REQUEST` | The request body structure is invalid |
| `INVALID_JSON` | The body or WebSocket message is not valid JSON |
| `EMPTY_BODY` | The request body is empty |
| `PAYLOAD_TOO_LARGE` | The body or message exceeds `maxBodyBytes` |
| `INTERNAL_ERROR` | An unexpected error stops the server while it handles the request |
| `HOOK_ERROR` | The `authenticate` hook or `authorizeClusterStatus` throws, or `authenticate` returns a refusal object |
| `NOT_FOUND` | The route does not exist, or cluster status is absent or refused |
| `INVALID_MAX_BODY_BYTES` | `maxBodyBytes` is not a positive integer that the transport can enforce exactly |
| `INVALID_WS_BACKPRESSURE` | `maxWebSocketBackpressureBytes` fails validation or is below `maxBodyBytes` |
| `INVALID_BACKUP_RESTORE` | `acceptBackupRestore` is on and the server has no `authenticate` hook |
| `INVALID_DEVICE_SYNC` | `acceptDeviceSync` is on and the server has no `authenticate` hook |
| `BULK_LOAD_UNSUPPORTED` | The execution target provides no bulk load |
| `INVALID_MESSAGE` | A WebSocket message lacks a required field or has a field of the wrong type |
| `UNKNOWN_TYPE` | A WebSocket message has an unrecognised type |
| `HANDLER_CLOSED` | The WebSocket handler is shutting down |
| `DUPLICATE_SUBSCRIPTION` | A subscription with the same ID already exists on the connection |
| `SUBSCRIPTION_NOT_FOUND` | An unsubscribe names a subscription that does not exist |
| `CDC_UNSUPPORTED` | Subscriptions need a file-based database, and this one is in memory |

## Registered operations

| Code | When |
| --- | --- |
| `UNKNOWN_QUERY` | The server has no operation of that name for the database |
| `MISSING_ARGUMENT` | A declared argument is missing from the request |
| `ARGUMENT_NOT_ALLOWED` | The caller supplies an undeclared argument, or one that the server fills from identity |
| `IDENTITY_REQUIRED` | An operation fills an argument from identity and the request has none |
| `REGISTRY_MISMATCH` | A live query echoes a registry digest that this server does not serve |
| `SQL_NOT_ACCEPTED` | The server accepts no SQL over the network |
| `UNSUPPORTED_SUBPROTOCOL` | A WebSocket upgrade offers no subprotocol that the server supports |

## Replication

| Error | Code | When |
| --- | --- | --- |
| `ReplicationError` | `REPLICATION_ERROR` | Base class for replication failures |
| `SyncError` | `SYNC_ERROR` | First sync fails, because the node is not ready, the transfer times out, or a manifest or batch order does not match |
| `ConflictError` | `CONFLICT_ERROR` | A conflict resolver throws on a replicated change, and the error names the table and row |
| `TransportError` | `TRANSPORT_ERROR` | A peer is unreachable, or a send fails |
| `BatchValidationError` | `BATCH_VALIDATION_ERROR` | A batch fails its checksum, breaks the schema allowlist, exceeds `maxClockDriftMs`, or contains unsafe DDL |
| `WriteConcernError` | `WRITE_CONCERN_ERROR` | The replicas do not meet the write concern within the timeout |
| `ReadConcernError` | `READ_CONCERN_ERROR` | The node cannot satisfy the requested read concern |
| `TopologyError` | `TOPOLOGY_ERROR` | A replica receives a write with forwarding off, no primary is available, or a peer is unauthorised |
| `CoordinatorError` | `COORDINATOR_UNAVAILABLE` | The node cannot reach the coordinator, or the coordinator cannot prove quorum authority |
| `AuthorityError` | `AUTHORITY_LOST` | A node loses primary or controller authority while it handles work |
| `StalePrimaryError` | `STALE_PRIMARY` | A request, batch, sync message, or forwarded write has a stale primary term |
| `NoSafePrimaryError` | `NO_SAFE_PRIMARY` | No eligible in-sync replica is safe to promote |
| `NodeNotInSyncError` | `NODE_NOT_IN_SYNC` | The node is alive but outside the group's in-sync set |
| `NodeDrainingError` | `NODE_DRAINING` | The node is in maintenance drain mode |
| `ProtocolVersionMismatchError` | `PROTOCOL_VERSION_MISMATCH` | Node compatibility metadata is incompatible with the cluster |
| `UnsafeRecoveryRequiredError` | `UNSAFE_RECOVERY_REQUIRED` | Automatic recovery needs explicit operator action |

`@delali/sirannon-db/replication` exports every class above. `FailoverError` is the shared base of `NoSafePrimaryError` and `UnsafeRecoveryRequiredError`.

## Device sync

| Code | When |
| --- | --- |
| `MIGRATION_REQUIRED` | The device schema version is behind the server, so the device migrates before it syncs |
| `SCHEMA_AHEAD` | The device schema version is ahead of the server, so the server migrates first |
| `SYNC_UNSUPPORTED` | The execution target applies no changes, or the server predates device sync |
| `SNAPSHOT_UNSUPPORTED` | A device requests a snapshot of an in-memory database |
| `SNAPSHOT_CHECKSUM_MISMATCH` | A downloaded snapshot page fails checksum verification |
| `DEVICE_SYNC_NOT_ACCEPTED` | The server has `acceptDeviceSync` off, so it serves no device sync |
| `DEVICE_CLOCK_AHEAD` | A pushed change is stamped more than five minutes ahead of the server clock |
| `DEVICE_NOT_SUBSCRIBED` | An acknowledgement names a device for which the connection holds no subscription |

## Client

| Code | When |
| --- | --- |
| `CONNECTION_ERROR` | The client fails to connect to the server |
| `UNAUTHORIZED` | The server refuses the WebSocket upgrade as unauthenticated and closes with 4401 |
| `FORBIDDEN` | The server refuses the WebSocket upgrade as not permitted and closes with 4403 |
| `TIMEOUT` | A request exceeds the configured timeout |
| `TRANSPORT_ERROR` | The current transport does not support this operation, such as a live query over HTTP |
| `INVALID_RESPONSE` | The server returns a response that the client cannot parse |
| `ROUTING_ERROR` | The routing metadata names no usable primary or read endpoint |
| `NO_SAFE_PRIMARY` | The topology client has no current primary for a write |
| `INVALID_ARGUMENT` | A client argument fails validation, such as a per-call read concern on the topology transport |
| `UNKNOWN_ERROR` | An error response has no recognisable code |

## Optional packages

Sirannon raises these codes in your own process when it starts a server, a transport, or a coordinator. The message names the missing package and the command that installs it.

| Code | When |
| --- | --- |
| `SERVER_DEPENDENCY_MISSING` | `server.listen()` cannot load uWebSockets.js |
| `TRANSPORT_DEPENDENCY_MISSING` | The gRPC transport cannot load `@grpc/grpc-js`, `@bufbuild/protobuf`, or `grpc-health-check` when the engine starts |
| `COORDINATOR_DEPENDENCY_MISSING` | The etcd coordinator cannot load `etcd3` when the engine first calls it |

## Retrying

`WRITE_OVERLOADED` means that the writer sheds the write before it executes, so the same request is safe to send again once the `Retry-After` interval passes. `WRITER_WORKER_TIMEOUT` leaves the outcome indeterminate, so reconcile the state before you retry anything that is not idempotent.

`STALE_PRIMARY`, `AUTHORITY_LOST`, `COORDINATOR_UNAVAILABLE`, `NO_SAFE_PRIMARY`, and `CONNECTION_ERROR` mean that the routing metadata that the client holds is out of date. In coordinator mode, the topology client refreshes that metadata and retries a read once. For a write, it raises the error, so you decide whether to send the write again.

A validation code such as `INVALID_WRITER_WORKER`, `INVALID_MAX_BODY_BYTES`, or `INVALID_DRIVER` reports a configuration mistake, so fix the configuration before you try again.

`UNAUTHORIZED` and `FORBIDDEN` report a refused WebSocket upgrade. The client leaves that connection closed, so issue a fresh credential and build a new client.

## HTTP status codes

| Status | Codes |
| --- | --- |
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR`, `INVALID_DURABILITY`, `INVALID_SYNCHRONOUS`, `BATCH_VALIDATION_ERROR`, `MISSING_ARGUMENT`, `ARGUMENT_NOT_ALLOWED`, `UNSUPPORTED_SUBPROTOCOL`, `DEVICE_CLOCK_AHEAD` |
| 401 | `IDENTITY_REQUIRED` |
| 403 | `READ_ONLY`, `FORBIDDEN_SQL`, `HOOK_DENIED`, `SQL_NOT_ACCEPTED`, `BACKUP_RESTORE_NOT_ACCEPTED`, `DEVICE_SYNC_NOT_ACCEPTED` |
| 404 | `DATABASE_NOT_FOUND`, `NOT_FOUND`, `UNKNOWN_QUERY` |
| 409 | `STALE_PRIMARY`, `PROTOCOL_VERSION_MISMATCH`, `MIGRATION_REQUIRED`, `SCHEMA_AHEAD`, `REGISTRY_MISMATCH`, `BACKUP_CHAIN_BROKEN`, `BACKUP_RESTORE_IN_PROGRESS` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR`, `HOOK_ERROR`, `WRITER_WORKER_TIMEOUT` |
| 501 | `BULK_LOAD_UNSUPPORTED`, `SYNC_UNSUPPORTED`, `BACKUP_UNSUPPORTED` |
| 502 | `BACKUP_DESTINATION_ERROR` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN`, `READ_CONCERN_ERROR`, `COORDINATOR_UNAVAILABLE`, `AUTHORITY_LOST`, `NO_SAFE_PRIMARY`, `NODE_NOT_IN_SYNC`, `NODE_DRAINING`, `UNSAFE_RECOVERY_REQUIRED`, `WRITE_OVERLOADED` |

A code outside the table maps to 500, and a `RequestDeniedError` uses the status that you give it.

[`packages/spec/07-errors.md`](../packages/spec/07-errors.md) holds the normative code list that every implementation shares.
