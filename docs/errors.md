# Errors

Every error extends `SirannonError` with a machine-readable `code`.

```ts
import { QueryError } from '@delali/sirannon-db'

try {
  await db.execute('INSERT INTO users (id) VALUES (?)', [1])
} catch (err) {
  if (err instanceof QueryError) console.error(`SQL failed [${err.code}]: ${err.message}`, err.sql)
}
```

## Core engine

| Error | Code | When |
| --- | --- | --- |
| `DatabaseNotFoundError` | `DATABASE_NOT_FOUND` | Database ID not in registry |
| `DatabaseAlreadyExistsError` | `DATABASE_ALREADY_EXISTS` | Duplicate database ID |
| `ReadOnlyError` | `READ_ONLY` | Write attempted on a read-only database |
| `QueryError` | `QUERY_ERROR` | SQL execution failure |
| `TransactionError` | `TRANSACTION_ERROR` | Transaction commit or rollback failure |
| `MigrationError` | `MIGRATION_ERROR` | Migration step failure |
| `HookDeniedError` | `HOOK_DENIED` | Before-hook rejected the operation |
| `CDCError` | `CDC_ERROR` | Change tracking pipeline failure |
| `BackupError` | `BACKUP_ERROR` | Backup operation failure |
| `ConnectionPoolError` | `CONNECTION_POOL_ERROR` | Pool closed or misconfigured |
| `MaxDatabasesError` | `MAX_DATABASES` | Capacity limit reached |
| `ExtensionError` | `EXTENSION_ERROR` | SQLite extension load failure |

## Writes and payloads

| Code | When | Retry? |
| --- | --- | --- |
| `WRITE_OVERLOADED` | More writes pending than `maxPendingWrites` allows, or a queued write was shed when an earlier deadline expired. HTTP returns 503 with `Retry-After`. | Yes; the write never applied |
| `WRITER_WORKER_TIMEOUT` | An in-flight operation missed `writeTimeoutMs` plus the grace window | Only after reconciling; the outcome is indeterminate |
| `WRITER_WORKER_UNSUPPORTED` | `writerWorker` enabled on a driver with no worker entry; the database refuses to open | No; change driver or option |
| `INVALID_WRITER_WORKER` | A `writerWorker` value is out of range | No; fix the configuration |
| `PAYLOAD_TOO_LARGE` | A request or message exceeded `maxBodyBytes` | No; send less |
| `INVALID_MAX_BODY_BYTES` | `maxBodyBytes` is not a positive integer within `4_294_967_295` | No; fix the configuration |
| `INVALID_WS_BACKPRESSURE` | `maxWebSocketBackpressureBytes` is out of range | No; fix the configuration |
| `INVALID_DURABILITY` | A load passed a `durability` other than `'off'` or `'normal'` | No; fix the call |
| `DURABILITY_RESTORE_FAILED` | The load committed, then the writer failed before durability was restored | No; the load succeeded, do not re-run it |
| `BULK_LOAD_UNSUPPORTED` | The resolved execution target implements no bulk load | No |

## Replication

| Error | Code | When |
| --- | --- | --- |
| `ReplicationError` | `REPLICATION_ERROR` | Base class for replication failures |
| `SyncError` | `SYNC_ERROR` | First sync failures: node not ready, timeout, integrity mismatch |
| `ConflictError` | `CONFLICT_ERROR` | Unresolvable write conflict |
| `TransportError` | `TRANSPORT_ERROR` | Inter-node communication failure |
| `BatchValidationError` | `BATCH_VALIDATION_ERROR` | Checksum mismatch, clock drift, or oversized batch |
| `TopologyError` | `TOPOLOGY_ERROR` | Write on a read-only node without forwarding |
| `WriteConcernError` | `WRITE_CONCERN_ERROR` | Quorum not reached within the timeout |

The normative code list every implementation shares is in [`packages/spec/07-errors.md`](../packages/spec/07-errors.md).
