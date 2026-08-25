# Sirannon Device Sync Specification

Device sync keeps an end-user device's local database and a server database in step, offline-first and bidirectional. A device pushes its own writes to the server and pulls other writes live over a WebSocket, applying each side's changes through the same conflict resolvers replication uses. It is distinct from server-to-server replication ([03-replication.md](03-replication.md)): a device is not a replication peer, holds no primary authority, and syncs a whole database. The per-database file is the tenancy boundary; there is no partial sync.

Device sync reuses the change log ([02-core.md](02-core.md#change-data-capture-cdc)), the HLC ([03-replication.md](03-replication.md#hybrid-logical-clock-hlc)), the conflict resolvers ([03-replication.md](03-replication.md#conflict-resolution)), the checksum canonicalisation ([03-replication.md](03-replication.md#checksum-normative)), and the tagged value encoding ([02-core.md](02-core.md#tagged-value-encoding-normative)).

---

## Encryption on a Device

A device encrypts its own local database under a master key its application supplies from the platform key store, through the `encryption` option [02-core.md](02-core.md) defines. Each device holds a key of its own, and a server holds the keys of the databases it opens. Changes cross the wire as values under the transport's own transport-layer security, so a device and a server that hold different keys sync with each other.

---

## Device Identity and Persisted State

A device has a stable identity: a 32-character lowercase hexadecimal string, held in `_sirannon_meta` under `node_id` and generated once. The device keeps its sync cursors in `_sirannon_meta`:

| Key | Meaning |
|-----|---------|
| `node_id` | The device identity. |
| `hlc_clock` | The persisted HLC. |
| `device_sync_pushed_seq` | Highest local `seq` acknowledged by the server on push. |
| `device_sync_pull_seq` | Highest server `seq` applied locally. |
| `device_sync_pull_epoch` | The epoch the pull cursor belongs to. |
| `device_sync_resync_required` | Set when the server signals a resync, cleared when a snapshot completes. |
| `device_sync_snapshot_state` | Set to `loading` while a snapshot load runs, cleared on completion. |

---

## Write-Time Stamping

A CDC trigger writes a change with `node_id`, `tx_id`, and `hlc` empty; such a row is an unstamped local change. Every local write path stamps its own rows in the same transaction, setting `node_id` to the device identity, `tx_id` to a fresh 32-hex transaction id, and `hlc` to a fresh timestamp, over the rows still holding an empty `node_id`. The `node_id` column is the change's origin. This stamping is what lets a device read back only its own writes and lets the server suppress echoes.

---

## Applying Changes

`applyChanges(batch, resolver?)` on the database applies a batch of remote changes locally, defaulting to the LWW resolver. Application validates and applies the batch as follows:

- The recomputed checksum must match `batch.checksum`, or the batch fails with `BATCH_VALIDATION_ERROR`. Each non-DDL `table` must be a valid identifier.
- A batch whose `toSeq` is at or below the highest already-applied sequence for its source is skipped whole. Individual changes already recorded in `_sirannon_applied_changes` are skipped, so application is idempotent.
- Changes are grouped by `txId`, each group applied in one transaction. A change whose target row is absent is inserted directly; a change whose row exists is resolved by the resolver (see [Conflict Resolution](03-replication.md#conflict-resolution)). Each applied change is recorded in `_sirannon_applied_changes`.
- After a group commits, the rows the applier's own triggers produced (those still holding an empty `node_id` above the pre-apply sequence) are stamped with the batch's `sourceNodeId` as origin and the highest applied `hlc`. Marking the origin as the source device is what makes the live pull suppress the echo back to that device.

`applyChanges` returns `ApplyResult { applied, skipped, conflicts }`.

A device applies pulled changes from the live stream under the same rules, one `txId` group per transaction, with no checksum and no batch envelope, reading the group from the staging table of [Staged Pull](#staged-pull). It writes `device_sync_pull_seq` inside the group's transaction and records nothing in `_sirannon_applied_changes`.

---

## Push

A device sends its own writes to the server.

```text
POST /db/{id}/changes
{ schemaVersion?, batch: {
    sourceNodeId, batchId, fromSeq, toSeq, hlcRange: { min, max },
    changes: [ { table, operation, rowId, primaryKey, hlc, txId, nodeId, newData, oldData } ],
    checksum } }
-> { applied, skipped, conflicts }
```

Sequences are decimal strings (`^\d{1,19}$`) and `fromSeq` must not exceed `toSeq`. `batchId` is a non-empty string of at most 128 characters. A batch holds 1 to 1000 changes. `sourceNodeId` and every change's `nodeId` are 32-hex device ids and must be equal. Each `operation` is `insert`, `update`, or `delete`; a DDL change is rejected, because the migration handshake is the only path for a schema change. Values inside `primaryKey`, `newData`, and `oldData` use the tagged value encoding. Any structural failure responds with `400 INVALID_REQUEST`.

The server applies the decoded batch through the execution target's `applyChanges`. A target without `applyChanges` responds `501 SYNC_UNSUPPORTED`. The schema gate runs first (see [Migration Handshake](#migration-handshake)).

---

## Device Cursors and Retention

The server tracks how far each device has acknowledged in `_sirannon_device_cursors`:

```sql
CREATE TABLE _sirannon_device_cursors (
  device_id  TEXT PRIMARY KEY,
  acked_seq  INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL
)
```

An acknowledgement upserts the cursor and moves `acked_seq` forward only (`max(current, incoming)`). Change-log retention is bounded so a device can still resume: the prune boundary is the minimum, across live devices, of the sequence immediately before each device's next unacknowledged foreign change (a change whose `node_id` differs from the device), or the current maximum sequence when the device has no foreign change ahead of its cursor. A device that only writes, and has acknowledged no foreign change, therefore does not pin retention below its own writes. Cursors idle past the retention window (default 2,592,000,000 ms, 30 days) are evicted, after which that device must resync from a snapshot.

---

## Live Pull

A device pulls other writes over the WebSocket subscription ([05-server.md](05-server.md#websocket-protocol-normative)), presenting its identity:

```text
{ type: 'subscribe', id, tables, sinceSeq?, epoch?, deviceId, schemaVersion?, stagedStream? }
```

A device subscribes once for its whole table set, so its changes arrive in one ascending stream and a transaction spanning tables stays contiguous. It resumes from the higher of `device_sync_pull_seq` and the highest sequence in `_sirannon_staged_changes`, on both a first subscribe and a reconnect.

`deviceId` is a 32-hex device id. The server does not deliver a change whose `origin` equals the subscribing `deviceId`, so a device never receives its own writes back. The server must also withhold an unstamped change, which carries no origin and no timestamp; a device obtains those rows by applying the migration that wrote them. Resumption and the `resync` signal follow the WebSocket rules, and a `sinceSeq` sent with an epoch other than the current one forces a resync.

Each change carries `rowId`, `txId`, and `txEnd`. A device stages each change on arrival (see [Staged Pull](#staged-pull)). Once it has staged the change marked `txEnd`, it applies the group with its resolver and advances `device_sync_pull_seq` in the same transaction.

The device acknowledges what it holds durably so that the server advances the device cursor and prunes:

```text
{ type: 'ack', id, deviceId, seq }  ->  result { acked: true, seq }
```

`seq` is a decimal string. The acknowledgement upserts the device cursor monotonically. A device acknowledges only a sequence it has committed, whether staged or applied, never the baseline cursor the subscription started from. It acknowledges on a debounce (recommended 2,000 ms), and immediately after a commit while more than half the delivery window is outstanding.

The server holds delivery to a device once the highest sequence sent runs more than `maxUnacknowledgedChanges` (default 1,000) ahead of that device's acknowledged cursor, and resumes on the next acknowledgement. The window is measured per transaction, so a transaction larger than the window is still delivered whole. On a subscription carrying `stagedStream`, the server measures the window per change and may pause delivery within a transaction. Held changes remain in the change log, and the server delivers them in order. The server reports the window on `subscribed` as `maxUnacknowledgedChanges`, and a device acknowledges immediately once it holds more than half of it.

---

## Staged Pull

A device stages every pulled change before applying it. The staging table is `_sirannon_staged_changes`:

```sql
CREATE TABLE _sirannon_staged_changes (
  seq        INTEGER PRIMARY KEY,
  table_name TEXT NOT NULL,
  operation  TEXT NOT NULL,
  row_id     TEXT NOT NULL,
  changed_at REAL NOT NULL,
  old_data   TEXT,
  new_data   TEXT,
  node_id    TEXT NOT NULL DEFAULT '',
  tx_id      TEXT NOT NULL DEFAULT '',
  hlc        TEXT NOT NULL DEFAULT '',
  tx_end     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx__sirannon_staged_changes_tx_end ON _sirannon_staged_changes (tx_end, seq);
```

`seq`, `table_name`, `operation`, `row_id`, and `changed_at` carry the change's sequence, table, operation, row id, and timestamp. `old_data` and `new_data` hold the row images as JSON under the [tagged value encoding](02-core.md#tagged-value-encoding-normative); `new_data` is null for a delete, and `old_data` is null when the change carries no previous image. `node_id`, `tx_id`, and `hlc` carry the change's origin, transaction, and HLC timestamp, each empty when the change carries none. `tx_end` is 1 on the change marked `txEnd` and 0 otherwise.

A device stages each received batch in one transaction, which commits before any acknowledgement covering those sequences. It applies each complete staged transaction in one local transaction that also writes `device_sync_pull_seq`, then deletes the staged rows at or below that sequence.

On open, a device deletes staged rows at or below `device_sync_pull_seq`, applies every complete staged transaction, and keeps an incomplete tail for the resumed subscription. A device that fails to apply during this recovery still attempts the subscription, so that a device below the server schema version reaches the migration handshake.

A device that requires per-change pacing declares `stagedStream: true` on subscribe, and sends the field only to a server announcing `sync.staged-stream`. The server then packs several changes into each `changes` message (see [05-server.md](05-server.md#websocket-protocol-normative)). The number of changes per message is implementation-defined.

---

## Snapshot Download

A device replaces its whole database from a server snapshot. A device that has fallen too far behind to resume resyncs automatically. A device syncing for the first time requests its copy with `downloadSnapshot()`.

```text
POST /db/{id}/snapshot
-> { databaseId, startSeq, epoch, schema: List<ddl>, tables: [ { name, rowCount } ],
     migrations: [ { version, name, checksum } ] }

POST /db/{id}/snapshot/page
{ table, afterKey?, limit? }
-> { rows, checksum, done, nextKey }
```

The manifest reports `startSeq` (the current maximum change sequence, captured before any copy) and `epoch` as decimal strings, the schema DDL in foreign-key-dependency order, per-table row counts, and the applied migration rows. An in-memory database is rejected with `400 SNAPSHOT_UNSUPPORTED`.

A page returns rows for one table using keyset pagination: `afterKey` is the last key of the previous page (1 to 16 values) and `nextKey` is the last key of this page (null when `done`). `limit` is 1 to 1000, defaulting to 500, and a page is further trimmed to fit an 8 MiB byte cap. `checksum` is the lowercase SHA-256 hex digest of the canonical form of `rows` (the same canonicalisation as the batch checksum); the device verifies it and fails with `SNAPSHOT_CHECKSUM_MISMATCH` on a mismatch.

The device applies the snapshot as a wipe-and-replace: it sets `device_sync_snapshot_state` to `loading`, turns foreign keys off, clears `_sirannon_staged_changes`, unwatches and drops the target tables in reverse dependency order, recreates the schema, inserts each table's pages, replaces its migration history and mirrors `user_version` (see [Migration Handshake](#migration-handshake)), sets the pull cursor to `startSeq` and `epoch`, then rewatches the tables, turns foreign keys on, and clears the state. A load interrupted before completion leaves the state set, so that a restart detects the incomplete copy and resumes the resync. While a load runs, every read and write on the database fails with `SNAPSHOT_IN_PROGRESS`; the gate is seeded at open from the durable state marker.

Because `startSeq` is captured before the copy and the pull cursor is set to it, the live pull replays every change after `startSeq`, reconciling writes made during the copy. This requires the change log to retain history back to `startSeq` for the copy's duration.

---

## Migration Handshake

Schema changes never travel through `/changes`; a device applies migrations and the server serves them.

The schema version is the highest applied migration version (see [02-core.md](02-core.md#migrations)). A device carries `schemaVersion` on the push body and on the subscribe message; a missing value is treated as 0. The server gates on it:

- A device version below the server version is refused with `MIGRATION_REQUIRED`.
- A device version above the server version is refused with `SCHEMA_AHEAD`.

On push the refusal is `409` with `details { serverVersion }`; on subscribe the refusal carries the same code.

A device fetches the migrations it lacks:

```text
POST /db/{id}/migrations
{ after? }
-> { serverVersion, migrations: [ { version, name, checksum, up? } ] }
```

The server returns every applied migration row and attaches `up` SQL only for a row whose `version` is above `after`, whose stored `checksum` is present, and whose registry migration's SQL still hashes to that checksum. The checksum is the 64-bit FNV-1a hash of the up SQL with line endings normalised to `\n` and surrounding whitespace trimmed, as 16 lowercase hexadecimal digits. A migration whose `up` cannot be served this way (a function migration, or a checksum that no longer matches) is returned without SQL, and the device must resync from a snapshot to gain it. The device re-verifies each served `up` against its `checksum` before applying it through the migration runner, and applies the missing migrations in order.

A resync signal must not move the pull cursor. The device records `device_sync_resync_required` and leaves its cursor unchanged, so that a restart before the snapshot runs still detects the stale copy.

A device whose applied history diverges from the server's (an overlapping version whose checksum differs, or a gap in the shared history) or that hits a baseline gap must resync from a snapshot, which is destructive and is surfaced to the application through a resync-required signal before the wipe.

---

## Capability Negotiation

```text
GET /capabilities  ->  { capabilities: List<string>, registry?: { digest: string } }
```

A device-sync server announces at least these capabilities, and device sync requires every one of them:

`sync.push`, `sync.echo-suppression`, `sync.ack`, `sync.resume`, `sync.snapshot`, `sync.migrations`, `sync.schema-gate`, `sync.stream-apply`.

`sync.stream-apply` covers the `rowId`, `txId`, and `txEnd` fields and the acknowledgement-paced delivery window. `sync.staged-stream` covers the `stagedStream` subscribe field, the `changes` message, and per-change window pacing; device sync does not require it, and a device omits `stagedStream` when a server does not announce it. A server announces further capabilities alongside these; see [05-server.md](05-server.md#registered-operations).

Before syncing, a client fetches `/capabilities`. A `404` (the server predates device sync) or a missing required capability fails with `SYNC_UNSUPPORTED`, naming the gap, so the client does not sync against a server whose WebSocket ignores the device-sync fields. A connection, timeout, or malformed-response failure is indeterminate; the client records it and continues in a degraded, offline-tolerant state rather than treating the server as unsupported.

---

## Client Sync Controller

The controller drives a device's sync loop.

```text
SyncControllerOptions {
  url, databaseId, tables,
  headers?, webSocketProtocols?, batchSize? (100), pushIntervalMs? (1000), ackIntervalMs? (2000),
  immediateAckAfterChanges?, maxPushRetryDelayMs? (30000), requestTimeout? (30000),
  autoResync? (true), snapshotRetryDelayMs? (5000), maxSnapshotRetryDelayMs? (300000),
  snapshotPageSize?, resolver?,
  onChange?, onStatusChange?, onResyncRequired?, onSnapshotProgress?, onSnapshotComplete?
}

SyncState = 'stopped' | 'starting' | 'running' | 'paused' | 'snapshotting'

SnapshotOutcome {
  ok:             boolean
  error:          { code, message } or null
  databaseUsable: boolean
  retrying:       boolean
}

SyncStatus {
  state:              SyncState
  deviceId:           string or null
  serverCapabilities: List<string> or null
  schemaVersion:      number or null
  pendingPushCount:   number
  lastPushedSeq:      number
  lastPulledSeq:      number or null
  pushCaughtUp:       boolean
  resyncRequired:     boolean
  lastError:          { code, message } or null
}
```

`headers` applies to the controller's HTTP requests and to the pull subscription's WebSocket upgrade in a runtime whose WebSocket carries a handshake header. A controller constructed with `headers`, no `webSocketProtocols`, and a runtime that carries none must fail at construction with `INVALID_ARGUMENT` and name `webSocketProtocols`.

`webSocketProtocols` applies to the pull subscription's WebSocket upgrade. A device carries a short-lived credential in `webSocketProtocols`. A controller that configures subprotocols must offer the `sirannon.v1` identifier ahead of them (see [05-server.md](05-server.md#subprotocol-negotiation)). A controller that configures none must offer no subprotocol. Each configured subprotocol is one or more of the characters a header token allows, and no two are equal; a controller given anything else must fail at construction with `INVALID_ARGUMENT`, name `webSocketProtocols`, and leave the value out of the message.

- **start** verifies server capabilities first and caches them; a `SYNC_UNSUPPORTED` result aborts the start, while an indeterminate failure is recorded and the controller continues degraded. It then reconciles the migration handshake (falling back to the local version when offline), opens the live pull, and starts the push loop.
- **push** drains the outbox after the durable `device_sync_pushed_seq` cursor in batches (default 100), advancing the cursor and the retention boundary per batch; a failure backs off exponentially to a cap (default 30,000 ms). A push refused with `MIGRATION_REQUIRED` reconciles migrations and retries.
- **pull** runs its own WebSocket subscription with echo suppression, stages each change, applies each complete transaction with `resolver` (defaulting to LWW), commits `device_sync_pull_seq` with the group, persists `device_sync_pull_epoch`, and acknowledges the highest staged sequence. `immediateAckAfterChanges` overrides the count of outstanding changes that forces an acknowledgement ahead of the debounce, defaulting to half the window the server reported and to 500 when it reported none. A server resync signal marks a resync required and calls `onResyncRequired`.
- **auto-resync**, when enabled, schedules a snapshot download on a start with a pending load, on a server resync signal, and on a snapshot failure, backing off exponentially (first attempt immediate, then `snapshotRetryDelayMs` doubling to `maxSnapshotRetryDelayMs`).
- **pause** tears down the loops and persists cursors; **resume** restarts; **stop** tears down and persists.

`onChange` reports each pulled change after the controller commits it, including a change staged before a restart.

The controller reports the current `SyncStatus` through `onStatusChange` on a state change, on a push that advances the cursor, on an applied pull batch, on a resync becoming required, and on an error recorded or cleared. Every one of those reaches the listener, in the order it occurred. `pendingPushCount` can lag the rest of a reported status, because the controller counts the outbox on its own schedule and raises a further status when that count changes.

The controller calls `onResyncRequired` when a resync becomes required: the server signals one, the migration handshake returns `resync-required`, or a start finds one the device still owes. It calls `onSnapshotComplete` once a snapshot load ends and the state has settled, for an automatic resync and for the application's own `downloadSnapshot()`. A failure after the wipe begins leaves the local database refusing reads and writes, so the outcome reports `databaseUsable` from the device's own load marker and `retrying` from whether another attempt is scheduled.
