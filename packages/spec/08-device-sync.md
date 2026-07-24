# Sirannon Device Sync Specification

Device sync keeps an end-user device's local database and a server database in
step, offline-first and bidirectional. A device pushes its own writes to the
server and pulls other writes live over a WebSocket, applying each side's changes
through the same conflict resolvers replication uses. It is distinct from
server-to-server replication ([03-replication.md](03-replication.md)): a device is
not a replication peer, holds no primary authority, and syncs a whole database.
The per-database file is the tenancy boundary; there is no partial sync.

Device sync reuses the change log ([02-core.md](02-core.md#change-data-capture-cdc)),
the HLC ([03-replication.md](03-replication.md#hybrid-logical-clock-hlc)), the
conflict resolvers ([03-replication.md](03-replication.md#conflict-resolution)),
the checksum canonicalisation ([03-replication.md](03-replication.md#checksum-normative)),
and the tagged value encoding ([02-core.md](02-core.md#tagged-value-encoding-normative)).

---

## Device Identity and Persisted State

A device has a stable identity: a 32-character lowercase hexadecimal string, held
in `_sirannon_meta` under `node_id` and generated once. The device keeps its sync
cursors in `_sirannon_meta`:

| Key | Meaning |
|-----|---------|
| `node_id` | The device identity. |
| `hlc_clock` | The persisted HLC. |
| `device_sync_pushed_seq` | Highest local `seq` acknowledged by the server on push. |
| `device_sync_pull_seq` | Highest server `seq` pulled. |
| `device_sync_pull_epoch` | The epoch the pull cursor belongs to. |
| `device_sync_snapshot_state` | Set to `loading` while a snapshot load runs, cleared on completion. |

---

## Write-Time Stamping

A CDC trigger writes a change with `node_id`, `tx_id`, and `hlc` empty; such a row
is an unstamped local change. Every local write path stamps its own rows in the
same transaction, setting `node_id` to the device identity, `tx_id` to a fresh
32-hex transaction id, and `hlc` to a fresh timestamp, over the rows still holding
an empty `node_id`. The `node_id` column is the change's origin. This stamping is
what lets a device read back only its own writes and lets the server suppress
echoes.

---

## Applying Changes

`applyChanges(batch, resolver?)` on the database applies a batch of remote changes
locally, defaulting to the LWW resolver. Application validates and applies the
batch as follows:

- The recomputed checksum must match `batch.checksum`, or the batch fails with
  `BATCH_VALIDATION_ERROR`. Each non-DDL `table` must be a valid identifier.
- A batch whose `toSeq` is at or below the highest already-applied sequence for
  its source is skipped whole. Individual changes already recorded in
  `_sirannon_applied_changes` are skipped, so application is idempotent.
- Changes are grouped by `txId`, each group applied in one transaction. A change
  whose target row is absent is inserted directly; a change whose row exists is
  resolved by the resolver (see
  [Conflict Resolution](03-replication.md#conflict-resolution)). Each applied
  change is recorded in `_sirannon_applied_changes`.
- After a group commits, the rows the applier's own triggers produced (those still
  holding an empty `node_id` above the pre-apply sequence) are stamped with the
  batch's `sourceNodeId` as origin and the highest applied `hlc`. Marking the
  origin as the source device is what makes the live pull suppress the echo back
  to that device.

`applyChanges` returns `ApplyResult { applied, skipped, conflicts }`.

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

Sequences are decimal strings (`^\d{1,19}$`) and `fromSeq` must not exceed
`toSeq`. `batchId` is a non-empty string of at most 128 characters. A batch holds
1 to 1000 changes. `sourceNodeId` and every change's `nodeId` are 32-hex device
ids and must be equal. Each `operation` is `insert`, `update`, or `delete`; a DDL
change is rejected, because schema flows only through the migration handshake.
Values inside `primaryKey`, `newData`, and `oldData` use the tagged value
encoding. Any structural failure responds with `400 INVALID_REQUEST`.

The server applies the decoded batch through the execution target's `applyChanges`.
A target without `applyChanges` responds `501 SYNC_UNSUPPORTED`. The schema gate
runs first (see [Migration Handshake](#migration-handshake)).

---

## Device Cursors and Retention

The server tracks how far each device has acknowledged in
`_sirannon_device_cursors`:

```sql
CREATE TABLE _sirannon_device_cursors (
  device_id  TEXT PRIMARY KEY,
  acked_seq  INTEGER NOT NULL DEFAULT 0,
  updated_at REAL NOT NULL
)
```

An acknowledgement upserts the cursor and moves `acked_seq` forward only
(`max(current, incoming)`). Change-log retention is bounded so a device can still
resume: the prune boundary is the minimum, across live devices, of the sequence
immediately before each device's next unacknowledged foreign change (a change
whose `node_id` differs from the device), or the current maximum sequence when the
device has no foreign change ahead of its cursor. A device that only writes, and
has acknowledged no foreign change, therefore does not pin retention below its own
writes. Cursors idle past the retention window (default 2,592,000,000 ms, 30 days)
are evicted, after which that device must resync from a snapshot.

---

## Live Pull

A device pulls other writes over the WebSocket subscription
([05-server.md](05-server.md#websocket-protocol-normative)), presenting its
identity:

```text
{ type: 'subscribe', id, table, sinceSeq?, epoch?, deviceId, schemaVersion? }
```

`deviceId` is a 32-hex device id. The server does not deliver a change whose
`origin` equals the subscribing `deviceId`, so a device never receives its own
writes back. Resumption and the `resync` signal follow the WebSocket rules; a
`sinceSeq` presented with an epoch other than the current one forces a resync.

The device acknowledges what it has processed so the server can advance the
device cursor and prune:

```text
{ type: 'ack', id, deviceId, seq }  ->  result { acked: true, seq }
```

`seq` is a decimal string. The acknowledgement upserts the device cursor
monotonically. A device sends an acknowledgement on a debounce (recommended
2,000 ms) for the highest sequence it has received, never for the baseline cursor
the subscription started from.

---

## Snapshot Download

A fresh device, or one too far behind to resume, replaces its whole database from
a server snapshot.

```text
POST /db/{id}/snapshot
-> { databaseId, startSeq, epoch, schema: List<ddl>, tables: [ { name, rowCount } ],
     migrations: [ { version, name, checksum } ] }

POST /db/{id}/snapshot/page
{ table, afterKey?, limit? }
-> { rows, checksum, done, nextKey }
```

The manifest reports `startSeq` (the current maximum change sequence, captured
before any copy) and `epoch` as decimal strings, the schema DDL in
foreign-key-dependency order, per-table row counts, and the applied migration
rows. An in-memory database is rejected with `400 SNAPSHOT_UNSUPPORTED`.

A page returns rows for one table using keyset pagination: `afterKey` is the last
key of the previous page (1 to 16 values) and `nextKey` is the last key of this
page (null when `done`). `limit` is 1 to 1000, defaulting to 500, and a page is
further trimmed to fit an 8 MiB byte cap. `checksum` is the lowercase SHA-256 hex
digest of the canonical form of `rows` (the same canonicalisation as the batch
checksum); the device verifies it and fails with `SNAPSHOT_CHECKSUM_MISMATCH` on a
mismatch.

The device applies the snapshot as a wipe-and-replace: it sets
`device_sync_snapshot_state` to `loading`, turns foreign keys off, unwatches and
drops the target tables in reverse dependency order, recreates the schema, inserts
each table's pages, replaces its migration history and mirrors `user_version`
(see [Migration Handshake](#migration-handshake)), sets the pull cursor to
`startSeq` and `epoch`, then rewatches the tables, turns foreign keys on, and
clears the state. A load interrupted before completion leaves the state set, so a
restart knows the copy is incomplete and resumes the resync. While a load runs,
every read and write on the database fails with `SNAPSHOT_IN_PROGRESS`; the gate is
seeded at open from the durable state marker.

Because `startSeq` is captured before the copy and the pull cursor is set to it,
the live pull replays every change after `startSeq`, reconciling writes made during
the copy. This requires the change log to retain history back to `startSeq` for
the copy's duration.

---

## Migration Handshake

Schema changes never travel through `/changes`; a device applies migrations and
the server serves them.

The schema version is the highest applied migration version (see
[02-core.md](02-core.md#migrations)). A device carries `schemaVersion` on the push
body and on the subscribe message; a missing value is treated as 0. The server
gates on it:

- A device version below the server version is refused with `MIGRATION_REQUIRED`.
- A device version above the server version is refused with `SCHEMA_AHEAD`.

On push the refusal is `409` with `details { serverVersion }`; on subscribe the
refusal carries the same code.

A device fetches the migrations it lacks:

```text
POST /db/{id}/migrations
{ after? }
-> { serverVersion, migrations: [ { version, name, checksum, up? } ] }
```

The server returns every applied migration row and attaches `up` SQL only for a
row whose `version` is above `after`, whose stored `checksum` is present, and whose
registry migration's SQL still hashes to that checksum. The checksum is the 64-bit
FNV-1a hash of the up SQL with line endings normalised to `\n` and surrounding
whitespace trimmed, as 16 lowercase hexadecimal digits. A migration whose `up`
cannot be served this way (a function migration, or a checksum that no longer
matches) is returned without SQL, and the device must resync from a snapshot to
gain it. The device re-verifies each served `up` against its `checksum` before
applying it through the migration runner, and applies the missing migrations in
order.

A device whose applied history diverges from the server's (an overlapping version
whose checksum differs, or a gap in the shared history) or that hits a baseline
gap must resync from a snapshot, which is destructive and is surfaced to the
application through a resync-required signal before the wipe.

---

## Capability Negotiation

```text
GET /capabilities  ->  { capabilities: List<string> }
```

A device-sync server announces exactly these capabilities, and all are required for
device sync:

`sync.push`, `sync.echo-suppression`, `sync.ack`, `sync.resume`, `sync.snapshot`,
`sync.migrations`, `sync.schema-gate`.

Before syncing, a client fetches `/capabilities`. A `404` (the server predates
device sync) or a missing required capability fails with `SYNC_UNSUPPORTED`, naming
the gap, so the client does not sync against a server whose WebSocket ignores the
device-sync fields. A connection, timeout, or malformed-response failure is
indeterminate; the client records it and continues in a degraded, offline-tolerant
state rather than treating the server as unsupported.

---

## Client Sync Controller

The controller drives a device's sync loop.

```text
SyncControllerOptions {
  url, databaseId, tables,
  headers?, batchSize? (100), pushIntervalMs? (1000), ackIntervalMs? (2000),
  maxPushRetryDelayMs? (30000), requestTimeout? (30000),
  autoResync? (true), snapshotRetryDelayMs? (5000), maxSnapshotRetryDelayMs? (300000),
  snapshotPageSize?,
  onChange?, onResyncRequired?, onSnapshotProgress?
}

SyncState = 'stopped' | 'starting' | 'running' | 'paused' | 'snapshotting'

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

- **start** verifies server capabilities first and caches them; a `SYNC_UNSUPPORTED`
  result aborts the start, while an indeterminate failure is recorded and the
  controller continues degraded. It then reconciles the migration handshake
  (falling back to the local version when offline), opens the live pull, and
  starts the push loop.
- **push** drains the outbox after the durable `device_sync_pushed_seq` cursor in
  batches (default 100), advancing the cursor and the retention boundary per batch;
  a failure backs off exponentially to a cap (default 30,000 ms). A push refused
  with `MIGRATION_REQUIRED` reconciles migrations and retries.
- **pull** runs its own WebSocket subscription with echo suppression, persists the
  `device_sync_pull_seq` and `device_sync_pull_epoch` cursors, and acknowledges on
  the debounce. A server resync signal marks a resync required and calls
  `onResyncRequired`.
- **auto-resync**, when enabled, schedules a snapshot download on a start with a
  pending load, on a server resync signal, and on a snapshot failure, backing off
  exponentially (first attempt immediate, then `snapshotRetryDelayMs` doubling to
  `maxSnapshotRetryDelayMs`).
- **pause** tears down the loops and persists cursors; **resume** restarts;
  **stop** tears down and persists.

Applying pulled changes into the local database is the application's
responsibility: the controller delivers each pulled change through `onChange`.
