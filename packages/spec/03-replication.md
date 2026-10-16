# Sirannon Replication Specification

Replication distributes writes between Sirannon nodes. A single primary per replication group accepts writes and sends them to replicas as batched change sets, each change carrying a Hybrid Logical Clock timestamp for causal ordering. This document covers server-to-server replication; end-user device sync is a separate feature defined in [08-device-sync.md](08-device-sync.md).

Sirannon supports two authority modes:

- **Static mode.** The primary and replicas are configured outside the engine. Static mode has no Sirannon-owned automatic failover.
- **Coordinator mode.** A linearisable cluster coordinator owns primary authority, node liveness, safe promotion, client routing metadata, and fail-closed write behaviour. [09-failover.md](09-failover.md) defines coordinator mode.

Conflict resolvers are batch-application primitives; a receiver uses one when an incoming change targets a row that already exists locally. They do not grant multi-writer authority and do not make failover safe.

---

## Hybrid Logical Clock (HLC)

Each node keeps one HLC that orders events causally without relying on synchronised wall clocks.

```text
HLC { wallMs: number, logical: number (0 to 65535), nodeId: string }
```

### Encoded Format (Normative)

```text
{wallMs_hex}-{logical_hex}-{nodeId}
```

`wallMs_hex` is 12 zero-padded hexadecimal digits of the wall-clock milliseconds; `logical_hex` is 4 zero-padded hexadecimal digits of the logical counter; `nodeId` is the node identifier, which may contain hyphens. All implementations must produce and parse this encoding. The encoded string orders by plain lexicographic comparison, which preserves causal order.

### Operations

```text
now():
  physical = currentTimeMillis()
  if physical > wallMs: wallMs = physical; logical = 0
  else:                 logical = logical + 1
  if logical > 65535: fail with REPLICATION_ERROR (HLC overflow)
  return encode(wallMs, logical, nodeId)

receive(remote):
  r = decode(remote)
  physical = currentTimeMillis()
  if physical > max(wallMs, r.wallMs): wallMs = physical; logical = 0
  else if r.wallMs > wallMs:           wallMs = r.wallMs; logical = r.logical + 1
  else if wallMs > r.wallMs:           logical = logical + 1
  else:                                logical = max(logical, r.logical) + 1
  if logical > 65535: fail with REPLICATION_ERROR (HLC overflow)
  return encode(wallMs, logical, nodeId)

compare(a, b): lexicographic string comparison, returns -1, 0, or 1
decode(s):     split on '-'; needs at least 3 parts; nodeId is every part from the third onward
```

The clock is persisted in `_sirannon_meta` under `hlc_clock`. On start, a node seeds its clock from the persisted value and from the highest `hlc` observed in the change log and per-column version table, so it never moves backwards. See [test-vectors/hlc.json](test-vectors/hlc.json).

---

## Replication Tables

Replication uses the shared change log (`_sirannon_changes`, see [02-core.md](02-core.md#change-log)) plus these tables:

```sql
CREATE TABLE _sirannon_peer_state (
  peer_node_id      TEXT PRIMARY KEY,
  last_acked_seq    INTEGER NOT NULL DEFAULT 0,
  last_received_hlc TEXT NOT NULL DEFAULT '',
  updated_at        REAL NOT NULL
);

CREATE TABLE _sirannon_applied_changes (   -- idempotency ledger
  source_node_id TEXT NOT NULL,
  source_seq     INTEGER NOT NULL,
  applied_at     REAL NOT NULL,
  PRIMARY KEY (source_node_id, source_seq)
);

CREATE TABLE _sirannon_column_versions (   -- per-column HLC for field-merge
  table_name  TEXT NOT NULL,
  row_id      TEXT NOT NULL,
  column_name TEXT NOT NULL,
  hlc         TEXT NOT NULL,
  node_id     TEXT NOT NULL,
  PRIMARY KEY (table_name, row_id, column_name)
);

CREATE TABLE _sirannon_sync_state (        -- first-sync progress
  table_name     TEXT PRIMARY KEY,
  status         TEXT NOT NULL DEFAULT 'pending',
  row_count      INTEGER NOT NULL DEFAULT 0,
  pk_hash        TEXT NOT NULL DEFAULT '',
  snapshot_seq   INTEGER,
  source_peer_id TEXT,
  started_at     REAL,
  completed_at   REAL,
  request_id     TEXT
);
```

A node reads its outgoing batch from `_sirannon_changes` where `seq > afterSeq` and `node_id = localNodeId`. In coordinator mode it reads every stamped change whose `node_id` is its own ID or the ID of a voting data-bearing node of its group, so that the history which earlier primaries wrote reaches every replica. It records applied remote changes in `_sirannon_applied_changes` for deduplication and merges each remote HLC into its clock through `receive`. In coordinator mode a node also records in `_sirannon_changes` each replicated schema change that it applies, stamped with the sending node's ID and the change's `txId` and HLC, so that its history holds every schema change that it can later send as primary.

---

## Primary-Replica Topology

```text
canWrite():                 role == 'primary'
shouldReplicateTo(peer):    role == 'primary' and peer.role == 'replica'
shouldAcceptFrom(peer):     role == 'replica' and peer.role == 'primary'
```

- Only a primary writes. A replica rejects a direct write with `TOPOLOGY_ERROR` in static mode and `STALE_PRIMARY` in coordinator mode.
- Replication flows one way, primary to replicas. A replica accepts batches only from a primary.
- In coordinator mode, a node with local role `primary` may write only while it also holds current authority for the group and primary term.
- Under normal operation no conflicts occur, because writes serialise through one node.

---

## Conflict Resolution

A resolver produces the outcome when an incoming change targets a row that already exists on the receiving node. The batch applier must apply the changes of one incoming transaction in `seq` order, so that a later change of a transaction always wins over an earlier one whatever the node ids. A change that targets a row which an earlier change of the same incoming transaction inserted, updated, merged, or deleted takes no resolver: a delete deletes the row, an update writes each column whose value in `newData` differs from its value in `oldData`, and an insert writes every column of `newData`. The batch applier invokes the resolver during normal replication and first-sync catch-up. All three resolvers are normative and must produce identical results across languages. A resolver that throws fails the batch with `CONFLICT_ERROR`, which carries the change's `table` and `rowId` and keeps the resolver's error as its cause. See [test-vectors/conflict-resolution.json](test-vectors/conflict-resolution.json).

```text
ConflictContext { table, rowId, localChange or null, remoteChange, localHlc or null, remoteHlc }
ConflictResolution { action: 'accept_remote' | 'keep_local' | 'merge', mergedData? }
```

The local HLC for a row is the highest `hlc` recorded in `_sirannon_column_versions` for that row, or, absent any, the highest `hlc` in the change log for it.

A deleted row keeps one entry in `_sirannon_column_versions` whose `column_name` is the empty string, holding the `hlc` and `node_id` of the latest delete applied to that row, whether local or remote. The batch applier settles a change against an absent row without a resolver, by the highest `hlc` that `_sirannon_column_versions` holds for that row. A delete writes its HLC to that entry when the entry holds a lower HLC or none, and counts as applied. An insert or an update whose HLC is equal to or above that value inserts the row from `newData`, and one below it is skipped. When the row has no entry, an insert inserts the row and an update is skipped.

**LWW (last-writer-wins), the default.**

```text
if localHlc is null: return accept_remote
c = compare(remoteHlc, localHlc)
if c > 0: return accept_remote
if c < 0: return keep_local
if remoteChange.operation == 'delete': return accept_remote
if remoteChange.nodeId > localChange.nodeId: return accept_remote else keep_local
```

LWW compares a delete by its HLC like any other change, so a delete wins over a concurrent update only when its HLC is the higher. Two equal HLCs come from one transaction of one node, so a remote delete wins against an equal local HLC.

**PrimaryWins.** Constructed with the primary node ID.

```text
if remoteChange.nodeId == primaryNodeId: return accept_remote
if localChange.nodeId == primaryNodeId:  return keep_local
return LWW(ctx)
```

**FieldMerge.** Merges non-overlapping column changes; the per-column HLC resolves an overlap.

```text
versions = columnVersions(table, rowId)
if versions is empty: return LWW(ctx)
localChanged  = columns where localData[col]  != oldData[col]
remoteChanged = columns where remoteData[col] != oldData[col]
overlap       = localChanged intersect remoteChanged
merged = copy of localData
for col in remoteChanged not in overlap: merged[col] = remoteData[col]
for col in overlap:
  if compare(remoteHlc, versions[col].hlc) > 0: merged[col] = remoteData[col]
  else if equal and remoteChange.nodeId > versions[col].nodeId: merged[col] = remoteData[col]
return merge(merged)
```

---

## Batches (Normative Wire Format)

```text
ReplicationBatch {
  sourceNodeId: string
  batchId:      string          -- "{nodeId}-{fromSeq}-{toSeq}"
  groupId?:     string
  primaryTerm?: number
  afterSeq?:    number          -- the sequence that the sender read the batch after
  fromSeq:      number
  toSeq:        number
  hlcRange:     { min: string, max: string }
  changes:      List<ReplicationChange>
  checksum:     string
}

ReplicationChange {
  table:         string
  operation:     'insert' | 'update' | 'delete' | 'ddl'
  rowId:         string
  primaryKey:    Map<string, any>
  hlc:           string
  txId:          string
  nodeId:        string
  newData:       Map<string, any> or null
  oldData:       Map<string, any> or null
  ddlStatement?: string
}

ReplicationAck { batchId: string, groupId?, primaryTerm?, ackedSeq: number, nodeId: string }
```

A batch holds at most 1000 changes (normative); a sender splits larger sets. A sender sets `afterSeq` to the sequence from which it read the batch, which is the `lastSentSeq` of the peer at that moment; a node that predates the field leaves it out, and a receiver skips the `afterSeq` check for a batch without it. `groupId` and `primaryTerm` are required in coordinator mode and absent or ignored in static mode; a coordinator-mode receiver rejects a batch or acknowledgement whose term does not match its current view with `STALE_PRIMARY`.

### Checksum (Normative)

The checksum is the lowercase SHA-256 hex digest of the canonical form of the `changes` list. Canonicalisation is transport-independent, computed over decoded native values:

- null is `null`; a boolean is `true` or `false`; a string is its JSON string.
- An integer or a 64-bit integer is `{"__sint":"<decimal>"}`; a finite non-integer number is its JSON number; a non-finite number is `null`.
- A byte array is `{"__blob":"<base64>"}`.
- An array is its elements canonicalised in order.
- An object lists its keys sorted ascending, skips keys whose value is absent, and renders each as `"key":value`.

This canonical form is distinct from the [tagged value encoding](02-core.md#tagged-value-encoding-normative) used on the JSON wire. Both sender and receiver compute and compare the checksum; a mismatch fails with `BATCH_VALIDATION_ERROR`.

---

## Replication Engine

### Configuration and Defaults

| Parameter | Default | Normative |
|-----------|---------|-----------|
| `batchSize` | 100 | No |
| `batchIntervalMs` | 100 | No |
| `maxPendingBatches` | 10 | No |
| `maxBatchChanges` | 1000 | Yes |
| `maxClockDriftMs` | 60,000 | Yes |
| `ackTimeoutMs` | 5,000 | Yes |
| `initialSync` | true | No |
| `syncBatchSize` | 10,000 | No |
| `maxConcurrentSyncs` | 2 | No |
| `maxSyncDurationMs` | 1,800,000 | No |
| `maxSyncLagBeforeReady` | 100 | No |
| `syncAckTimeoutMs` | 30,000 | No |
| `catchUpDeadlineMs` | 600,000 | No |
| `coordinator.sessionTtlMs` | 10,000 | Yes |
| `coordinator.controller` (enabled) | true | Yes |
| `coordinator.controller.leaseTtlMs` | 10,000 | Yes |
| `coordinator.controller.tickIntervalMs` | 1,000 | No |

Static mode generates a `nodeId` when none is given; coordinator mode requires a stable persisted `nodeId` and rejects a configuration without one. The `snapshotThreshold` field is reserved and has no run-time effect.

When a start fails, an implementation must leave the engine stopped, release whatever that start took, and report the failure to its caller, so that a later start opens the transport afresh.

### Sender Loop

Every `batchIntervalMs`, for each peer the topology replicates to (in coordinator mode, every peer while the node holds authority), the sender expires in-flight batches older than `ackTimeoutMs`, skips the peer while `pendingBatches` reaches `maxPendingBatches`, reads one batch from the peer's `lastSentSeq`, and sends it. An expired batch rewinds `lastSentSeq` to retransmit from the lost batch.

### Batch Reception

A receiver accepts a batch only while it is `ready` or `catching-up`, and it processes the batches from one sender one at a time, in the order in which they arrive. The receiver checks that `sourceNodeId` matches the sending peer, that the peer is known, that static-mode topology allows the sender, that the coordinator term is current, that `changes` does not exceed `maxBatchChanges`, and that the wall-clock milliseconds of `hlcRange.max` are no more than `maxClockDriftMs` ahead of the local physical clock. A batch whose `hlcRange.max` is earlier than the local physical clock passes the clock check whatever its age. In coordinator mode a replica that has not reconciled its history with the history of the batch's term discards the batch without applying or acknowledging it and starts that reconciliation, while a former primary and a repairing or faulted node process the batch (see [History Reconciliation](09-failover.md#history-reconciliation)). When `afterSeq` exceeds the highest sequence that the receiver has applied from the sender, the receiver discards the batch without applying it and sends that applied sequence as a `ReplicationAck` with an empty `batchId`. Otherwise the receiver applies the batch, prunes tables dropped by replicated DDL, records the applied sequence, and returns a `ReplicationAck`.

### Write Concern

After a local commit the engine can wait for acknowledgements.

- `local`: return after the local commit.
- `majority`: static mode waits for `floor(connectedPeers / 2) + 1` peers; coordinator mode waits for a majority of the configured voting data-bearing nodes, counting the primary's own durable commit.
- `all`: static mode waits for all connected peers; coordinator mode waits for all configured voting nodes that are not drained.

A concern not met within the timeout (default 5,000 ms) fails with `WRITE_CONCERN_ERROR`. When a write states no concern, static mode returns after the local commit and coordinator mode uses `majority`. A coordinator-mode `majority` write remains durable across automatic failover when only the failed primary is lost and an eligible in-sync replica remains; `local` is an explicit opt-in and is not failover-safe.

### Read Concern

Read concern is enforced in coordinator mode; static mode ignores it.

- `local`: read local state, which may later be quarantined after failover.
- `majority`: the node must be in the in-sync set and not draining or repairing.
- `linearizable`: read from the current primary after it proves live authority for the term.

A read concern that cannot be met fails rather than returning a weaker result.

---

## Write Forwarding

When `writeForwarding` is enabled on a replica, `execute` and `executeBatch` forward to the primary instead of being rejected; `transaction` is never forwarded and fails with `TOPOLOGY_ERROR` on a non-writable node. The forwarder targets the connected primary (in coordinator mode, the current primary for the group) and sends the statements under the current `groupId` and `primaryTerm`; a receiver that no longer holds the term rejects with `STALE_PRIMARY`. With no primary reachable, forwarding fails with `TOPOLOGY_ERROR`. The forwarder sends the caller's write concern with the statements, and the primary waits for that concern, or for its own default when the caller states none, before it replies (see [Write Concern](#write-concern)).

```text
ForwardedTransaction       { statements: List<{ sql, params? }>, requestId, groupId?, primaryTerm?, writeConcern? }
ForwardedTransactionResult { results: List<{ changes, lastInsertRowId }>, requestId, groupId?, primaryTerm? }
```

---

## Peer Tracking

```text
PeerState {
  nodeId:          string
  lastAckedSeq:    number
  lastSentSeq:     number
  lastReceivedHlc: string
  connected:       boolean
  pendingBatches:  number
  inFlightBatches: List<{ batchId, fromSeq, toSeq, sentAt }>
}
```

An acknowledgement advances `lastAckedSeq`, drops in-flight batches up to the acked sequence, and decrements `pendingBatches`. Applying a batch from a peer raises that peer's `lastReceivedHlc` to the batch's `hlcRange.max`. An in-flight batch older than `ackTimeoutMs` is expired, and `lastSentSeq` rewinds to the lost batch's start so retransmission resumes there.

On connecting to the node it replicates from, a node that is `ready` or `catching-up` sends its applied sequence for that node's stream as a `ReplicationAck` with an empty `batchId`. An acknowledgement with an empty `batchId` sets the sender's `lastSentSeq` for that peer to `ackedSeq`, clears its in-flight batches, and resets `pendingBatches` to 0, so that the sender resends from the receiver's applied sequence.

---

## First Sync

A replica that joins for the first time copies a full snapshot from its source.

```text
pending -> syncing -> catching-up -> ready
```

### Sync Messages (Normative Wire Format)

```text
SyncRequest { requestId, joinerNodeId, completedTables: List<string>, groupId?, primaryTerm?, supportsStreamVerification?,
              historyTail?: HistoryPoint }
SyncBatch   { requestId, table, batchIndex, rows: List<Map>, schema?: List<string>, checksum,
              isLastBatchForTable, totalTables?, groupId?, primaryTerm? }
SyncComplete { requestId, snapshotSeq, manifests: List<SyncTableManifest>, groupId?, primaryTerm?,
               historyPoint?: HistoryPoint, resumed? }
HistoryPoint { hlc, txId }
SyncTableManifest { table, rowCount, pkHash?, batchDigest? }
SyncAck      { requestId, joinerNodeId, table, batchIndex, success, error?, groupId?, primaryTerm? }
```

### Flow

1. The joiner sends a `SyncRequest`, listing any tables a resumed sync already finished, and always setting `supportsStreamVerification`.
2. The source holds a read snapshot, records `snapshotSeq` as its current sequence, sends the table schemas as a first batch, then streams each table's rows in `SyncBatch` messages ordered by `batchIndex` from 0. The schema batch carries `totalTables`, the full count the joiner will receive; a source that predates the field omits it and the joiner reports a total of 0.
3. The joiner acknowledges each batch; an out-of-order `batchIndex` is rejected. On the first batch of a table it clears the target table, then inserts each batch's rows after re-verifying the batch checksum.
4. The source sends `SyncComplete` with a per-table manifest.
5. The joiner verifies each manifest, transitions to `catching-up`, and applies changes accumulated since `snapshotSeq`. When its lag falls below `maxSyncLagBeforeReady` (or the catch-up deadline passes), it becomes `ready`.

The source's `maxSyncDurationMs` deadline restarts on every acknowledged batch, so the permitted duration scales with the data; the source aborts when no batch is acknowledged within it. A term change before completion restarts the sync. A `SyncRequest` that carries `historyTail` reconciles a replica's history and receives no copy (see [History Reconciliation](09-failover.md#history-reconciliation)).

### Manifest Verification

With `supportsStreamVerification`, the source sends `batchDigest`: a chained SHA-256 hex digest over the table's batch checksums in order, starting from the empty string, each step hashing the previous digest concatenated with the next batch's `checksum`; an empty table contributes one empty batch. `rowCount` is the total rows streamed. Both sides accumulate these during the stream, so neither re-reads a table. Without the flag, the source sends `pkHash`, the SHA-256 hex digest of all primary key values in primary-key order, which the joiner recomputes by re-reading the table. A manifest is verified by `batchDigest` when present, otherwise by `pkHash`; a manifest with neither fails. Any mismatch wipes the synced tables and restarts the sync.

---

## DDL Safety

DDL received through replication or first sync is validated against an allowlist. Allowed statements begin with `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN`, `DROP TABLE`, `CREATE INDEX`, `CREATE UNIQUE INDEX`, or `DROP INDEX`. A statement is rejected when it targets a table or index under the reserved `_sirannon` or `sqlite_` prefix, contains `;`, contains `AS SELECT`, or references `load_extension`, `ATTACH`, `randomblob`, `zeroblob`, `writefile`, `readfile`, or `fts3_tokenizer`. A DDL statement that fails validation fails with `BATCH_VALIDATION_ERROR`.

The replication engine validates every write that it runs through `execute`, `executeBatch`, `transaction`, or write forwarding against the same allowlist before it runs the write. A statement that begins with `CREATE`, `ALTER`, or `DROP` once its leading whitespace and comments are removed is a schema change; a schema change that fails validation fails with `REPLICATION_ERROR` before SQLite runs it. The engine records an accepted schema change in the change log without its leading whitespace and comments.
