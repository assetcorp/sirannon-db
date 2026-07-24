# Sirannon Replication Specification

Replication distributes writes between Sirannon nodes. A single primary per
replication group accepts writes and sends them to replicas as batched change
sets, each change carrying a Hybrid Logical Clock timestamp for causal ordering.
This document covers server-to-server replication; end-user device sync is a
separate feature defined in [08-device-sync.md](08-device-sync.md).

Sirannon supports two authority modes:

- **Static mode.** The primary and replicas are configured outside the engine.
  Static mode has no Sirannon-owned automatic failover.
- **Coordinator mode.** A linearisable cluster coordinator owns primary
  authority, node liveness, safe promotion, client routing metadata, and
  fail-closed write behaviour.

Conflict resolvers are batch-application primitives; a receiver uses one when an
incoming change targets a row that already exists locally. They do not grant
multi-writer authority and do not make failover safe.

---

## Hybrid Logical Clock (HLC)

Each node keeps one HLC that orders events causally without relying on
synchronised wall clocks.

```text
HLC { wallMs: number, logical: number (0 to 65535), nodeId: string }
```

### Encoded Format (Normative)

```text
{wallMs_hex}-{logical_hex}-{nodeId}
```

`wallMs_hex` is 12 zero-padded hexadecimal digits of the wall-clock milliseconds;
`logical_hex` is 4 zero-padded hexadecimal digits of the logical counter; `nodeId`
is the node identifier, which may contain hyphens. All implementations must
produce and parse this encoding. The encoded string orders by plain lexicographic
comparison, which preserves causal order.

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

The clock is persisted in `_sirannon_meta` under `hlc_clock`. On start, a node
seeds its clock from the persisted value and from the highest `hlc` observed in
the change log and per-column version table, so it never moves backwards. See
[test-vectors/hlc.json](test-vectors/hlc.json).

---

## Replication Tables

Replication uses the shared change log (`_sirannon_changes`, see
[02-core.md](02-core.md#change-log)) plus these tables:

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

A node reads its outgoing batch from `_sirannon_changes` where `seq > afterSeq`
and `node_id = localNodeId`. It records applied remote changes in
`_sirannon_applied_changes` for deduplication and merges each remote HLC into its
clock through `receive`.

---

## Primary-Replica Topology

```text
canWrite():                 role == 'primary'
shouldReplicateTo(peer):    role == 'primary' and peer.role == 'replica'
shouldAcceptFrom(peer):     role == 'replica' and peer.role == 'primary'
```

- Only a primary writes. A replica rejects a direct write with `TOPOLOGY_ERROR`
  in static mode and `STALE_PRIMARY` in coordinator mode.
- Replication flows one way, primary to replicas. A replica accepts batches only
  from a primary.
- In coordinator mode, a node with local role `primary` may write only while it
  also holds current authority for the group and primary term.
- Under normal operation no conflicts occur, because writes serialise through one
  node.

---

## Conflict Resolution

A resolver decides the outcome when an incoming change targets a row that already
exists on the receiving node. The batch applier invokes it during normal
replication and first-sync catch-up. All three resolvers are normative and must
produce identical results across languages. See
[test-vectors/conflict-resolution.json](test-vectors/conflict-resolution.json).

```text
ConflictContext { table, rowId, localChange or null, remoteChange, localHlc or null, remoteHlc }
ConflictResolution { action: 'accept_remote' | 'keep_local' | 'merge', mergedData? }
```

The local HLC for a row is the highest `hlc` recorded in `_sirannon_column_versions`
for that row, or, absent any, the highest `hlc` in the change log for it.

**LWW (last-writer-wins), the default.**

```text
if localHlc is null: return accept_remote
c = compare(remoteHlc, localHlc)
if c > 0: return accept_remote
if c < 0: return keep_local
if remoteChange.nodeId > localChange.nodeId: return accept_remote else keep_local
```

**PrimaryWins.** Constructed with the primary node ID.

```text
if remoteChange.nodeId == primaryNodeId: return accept_remote
if localChange.nodeId == primaryNodeId:  return keep_local
return LWW(ctx)
```

**FieldMerge.** Merges non-overlapping column changes; per-column HLC decides an
overlap.

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

A batch holds at most 1000 changes (normative); a sender splits larger sets.
`groupId` and `primaryTerm` are required in coordinator mode and absent or ignored
in static mode; a coordinator-mode receiver rejects a batch or acknowledgement
whose term does not match its current view with `STALE_PRIMARY`.

### Checksum (Normative)

The checksum is the lowercase SHA-256 hex digest of the canonical form of the
`changes` list. Canonicalisation is transport-independent, computed over decoded
native values:

- null is `null`; a boolean is `true` or `false`; a string is its JSON string.
- An integer or a 64-bit integer is `{"__sint":"<decimal>"}`; a finite
  non-integer number is its JSON number; a non-finite number is `null`.
- A byte array is `{"__blob":"<base64>"}`.
- An array is its elements canonicalised in order.
- An object lists its keys sorted ascending, skips keys whose value is absent,
  and renders each as `"key":value`.

This canonical form is distinct from the
[tagged value encoding](02-core.md#tagged-value-encoding-normative) used on the
JSON wire. Both sender and receiver compute and compare the checksum; a mismatch
fails with `BATCH_VALIDATION_ERROR`.

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

Static mode generates a `nodeId` when none is given; coordinator mode requires a
stable persisted `nodeId` and rejects a configuration without one. The
`snapshotThreshold` field is reserved and has no run-time effect.

### Sender Loop

Every `batchIntervalMs`, for each peer the topology replicates to (in coordinator
mode, every peer while the node holds authority), the sender expires in-flight
batches older than `ackTimeoutMs`, skips the peer while `pendingBatches` reaches
`maxPendingBatches`, reads one batch from the peer's `lastSentSeq`, and sends it.
An expired batch rewinds `lastSentSeq` to retransmit from the lost batch.

### Batch Reception

A batch is accepted only while the node is `ready` or `catching-up`. The receiver
checks that `sourceNodeId` matches the sending peer, that the peer is known, that
static-mode topology allows the sender, that the coordinator term is current, that
`changes` does not exceed `maxBatchChanges`, and that `hlcRange.max` is within
`maxClockDriftMs` of the local clock. It applies the batch, prunes tables dropped
by replicated DDL, records the applied sequence, and returns a `ReplicationAck`.

### Write Concern

After a local commit the engine can wait for acknowledgements.

- `local`: return after the local commit.
- `majority`: static mode waits for `floor(connectedPeers / 2) + 1` peers;
  coordinator mode waits for a majority of the configured voting data-bearing
  nodes, counting the primary's own durable commit.
- `all`: static mode waits for all connected peers; coordinator mode waits for all
  configured voting nodes that are not drained.

A concern not met within the timeout (default 5,000 ms) fails with
`WRITE_CONCERN_ERROR`. When a write states no concern, static mode returns after
the local commit and coordinator mode uses `majority`. A coordinator-mode
`majority` write survives automatic failover when only the failed primary is lost
and an eligible in-sync replica remains; `local` is an explicit opt-in and is not
failover-safe.

### Read Concern

Read concern is enforced in coordinator mode; static mode ignores it.

- `local`: read local state, which may later be quarantined after failover.
- `majority`: the node must be in the in-sync set and not draining or repairing.
- `linearizable`: read from the current primary after it proves live authority for
  the term.

A read concern that cannot be met fails rather than returning a weaker result.

---

## Write Forwarding

When `writeForwarding` is enabled on a replica, `execute` and `executeBatch`
forward to the primary instead of being rejected; `transaction` is never forwarded
and fails with `TOPOLOGY_ERROR` on a non-writable node. The forwarder targets the
connected primary (in coordinator mode, the current primary for the group) and
sends the statements under the current `groupId` and `primaryTerm`; a receiver
that no longer holds the term rejects with `STALE_PRIMARY`. With no primary
reachable, forwarding fails with `TOPOLOGY_ERROR`.

```text
ForwardedTransaction       { statements: List<{ sql, params? }>, requestId, groupId?, primaryTerm? }
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

An acknowledgement advances `lastAckedSeq`, drops in-flight batches up to the
acked sequence, and decrements `pendingBatches`. An in-flight batch older than
`ackTimeoutMs` is expired, and `lastSentSeq` rewinds to the lost batch's start so
retransmission resumes there.

---

## First Sync

A replica that joins for the first time copies a full snapshot from its source.

```text
pending -> syncing -> catching-up -> ready
```

### Sync Messages (Normative Wire Format)

```text
SyncRequest { requestId, joinerNodeId, completedTables: List<string>, groupId?, primaryTerm?, supportsStreamVerification? }
SyncBatch   { requestId, table, batchIndex, rows: List<Map>, schema?: List<string>, checksum,
              isLastBatchForTable, totalTables?, groupId?, primaryTerm? }
SyncComplete { requestId, snapshotSeq, manifests: List<SyncTableManifest>, groupId?, primaryTerm? }
SyncTableManifest { table, rowCount, pkHash?, batchDigest? }
SyncAck      { requestId, joinerNodeId, table, batchIndex, success, error?, groupId?, primaryTerm? }
```

### Flow

1. The joiner sends a `SyncRequest`, listing any tables a resumed sync already
   finished, and always setting `supportsStreamVerification`.
2. The source holds a read snapshot, records `snapshotSeq` as its current
   sequence, sends the table schemas as a first batch, then streams each table's
   rows in `SyncBatch` messages ordered by `batchIndex` from 0. The schema batch
   carries `totalTables`, the full count the joiner will receive; a source that
   predates the field omits it and the joiner reports a total of 0.
3. The joiner acknowledges each batch; an out-of-order `batchIndex` is rejected.
   On the first batch of a table it clears the target table, then inserts each
   batch's rows after re-verifying the batch checksum.
4. The source sends `SyncComplete` with a per-table manifest.
5. The joiner verifies each manifest, transitions to `catching-up`, and applies
   changes accumulated since `snapshotSeq`. When its lag falls below
   `maxSyncLagBeforeReady` (or the catch-up deadline passes), it becomes `ready`.

The source's `maxSyncDurationMs` deadline restarts on every acknowledged batch, so
the permitted duration scales with the data; the source aborts when no batch is
acknowledged within it. A term change before completion restarts the sync.

### Manifest Verification

With `supportsStreamVerification`, the source sends `batchDigest`: a chained
SHA-256 hex digest over the table's batch checksums in order, starting from the
empty string, each step hashing the previous digest concatenated with the next
batch's `checksum`; an empty table contributes one empty batch. `rowCount` is the
total rows streamed. Both sides accumulate these during the stream, so neither
re-reads a table. Without the flag, the source sends `pkHash`, the SHA-256 hex
digest of all primary key values in primary-key order, which the joiner recomputes
by re-reading the table. A manifest is verified by `batchDigest` when present,
otherwise by `pkHash`; a manifest with neither fails. Any mismatch wipes the synced
tables and restarts the sync.

---

## DDL Safety

DDL received through replication or first sync is validated against an allowlist.
Allowed statements begin with `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN`,
`DROP TABLE`, `CREATE INDEX`, or `DROP INDEX`. A statement is rejected when it
contains `;`, contains `AS SELECT`, or references `load_extension`, `ATTACH`,
`randomblob`, `zeroblob`, `writefile`, `readfile`, or `fts3_tokenizer`. A DDL
statement that fails validation fails with `BATCH_VALIDATION_ERROR`.

---

## Coordinator-Backed Failover

Coordinator mode uses a linearisable coordinator to store authority metadata and
provide watches. The first production backend is etcd; an in-memory backend is
allowed for tests and local development. The coordinator stores only authority
metadata (controller lease, node session leases, group configuration, current
primary, primary term, in-sync set, and compact progress markers) and never user
rows or the replication log.

The coordinator must provide, at least: acquiring and renewing a controller
lease; registering, reading, and deregistering node session leases; reading,
writing, and watching replication-group state; comparing and advancing the primary
term atomically; and admitting a node to the in-sync set only against a proven
durability point.

### Replication Group and Node Identity

A replication group is the set of nodes holding copies of one database and sharing
one primary authority; a process may host several groups, with no process-wide
global primary. Each data-bearing node has a stable persisted identity tied to its
durable database copy: a restarted process reusing that copy reuses its identity,
while a new empty node receives a new identity and must sync before serving safe
reads or becoming promotable. Session leases represent a running session and do
not replace stable identity.

### Term Fencing

Each group has a monotonically increasing `primaryTerm`. Every coordinator-mode
write path and replication message carries the current term; promotion atomically
increments the term and records the new primary. A primary may accept writes only
while it proves current authority, and must fail closed for new writes and for
in-flight writes that cannot prove it when renewal fails. Replicas reject batches,
sync messages, and forwarded writes from a stale term or a non-current primary. A
former primary that observes a newer term demotes and compares its history: a safe
node rejoins through sync, while a node with local-only writes is removed from the
in-sync set, marked faulted, and kept out of the read, write, and promotion paths
until an operator remediates it.

### In-Sync Set and Promotion

The in-sync set holds replicas proven current enough for safe reads and promotion;
a live node is not automatically in sync. A node is admitted to the set only when
its applied sequence reaches the group's recorded durability point, and only by
the current primary. A replica that falls behind, misses acknowledgements, or
reports a storage or sync error is removed before writes that exclude it are
acknowledged.

Automatic promotion may choose only a replica whose session lease is alive, that
is in the in-sync set, that has no storage, checksum, sync, or repair error, and
whose promotion the controller can record while atomically advancing the term. When
no replica qualifies, Sirannon fails closed with `NO_SAFE_PRIMARY` and does not
promote an arbitrary node. Production-safe automatic write failover requires at
least three voting data-bearing nodes in a group.

### Controller, Liveness, and Maintenance

One active controller per cluster holds the controller lease and makes failover
decisions; standby controllers watch the lease and take over on loss. Coordinator
lease expiry or explicit deregistration is the authoritative node-death signal; a
transport disconnect is connection state that may drop a replica from the in-sync
set but must not by itself trigger promotion. Coordinator mode provides
maintenance primitives to drain a node, move primary duty before planned work,
remove replicas from safe sets, wait for in-flight work, deregister a session, and
rejoin after restart. Sirannon does not provision infrastructure; it detects
registration through the coordinator, assigns work, validates catch-up, and marks
a node safe only once it is current.

### Recovery and Rolling Upgrades

When Sirannon cannot prove a safe primary, it fails closed; there is no
force-promotion or unsafe-recovery API. An operator restores a backup, rebuilds a
node, or repairs the deployment, then rejoins through the normal sync path. Nodes
register package, spec, and protocol compatibility metadata; the controller
rejects an incompatible major protocol change and allows mixed minor or patch
levels only while metadata formats, wire formats, and safety semantics stay
compatible. New durable metadata or new safety semantics require an explicit
cluster compatibility gate before activation.

### Conformance Invariants

Coordinator mode conforms only when its test suite proves, with a real coordinator
and real multi-node groups: at most one writable primary per group and term; no
loss of a `majority`-acknowledged write when only the failed primary is lost; stale
primaries reject writes, batches, forwarded transactions, and sync messages;
minority partitions fail closed for writes; only in-sync replicas are promoted;
clients reroute writes to the new primary or receive a clear error; returning former
primaries rejoin through sync when safe and are quarantined when they hold
local-only writes; and health state reports write availability, read availability,
coordinator status, current primary, primary term, and repair state accurately.
