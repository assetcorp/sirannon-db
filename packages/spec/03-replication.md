# Sirannon Replication Specification

This document defines the distributed replication system: the
replication engine, Hybrid Logical Clocks, the replication log,
primary-replica topology, conflict resolution for split-brain
recovery, peer tracking, first sync, and write forwarding. All
Sirannon implementations must follow the contracts defined here.

---

## Overview

Sirannon replicates changes between nodes using an eventual
consistency model. A single primary node accepts writes and
distributes them to replicas as batched change sets. Each change
carries a Hybrid Logical Clock (HLC) timestamp for causal ordering.
During failover (when a former primary recovers after a split-brain
event), conflict resolvers determine which version of a row wins.

---

## Hybrid Logical Clock (HLC)

The HLC provides causal ordering across nodes without relying on
perfectly synchronised wall clocks. Each node maintains its own HLC
instance.

### HLC State

```text
HLC {
  wallMs:  number    (wall-clock time in milliseconds)
  logical: number    (logical counter, 0 to 65535)
  nodeId:  string    (unique node identifier)
}
```

### Encoded Format

An HLC timestamp is encoded as a single string:

```text
{wallMs_hex}-{logical_hex}-{nodeId}
```

- `wallMs_hex`: 12-character zero-padded hexadecimal representation
  of the wall-clock milliseconds.
- `logical_hex`: 4-character zero-padded hexadecimal representation
  of the logical counter.
- `nodeId`: the node identifier string (may contain hyphens).

This format is **normative**. All implementations must produce
and parse this exact encoding. The encoded string is directly
comparable using lexicographic string comparison, which preserves
causal ordering.

### now()

Generates a new HLC timestamp for a local event.

```text
function now():
  physicalMs = currentTimeMillis()
  if physicalMs > wallMs:
    wallMs = physicalMs
    logical = 0
  else:
    logical = logical + 1
  if logical > MAX_LOGICAL (65535):
    throw ReplicationError('HLC logical counter overflow')
  return encode(wallMs, logical, nodeId)
```

### receive(remote)

Merges a remote HLC timestamp into the local clock. Called when a
replication batch arrives from another node.

```text
function receive(remote: string):
  remoteWallMs, remoteLogical, _ = decode(remote)
  physicalMs = currentTimeMillis()

  if physicalMs > max(wallMs, remoteWallMs):
    wallMs = physicalMs
    logical = 0
  else if remoteWallMs > wallMs:
    wallMs = remoteWallMs
    logical = remoteLogical + 1
  else if wallMs > remoteWallMs:
    logical = logical + 1
  else:
    logical = max(logical, remoteLogical) + 1

  if logical > MAX_LOGICAL (65535):
    throw ReplicationError('HLC logical counter overflow')
  return encode(wallMs, logical, nodeId)
```

### compare(a, b)

Compares two encoded HLC timestamps. Returns `-1`, `0`, or `1`.
Because the encoding is lexicographically orderable, this is a
direct string comparison.

### decode(hlc)

Parses an encoded HLC string into its components. Throws if the
format is invalid.

### Test Vectors

See [test-vectors/hlc.json](test-vectors/hlc.json) for encoding,
decoding, and comparison test cases.

---

## Replication Log

The replication log tracks all local changes and manages the
application of remote changes.

### Internal Tables

Implementations must create these tables when replication is
enabled. Table names and schemas are recommended; implementations
may use different internal storage as long as they produce
conforming batch messages.

**Change tracking (shared with CDC):**

```sql
CREATE TABLE _sirannon_changes (
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name  TEXT NOT NULL,
  operation   TEXT NOT NULL,
  row_id      TEXT NOT NULL,
  changed_at  REAL NOT NULL DEFAULT (unixepoch('subsec')),
  old_data    TEXT,
  new_data    TEXT,
  node_id     TEXT NOT NULL DEFAULT '',
  tx_id       TEXT NOT NULL DEFAULT '',
  hlc         TEXT NOT NULL DEFAULT ''
)
```

**Peer state:**

```sql
CREATE TABLE _sirannon_peer_state (
  peer_node_id     TEXT PRIMARY KEY,
  last_acked_seq   INTEGER NOT NULL DEFAULT 0,
  last_received_hlc TEXT NOT NULL DEFAULT '',
  updated_at       REAL NOT NULL
)
```

**Applied changes (deduplication):**

```sql
CREATE TABLE _sirannon_applied_changes (
  source_node_id TEXT NOT NULL,
  source_seq     INTEGER NOT NULL,
  applied_at     REAL NOT NULL,
  PRIMARY KEY (source_node_id, source_seq)
)
```

**Per-column HLC versions (for field-merge conflict resolution):**

```sql
CREATE TABLE _sirannon_column_versions (
  table_name  TEXT NOT NULL,
  row_id      TEXT NOT NULL,
  column_name TEXT NOT NULL,
  hlc         TEXT NOT NULL,
  node_id     TEXT NOT NULL,
  PRIMARY KEY (table_name, row_id, column_name)
)
```

**Sync state:**

```sql
CREATE TABLE _sirannon_sync_state (
  table_name     TEXT PRIMARY KEY,
  status         TEXT NOT NULL DEFAULT 'pending',
  row_count      INTEGER NOT NULL DEFAULT 0,
  pk_hash        TEXT NOT NULL DEFAULT '',
  snapshot_seq   INTEGER,
  source_peer_id TEXT,
  started_at     REAL,
  completed_at   REAL,
  request_id     TEXT
)
```

### readBatch(afterSeq, batchSize)

Reads local changes from `_sirannon_changes` where
`seq > afterSeq` and `node_id = localNodeId`, limited to
`batchSize` rows. Returns a `ReplicationBatch` or `null` if no
changes are pending.

### applyBatch(batch, resolver)

Applies a remote batch to the local database. For each change:

1. Validate the batch checksum against a recomputed checksum.
2. Skip already-applied changes (deduplication via
   `_sirannon_applied_changes`).
3. For each change, check if the target row exists locally.
4. If no local row exists, apply the change directly.
5. If a local row exists, invoke the conflict resolver.
6. Record the applied change for future deduplication.
7. Merge the remote HLC into the local clock via `receive()`.

### stampChanges(tx, afterSeq, txId)

Stamps unstamped local changes (those with empty `node_id`) with
the local node ID, a transaction ID, and a fresh HLC timestamp.
This is called after each local write to mark changes as belonging
to this node.

---

## Primary-Replica Topology

Sirannon uses a primary-replica topology where a single primary
node accepts writes and replicates to read replicas.

```text
PrimaryReplicaTopology {
  role: 'primary' | 'replica'

  canWrite(): boolean
    -> role === 'primary'

  shouldReplicateTo(peerId, peerRole): boolean
    -> role === 'primary' AND peerRole === 'replica'

  shouldAcceptFrom(peerId, peerRole): boolean
    -> role === 'replica' AND peerRole === 'primary'

  requiresConflictResolution(): boolean
    -> false
}
```

### Rules

- Only the primary can write. Replicas must reject direct writes
  with error code `TOPOLOGY_ERROR`.
- Replication flows in one direction: primary to replicas.
- Replicas only accept batches from a node with role `primary`.
- Under normal operation, no conflicts occur because writes are
  serialised through a single node.

### Failover

Automated failover is not part of this specification. Failover is
an operational concern handled by external orchestration (e.g.,
health checks, service discovery, manual promotion).

During a split-brain event (a former primary recovers with writes
that the new primary also made), conflict resolvers handle the
divergent state. See [Conflict Resolution](#conflict-resolution).

---

## Conflict Resolution

Conflict resolvers determine the outcome when two versions of the
same row exist on different nodes. This occurs during split-brain
recovery in primary-replica deployments.

All three built-in resolvers are **normative**. Every
implementation must ship them, and they must produce identical
results for identical inputs across all languages.

### ConflictResolver Interface

```text
ConflictResolver {
  resolve(ctx: ConflictContext): ConflictResolution or async ConflictResolution
}

ConflictContext {
  table:        string
  rowId:        string
  localChange:  ReplicationChange or null
  remoteChange: ReplicationChange
  localHlc:     string or null
  remoteHlc:    string
}

ConflictResolution {
  action:      'accept_remote' | 'keep_local' | 'merge'
  mergedData?: Map<string, any>
}
```

### LWW Resolver (Last-Writer-Wins)

The default resolver. Compares HLC timestamps to determine the
winner.

```text
function resolve(ctx):
  if ctx.localHlc is null:
    return { action: 'accept_remote' }

  cmp = HLC.compare(ctx.remoteHlc, ctx.localHlc)

  if cmp > 0:
    return { action: 'accept_remote' }
  if cmp < 0:
    return { action: 'keep_local' }

  // Tie-break: lexicographic nodeId comparison
  if ctx.remoteChange.nodeId > ctx.localChange.nodeId:
    return { action: 'accept_remote' }

  return { action: 'keep_local' }
```

**Determinism guarantee.** Because HLC comparison is deterministic
and node ID tie-breaking uses lexicographic ordering, every node
reaches the same resolution without coordination.

### PrimaryWins Resolver

Unconditional primary authority. Designed for primary-replica
topologies where the primary's version should always take
precedence during split-brain recovery.

```text
function resolve(ctx):
  if ctx.remoteChange.nodeId === primaryNodeId:
    return { action: 'accept_remote' }
  if ctx.localChange?.nodeId === primaryNodeId:
    return { action: 'keep_local' }
  // Neither side is primary; fall back to LWW
  return lwwResolver.resolve(ctx)
```

Requires the primary node ID at construction time.

### FieldMerge Resolver

Merges non-overlapping column changes. When two nodes modify
different columns of the same row, both changes are preserved.
When both nodes modify the same column, per-column HLC timestamps
determine the winner.

```text
function resolve(ctx):
  columnVersions = getColumnVersions(ctx.table, ctx.rowId)

  if columnVersions is empty:
    return lwwResolver.resolve(ctx)

  localData  = ctx.localChange?.newData or {}
  remoteData = ctx.remoteChange.newData or {}
  oldData    = ctx.remoteChange.oldData or {}

  localChanged  = columns where localData[col] != oldData[col]
  remoteChanged = columns where remoteData[col] != oldData[col]
  overlapping   = intersection of localChanged and remoteChanged

  merged = { ...localData }

  if overlapping is empty:
    // Non-overlapping: merge both sides
    for col in remoteChanged:
      merged[col] = remoteData[col]
    return { action: 'merge', mergedData: merged }

  // Overlapping: per-column HLC resolution
  for col in overlapping:
    colVersion = columnVersions.get(col)
    cmp = HLC.compare(ctx.remoteHlc, colVersion.hlc)
    if cmp > 0:
      merged[col] = remoteData[col]
    else if cmp === 0 AND ctx.remoteChange.nodeId > colVersion.nodeId:
      merged[col] = remoteData[col]

  for col in remoteChanged where col not in overlapping:
    merged[col] = remoteData[col]

  return { action: 'merge', mergedData: merged }
```

Requires a `getColumnVersions` function at construction time that
reads from the `_sirannon_column_versions` table.

### Test Vectors

See [test-vectors/conflict-resolution.json](test-vectors/conflict-resolution.json)
for resolution test cases covering all three resolvers.

---

## Replication Batch

A batch is the unit of replication between nodes. It contains one
or more changes with a checksum for integrity verification.

### ReplicationBatch (Normative Wire Format)

```text
ReplicationBatch {
  sourceNodeId:  string
  batchId:       string
  fromSeq:       bigint
  toSeq:         bigint
  hlcRange:      { min: string, max: string }
  changes:       List<ReplicationChange>
  checksum:      string
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
```

### Batch ID Format

```text
{nodeId}-{fromSeq}-{toSeq}
```

### Checksum

The checksum is computed as the SHA-256 hex digest of the
deterministic protobuf encoding of the changes list. Deterministic
encoding means fields are serialised in field-number order and map
entries are sorted by key. Both the sender and receiver must
compute and compare this checksum. A mismatch must result in error
code `BATCH_VALIDATION_ERROR`.

### Maximum Batch Size

The maximum number of changes in a single batch must not exceed
1000. This is a normative cap to prevent memory exhaustion on
receiving nodes. Senders must split larger change sets into
multiple batches.

### ReplicationAck (Normative Wire Format)

```text
ReplicationAck {
  batchId:   string
  ackedSeq:  bigint
  nodeId:    string
}
```

---

## Replication Engine

The engine orchestrates change batching, transmission, reception,
and application.

### Configuration

```text
ReplicationConfig {
  nodeId?:                  string     (auto-generated if omitted)
  topology:                 Topology
  transport:                ReplicationTransport
  transportConfig?:         TransportConfig
  writeForwarding?:         boolean
  conflictResolvers?:       Map<string, ConflictResolver>
  defaultConflictResolver?: ConflictResolver
  batchSize?:               number
  batchIntervalMs?:         number
  maxPendingBatches?:       number
  maxClockDriftMs?:         number
  maxBatchChanges?:         number
  ackTimeoutMs?:            number
  initialSync?:             boolean
  syncBatchSize?:           number
  maxConcurrentSyncs?:      number
  maxSyncDurationMs?:       number
}
```

### Default Values

| Parameter | Default | Normative? |
|-----------|---------|------------|
| `batchSize` | 100 (recommended) | No |
| `batchIntervalMs` | 100 ms (recommended) | No |
| `maxClockDriftMs` | 60,000 ms | Yes |
| `maxPendingBatches` | 10 (recommended) | No |
| `maxBatchChanges` | 1,000 | Yes |
| `ackTimeoutMs` | 5,000 ms | Yes |
| `initialSync` | `true` | No |
| `syncBatchSize` | 10,000 (recommended) | No |
| `maxConcurrentSyncs` | 2 (recommended) | No |
| `maxSyncDurationMs` | 1,800,000 ms (recommended) | No |

### Sender Loop

The sender loop runs every `batchIntervalMs` milliseconds:

1. For each connected peer where `topology.shouldReplicateTo()`
   returns `true`:
   a. Check that `pendingBatches < maxPendingBatches`.
   b. Read a batch from the replication log.
   c. Send the batch via the transport.
   d. Record the batch as in-flight.
2. Expire in-flight batches that exceed `ackTimeoutMs`.
3. On expiry, reset `lastSentSeq` to retry from the expired
   batch's starting sequence.

### Batch Reception

When a batch arrives from a remote node:

1. Verify the sender is an authorised peer via
   `topology.shouldAcceptFrom()`.
2. Verify the HLC timestamps are within `maxClockDriftMs` of the
   local clock.
3. Apply the batch via the replication log.
4. Send an acknowledgment back to the sender.

### Write Concern

After executing a write, the engine can wait for replication
acknowledgments:

- `local`: Return immediately after local commit.
- `majority`: Wait for `floor(connectedPeers / 2) + 1` peers to
  acknowledge. Reject with `WRITE_CONCERN_ERROR` on timeout.
- `all`: Wait for all connected peers to acknowledge. Reject with
  `WRITE_CONCERN_ERROR` on timeout or if any peer disconnects.

---

## Write Forwarding

When `writeForwarding` is enabled on a replica, write operations
are forwarded to the primary instead of being rejected.

```text
function execute(sql, params, options):
  if topology.canWrite():
    return executeLocally(sql, params, options)
  if writeForwarding:
    return forwardStatements([{ sql, params }], options)
  throw TopologyError('This node cannot accept writes')
```

The forwarding mechanism finds the connected peer with role
`primary` and sends a `ForwardedTransaction` message. If no
primary is connected, throw with error code `TOPOLOGY_ERROR`.

### ForwardedTransaction (Normative Wire Format)

```text
ForwardedTransaction {
  statements:  List<{ sql: string, params?: Params }>
  requestId:   string
}

ForwardedTransactionResult {
  results:    List<{ changes: number, lastInsertRowId: number or string }>
  requestId:  string
}
```

---

## Peer Tracking

The peer tracker maintains state for each connected peer.

```text
PeerState {
  nodeId:          string
  lastAckedSeq:    bigint
  lastSentSeq:     bigint
  lastReceivedHlc: string
  connected:       boolean
  pendingBatches:  number
  inFlightBatches: List<InFlightBatch>
}

InFlightBatch {
  batchId:  string
  fromSeq:  bigint
  toSeq:    bigint
  sentAt:   number
}
```

### Acknowledgment Handling

When an ack arrives, update `lastAckedSeq`, remove the
acknowledged batch from `inFlightBatches`, and decrement
`pendingBatches`.

### Batch Timeout

In-flight batches that exceed `ackTimeoutMs` are expired. On
expiry, reset `lastSentSeq` to `earliestFromSeq - 1` to
retransmit starting from the lost batch.

---

## First Sync

When a replica joins the cluster for the first time, it performs
a first sync to receive a full snapshot of the primary's data.

### Sync Phases

```text
pending -> syncing -> catching-up -> ready
```

1. **pending**: Replica is waiting to find a sync source.
2. **syncing**: Replica is receiving snapshot data.
3. **catching-up**: Snapshot is complete; replica is applying
   changes that accumulated during the sync.
4. **ready**: Replica is fully synchronised and serving reads.

### Sync Protocol Messages (Normative Wire Format)

```text
SyncRequest {
  requestId:       string
  joinerNodeId:    string
  completedTables: List<string>
}

SyncBatch {
  requestId:          string
  table:              string
  batchIndex:         number
  rows:               List<Map<string, any>>
  schema?:            List<string>
  checksum:           string
  isLastBatchForTable: boolean
}

SyncComplete {
  requestId:   string
  snapshotSeq: bigint
  manifests:   List<SyncTableManifest>
}

SyncTableManifest {
  table:    string
  rowCount: number
  pkHash:   string
}

SyncAck {
  requestId:   string
  joinerNodeId: string
  table:        string
  batchIndex:   number
  success:      boolean
  error?:       string
}
```

### Sync Flow

1. Replica sends `SyncRequest` to primary.
2. Primary sends table schemas as DDL batches.
3. Primary sends table rows as `SyncBatch` messages, ordered by
   table and batch index.
4. Replica acknowledges each batch with `SyncAck`.
5. Primary sends `SyncComplete` with per-table manifests and the
   snapshot sequence number.
6. Replica verifies manifests (row count and primary key hash).
7. Replica transitions to `catching-up` and applies changes that
   accumulated since `snapshotSeq`.
8. When the replica's lag drops below `maxSyncLagBeforeReady`,
   it transitions to `ready`.

### Batch Ordering

Sync batches for each table must arrive in order by `batchIndex`,
starting at 0. The receiver must reject out-of-order batches.

### Manifest Verification

The `pkHash` in each manifest is the SHA-256 hex digest of all
primary key values in the table, concatenated in primary key order.
The receiver recomputes this hash locally and compares it against
the manifest. A mismatch indicates data corruption during sync.

---

## DDL Safety

When applying DDL statements received via replication, the engine
must validate them against an allowlist:

**Allowed prefixes:**
- `CREATE TABLE`
- `ALTER TABLE ... ADD COLUMN`
- `DROP TABLE`
- `CREATE INDEX`
- `DROP INDEX`

**Rejected patterns:**
- Statements containing `;` (multiple statements).
- Statements containing `AS SELECT` (views, CTAs).
- Statements referencing dangerous functions: `load_extension`,
  `ATTACH`, `randomblob`, `zeroblob`, `writefile`, `readfile`,
  `fts3_tokenizer`.

Implementations must reject any DDL that does not pass validation
with error code `BATCH_VALIDATION_ERROR`.
