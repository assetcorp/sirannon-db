# Sirannon Replication Specification

This document defines the distributed replication system: the
replication engine, Hybrid Logical Clocks, the replication log,
primary-replica topology, coordinator-backed failover, conflict
resolution, peer tracking, first sync, and write forwarding. All
Sirannon implementations must follow the contracts defined here.

---

## Overview

Sirannon replicates changes between nodes using a primary-replica
model. A single primary node per replication group accepts writes
and distributes them to replicas as batched change sets. Each
change carries a Hybrid Logical Clock (HLC) timestamp for causal
ordering.

Sirannon supports two authority modes:

- **Static mode**: the primary and replicas are configured outside
  the replication engine. Static mode has no Sirannon-owned
  automatic failover.
- **Coordinator mode**: Sirannon uses a real cluster coordinator
  to own primary authority, node liveness, safe promotion, client
  routing metadata, and fail-closed write behaviour.

Conflict resolvers are not the normal high-availability path.
They are used during explicit repair and disaster recovery when
divergent writes must be reconciled.

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
node accepts writes and replicates to read replicas. In static
mode, the configured role determines write authority. In
coordinator mode, the current primary and its primary term are
read from the coordinator.

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
- In coordinator mode, a node with local role `primary` can write
  only when it also holds current authority for the replication
  group and primary term.
- Under normal operation, no conflicts occur because writes are
  serialised through a single node.

### Failover

Static mode has no Sirannon-owned automatic failover. If a static
primary fails, writes remain unavailable until an external system
or operator promotes another node and reroutes clients.

Coordinator mode makes automatic failover part of the Sirannon
contract. See [Coordinator-Backed Failover](#coordinator-backed-failover).

---

## Coordinator-Backed Failover

Coordinator mode uses a linearizable cluster coordinator to store
authority metadata and provide watches. The first production
coordinator backend is etcd. In-memory coordinators are allowed
only for tests and local development.

The coordinator stores only authority metadata:

- controller lease;
- node session leases;
- replication-group configuration;
- current primary;
- primary term;
- in-sync set;
- compact progress markers.

The coordinator must not store user rows or the full replication
log.

### Replication Group

A replication group is the set of Sirannon nodes that hold copies
of the same database and share one primary authority. Primary
authority, write concern, read concern, promotion, maintenance
mode, and repair state are scoped to a replication group.

A Sirannon process may host more than one replication group.
There is no process-wide global primary.

### Stable Node Identity

Each data-bearing node must have a stable persisted identity tied
to its local database copy. A restarted process that uses the same
durable database copy must reuse the same node identity. A new
empty node must receive a new identity and sync from the current
primary before it can serve safe reads or become promotable.

Coordinator leases represent a running node session. They do not
replace stable node identity.

### Controller Election

Coordinator mode has one active controller per Sirannon cluster.
Controller-capable Sirannon processes compete for the controller
lease in the coordinator. The process that holds the lease makes
failover decisions. Standby controllers watch the lease and may
take over when the active controller loses authority.

Dedicated controller-only Sirannon processes are allowed, but the
default deployment model is that normal controller-capable nodes
can stand by for the controller role.

### Node Liveness

etcd lease expiry or explicit deregistration is the authoritative
node-death signal. A transport disconnect is connection state. It
may make a replica lag or leave the in-sync set, but it must not
by itself trigger primary promotion.

### Primary Term and Fencing

Each replication group has a monotonically increasing `primaryTerm`.
Every coordinator-mode write path and replication message must
carry the current term. Promotion atomically increments the term
and records the new primary.

Nodes must apply these fencing rules:

- A primary may accept writes only while it can prove current
  coordinator authority for the group and term.
- If authority renewal or verification fails, the primary must
  fail closed for new writes and fail in-flight writes that cannot
  prove current authority.
- Replicas must reject batches, sync messages, and forwarded
  writes from stale terms or non-current primaries.
- A former primary that observes a newer term must demote and
  rejoin through sync or repair.

### In-Sync Set

The in-sync set contains replicas proven current enough for safe
reads and safe promotion. A live node is not automatically in
sync.

If a replica falls behind, fails to acknowledge writes in time, or
reports a storage or sync error, the controller must remove it
from the in-sync set before acknowledging writes that exclude it.
The replica can return to the in-sync set only after catch-up
proves that it is current for the group's required durability
point.

### Promotion Rules

Automatic promotion may choose only a replica that meets all of
these conditions:

- its node session lease is alive;
- it is in the replication group's in-sync set;
- it has no known storage, checksum, sync, or repair error;
- the controller can atomically advance the group's primary term
  and record the replica as current primary.

If no replica meets these conditions, Sirannon must fail closed
with `NO_SAFE_PRIMARY`. It must not promote an arbitrary live
replica.

### Minimum Production Shape

Production-safe automatic write failover requires at least three
voting data-bearing Sirannon nodes in a replication group. One
node has no failover. Two nodes can replicate, but one survivor
cannot prove majority authority after the other node is lost.

### Returning Former Primary

A returning former primary must not silently merge local-only
divergent writes into the current primary. It must demote, compare
history, quarantine or report divergent local-only writes when
present, and rejoin through sync or repair from the current
primary.

Conflict resolvers may be used during the explicit repair step.
They are not a substitute for term fencing or safe promotion.

### Maintenance Mode

Coordinator mode must provide maintenance-mode primitives:

- drain a node;
- move primary duty away from a node before planned work;
- remove replicas from safe read and promotion sets when needed;
- wait for in-flight work to complete or fail cleanly;
- deregister the running session after the cluster adjusts;
- rejoin and catch up after restart.

Sirannon does not provision machines, containers, pods, or
volumes. Deployment infrastructure creates or restarts servers.
Sirannon detects node registration through the coordinator,
assigns replication work, validates catch-up, and marks nodes safe
only after they are current.

### Unsafe Recovery

When Sirannon cannot prove a safe primary, automatic recovery must
fail closed. Unsafe recovery requires explicit operator intent
through an auditable command or API, such as restoring a backup,
rejoining persisted data, resyncing a replica, or force-promoting
a named node with a data-loss acknowledgement.

### Rolling Upgrade Compatibility

Nodes must register package, spec, and protocol compatibility
metadata with the coordinator. The controller must reject
incompatible major protocol changes. Mixed minor or patch levels
are allowed only when metadata formats, wire formats, and safety
semantics remain compatible.

New durable metadata or new failover safety semantics require an
explicit cluster compatibility gate before activation.

### Coordinator-Mode Conformance Invariants

Coordinator mode does not conform to this specification unless its
test suite proves these invariants with a real coordinator and
real multi-node Sirannon groups:

- at most one writable primary per replication group and primary
  term;
- no loss of a write acknowledged with production `majority` write
  concern when only the failed primary is lost;
- stale primaries reject writes, replication batches, forwarded
  transactions, and sync messages;
- minority partitions fail closed for writes;
- only in-sync replicas are promoted;
- clients reroute writes to the new primary or receive a clear
  error;
- returning former primaries rejoin through sync or repair;
- health state reports write availability, read availability,
  coordinator status, current primary, primary term, and repair
  state accurately.

---

## Conflict Resolution

Conflict resolvers determine the outcome when two versions of the
same row exist on different nodes. This can occur during explicit
repair or disaster recovery after a former primary returns with
local-only writes.

Conflict resolvers must not be used to make normal automatic
failover safe. Coordinator-mode failover prevents two writable
primaries through leases, primary terms, in-sync sets, and
fail-closed promotion.

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
  groupId?:      string
  primaryTerm?:  bigint
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
  batchId:      string
  groupId?:     string
  primaryTerm?: bigint
  ackedSeq:     bigint
  nodeId:       string
}
```

`groupId` and `primaryTerm` are required in coordinator mode and
absent or ignored in static mode. A receiver in coordinator mode
must reject a batch or acknowledgement whose term does not match
the receiver's current view of the replication group.

---

## Replication Engine

The engine orchestrates change batching, transmission, reception,
and application.

### Configuration

```text
ReplicationConfig {
  nodeId?:                  string     (auto-generated if omitted)
  stableNodeId?:            string
  replicationGroupId?:      string
  topology:                 Topology
  coordinator?:             CoordinatorConfig
  controller?:              ControllerConfig
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

CoordinatorConfig {
  mode:             'etcd' | 'memory'
  endpoints:        List<string>
  keyPrefix:        string
  tls?:             CoordinatorTlsConfig
  auth?:            CoordinatorAuthConfig
  leaseTtlMs?:      number
  electionTimeoutMs?: number
}

ControllerConfig {
  candidate?: boolean     (default: true)
}

CoordinatorTlsConfig {
  caCertPath?:     string
  certPath?:       string
  keyPath?:        string
  insecure?:       boolean     (development and test only)
}

CoordinatorAuthConfig {
  username?:       string
  password?:       string
  clientIdentity?: string
}
```

`coordinator.mode = 'memory'` is not a production coordinator. It
is allowed only for tests and local development.

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
4. Send an acknowledgement back to the sender.

### Write Concern

After executing a write, the engine can wait for replication
acknowledgements:

- `local`: Return immediately after local commit.
- `majority`: Wait for `floor(connectedPeers / 2) + 1` peers to
  acknowledge. Reject with `WRITE_CONCERN_ERROR` on timeout.
- `all`: Wait for all connected peers to acknowledge. Reject with
  `WRITE_CONCERN_ERROR` on timeout or if any peer disconnects.

In coordinator mode, `majority` is calculated from configured
voting data-bearing nodes in the replication group, including the
primary's local durable commit. It is not calculated from currently
connected peers. A successful coordinator-mode `majority` write
means the write survives automatic primary failover when only the
failed primary is lost and an eligible in-sync replica remains.

In coordinator mode, `all` means all configured voting data-bearing
nodes that are not explicitly drained from the group.

`local` writes in coordinator mode are explicit opt-in operations.
They are not failover-safe and must not be the default for
production high-availability configurations.

### Read Concern

Read concern controls which durability point a read may observe.

- `local`: Read the selected node's local state. In coordinator
  mode, this mode can observe data that may later be quarantined
  during repair and must be selected explicitly.
- `majority`: Read data that has reached the replication group's
  majority commit point.
- `linearizable`: Read from the current primary after it proves
  live authority for the current primary term.

If the requested read concern cannot be satisfied, the read must
fail rather than silently returning a weaker result.

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

In coordinator mode, forwarding must target the current primary
recorded for the replication group. The forwarded request must
carry `groupId` and `primaryTerm`. A receiver must reject a
forwarded request with `STALE_PRIMARY` when it no longer holds the
current primary term.

### ForwardedTransaction (Normative Wire Format)

```text
ForwardedTransaction {
  statements:  List<{ sql: string, params?: Params }>
  requestId:   string
  groupId?:    string
  primaryTerm?: bigint
}

ForwardedTransactionResult {
  results:    List<{ changes: number, lastInsertRowId: number or string }>
  requestId:  string
  groupId?:   string
  primaryTerm?: bigint
}
```

---

## Peer Tracking

The peer tracker maintains state for each connected peer.

```text
PeerState {
  nodeId:          string
  groupId?:        string
  lastAckedSeq:    bigint
  lastSentSeq:     bigint
  lastReceivedHlc: string
  connected:       boolean
  inSync?:         boolean
  draining?:       boolean
  lastPrimaryTerm?: bigint
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
  groupId?:        string
  primaryTerm?:    bigint
  completedTables: List<string>
}

SyncBatch {
  requestId:          string
  groupId?:           string
  primaryTerm?:       bigint
  table:              string
  batchIndex:         number
  rows:               List<Map<string, any>>
  schema?:            List<string>
  checksum:           string
  isLastBatchForTable: boolean
}

SyncComplete {
  requestId:   string
  groupId?:    string
  primaryTerm?: bigint
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
  groupId?:    string
  primaryTerm?: bigint
  table:        string
  batchIndex:   number
  success:      boolean
  error?:       string
}
```

`groupId` and `primaryTerm` are required for sync messages in
coordinator mode. A replica must restart sync when the primary
term changes before the sync completes.

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
