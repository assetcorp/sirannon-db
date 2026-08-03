import type { ChangeTracker } from '../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import type { ConflictResolver, ReplicationBatch, SyncTableManifest } from '../core/sync/types.js'
import type { NodeHealth } from '../core/types.js'
import type {
  ClusterCoordinator,
  CoordinatorCompatibilityMetadata,
  ReplicationGroupState,
} from './coordinator/types.js'

export type {
  ApplyResult,
  ConflictContext,
  ConflictResolution,
  ConflictResolver,
  HLCTimestamp,
  ReplicationBatch,
  ReplicationChange,
  SyncTableManifest,
} from '../core/sync/types.js'
export type { NodeHealth, NodeHealthReason, NodeHealthState } from '../core/types.js'

/**
 * What one node knows about a peer it is connected to.
 *
 * @public
 */
export interface NodeInfo {
  /** Identifier of the peer. */
  id: string
  /** Replication group the peer belongs to. */
  groupId?: string
  /** Whether the peer accepts writes or serves reads. */
  role: 'primary' | 'replica'
  /** Primary term the peer holds. */
  primaryTerm?: bigint
  /** Replication protocol version the peer speaks. */
  protocolVersion?: string
  /** Milliseconds since the Unix epoch, taken when the peer connected. */
  joinedAt: number
  /** Milliseconds since the Unix epoch, taken at the last message from the peer. */
  lastSeenAt: number
  /** Highest change-log position the peer has acknowledged. */
  lastAckedSeq: bigint
  /** Anything else the transport attached about the peer. */
  metadata?: Record<string, unknown>
}

/**
 * Confirms that a node applied one batch.
 *
 * @public
 */
export interface ReplicationAck {
  /** Identifier of the batch being acknowledged. */
  batchId: string
  /** Highest change-log position the sender has now applied. */
  ackedSeq: bigint
  /** Identifier of the node sending the acknowledgement. */
  nodeId: string
  /** Replication group the sender belongs to. */
  groupId?: string
  /** Primary term the sender believes is current. */
  primaryTerm?: bigint
}

/**
 * A write a replica sends to the primary, because it accepts no writes itself.
 *
 * @public
 */
export interface ForwardedTransaction {
  /** The statements to run, in order, each with its own parameters. */
  statements: Array<{ sql: string; params?: Record<string, unknown> | unknown[] }>
  /** Identifier the result echoes, so the replica matches it to the request. */
  requestId: string
  /** Replication group the forwarding replica belongs to. */
  groupId?: string
  /** Primary term the replica believes is current. */
  primaryTerm?: bigint
}

/**
 * What the primary reports back for a write a replica forwarded to it.
 *
 * @public
 */
export interface ForwardedTransactionResult {
  /** One result per statement, in the order the primary ran them. */
  results: Array<{ changes: number; lastInsertRowId: number | string }>
  /** Identifier the request carried. */
  requestId: string
  /** Replication group the primary belongs to. */
  groupId?: string
  /** Primary term the primary held when it ran the write. */
  primaryTerm?: bigint
}

/**
 * Whether a node accepts writes or serves reads.
 *
 * @public
 */
export type TopologyRole = 'primary' | 'replica'

/**
 * Decides which nodes a change flows between, and whether this node writes.
 *
 * @public
 */
export interface Topology {
  /** Whether this node accepts writes or serves reads. */
  role: TopologyRole
  /** Reports whether this node accepts writes right now. */
  canWrite(): boolean
  /** Reports whether this node sends its changes to a given peer. */
  shouldReplicateTo(peerId: string, peerRole: TopologyRole): boolean
  /** Reports whether this node applies changes arriving from a given peer. */
  shouldAcceptFrom(peerId: string, peerRole: TopologyRole): boolean
  /** Reports whether incoming changes can meet a local row and need a resolver. */
  requiresConflictResolution(): boolean
}

/**
 * What a transport announces about this node when it connects, and where it dials.
 *
 * @public
 */
export interface TransportConfig {
  /** Addresses of the peers this node dials. */
  endpoints?: string[]
  /** Role this node announces. A replication engine fills it in on start. */
  localRole?: TopologyRole
  /** Replication group this node announces. */
  groupId?: string
  /** Primary term this node announces. */
  primaryTerm?: bigint
  /** Replication protocol version this node announces. */
  protocolVersion?: string
  /** Anything else the transport should announce about this node. */
  metadata?: Record<string, unknown>
}

/**
 * Carries change batches, acknowledgements, forwarded writes, and first sync
 * between nodes. Build your own to replicate over something the package does
 * not already speak.
 *
 * @public
 */
export interface ReplicationTransport {
  /** Connects to the configured peers and announces this node. */
  connect(localNodeId: string, config: TransportConfig): Promise<void>
  /** Closes every peer connection. */
  disconnect(): Promise<void>
  /** Sends one batch of changes to one peer. */
  send(peerId: string, batch: ReplicationBatch): Promise<void>
  /** Sends one batch of changes to every connected peer. */
  broadcast(batch: ReplicationBatch): Promise<void>
  /** Confirms to a peer that this node applied one of its batches. */
  sendAck(peerId: string, ack: ReplicationAck): Promise<void>
  /** Sends a write to the primary and waits for its result. */
  forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult>
  /** Asks a peer to stream a full copy of the database. */
  requestSync(peerId: string, request: SyncRequest): Promise<void>
  /** Sends one page of first-sync table data. */
  sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void>
  /** Tells a joining node that first sync has finished, and sends the manifests to verify it. */
  sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void>
  /** Confirms to the source that a joining node stored one first-sync page. */
  sendSyncAck(peerId: string, ack: SyncAck): Promise<void>
  /** Registers the handler that applies incoming change batches. */
  onBatchReceived(handler: (batch: ReplicationBatch, fromPeerId: string) => Promise<void>): void
  /** Registers the handler that records incoming acknowledgements. */
  onAckReceived(handler: (ack: ReplicationAck, fromPeerId: string) => void): void
  /** Registers the handler that runs a write a replica forwarded. */
  onForwardReceived(
    handler: (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>,
  ): void
  /** Registers the handler that serves a first-sync request. */
  onSyncRequested(handler: (request: SyncRequest, fromPeerId: string) => Promise<void>): void
  /** Registers the handler that stores an incoming first-sync page. */
  onSyncBatchReceived(handler: (batch: SyncBatch, fromPeerId: string) => Promise<void>): void
  /** Registers the handler that finishes first sync and verifies the manifests. */
  onSyncCompleteReceived(handler: (complete: SyncComplete, fromPeerId: string) => Promise<void>): void
  /** Registers the handler that records first-sync page acknowledgements. */
  onSyncAckReceived(handler: (ack: SyncAck, fromPeerId: string) => void): void
  /** Registers the handler that runs when a peer connects. */
  onPeerConnected(handler: (peer: NodeInfo) => void): void
  /** Registers the handler that runs when a peer disconnects. */
  onPeerDisconnected(handler: (peerId: string) => void): void
  /** Returns every connected peer, keyed by identifier. */
  peers(): ReadonlyMap<string, NodeInfo>
}

/**
 * One batch a node has sent and is still waiting to see acknowledged.
 *
 * @public
 */
export interface InFlightBatch {
  /** Identifier of the batch. */
  batchId: string
  /** Change-log position of its first change. */
  fromSeq: bigint
  /** Change-log position of its last change. */
  toSeq: bigint
  /** Milliseconds since the Unix epoch, taken when the batch was sent. */
  sentAt: number
}

/**
 * Where one peer stands from this node's point of view.
 *
 * @public
 */
export interface PeerState {
  /** Identifier of the peer. */
  nodeId: string
  /** Highest change-log position the peer has acknowledged. */
  lastAckedSeq: bigint
  /** Highest change-log position this node has sent it. */
  lastSentSeq: bigint
  /** Most recent hybrid logical clock stamp received from the peer. */
  lastReceivedHlc: string
  /** Whether the transport holds an open connection to the peer. */
  connected: boolean
  /** Batches waiting to be sent to the peer. */
  pendingBatches: number
  /** Batches sent to the peer and not yet acknowledged. */
  inFlightBatches: InFlightBatch[]
}

/**
 * Whether this node runs the group's controller loop, and how it holds the lease that grants it.
 *
 * @public
 */
export interface CoordinatorControllerConfig {
  /** Whether this node stands for the controller lease. */
  enabled?: boolean
  /** Identifier this node claims the lease under. Defaults to the node identifier. */
  holderId?: string
  /** Milliseconds the lease lasts before it must be renewed. */
  leaseTtlMs?: number
  /** Milliseconds between controller passes. */
  tickIntervalMs?: number
}

/**
 * Coordinator-backed failover: where group state is stored, which nodes vote, and how this node registers.
 *
 * @public
 */
export interface CoordinatorModeConfig {
  /** Identifier of the cluster this group belongs to. */
  clusterId: string
  /** Identifier of the replication group. */
  groupId: string
  /** Address clients reach this node at, which the group publishes to readers. */
  endpoint?: string
  /** Identifiers of the nodes that count towards majority. Automatic failover needs at least three. */
  votingDataBearingNodeIds?: string[]
  /** Where primary authority, node sessions, group state, and the in-sync set are stored. */
  coordinator: ClusterCoordinator
  /** Milliseconds this node's session lasts before it must be renewed. */
  sessionTtlMs?: number
  /** Whether this node runs the group's controller loop. */
  controller?: boolean | CoordinatorControllerConfig
  /** Package and specification versions this node publishes, so the group refuses an incompatible peer. */
  compatibility?: CoordinatorCompatibilityMetadata
}

/**
 * How one node replicates: its identity, topology, transport, batching, first
 * sync, and coordinator-backed failover.
 *
 * @public
 */
export interface ReplicationConfig {
  /** Identifier of this node. Coordinator mode requires a stable, persisted value. */
  nodeId?: string
  /** Decides which nodes a change flows between, and whether this node writes. */
  topology: Topology
  /** Carries batches, acknowledgements, forwarded writes, and first sync between nodes. */
  transport: ReplicationTransport
  /** What the transport announces about this node, and where it dials. */
  transportConfig?: TransportConfig
  /** Sends a write this node cannot serve to the primary instead of refusing it. */
  writeForwarding?: boolean
  /** Resolvers to use for named tables, which override the default. */
  conflictResolvers?: Record<string, ConflictResolver>
  /** Resolver for every table without one of its own. Default: last-writer-wins. */
  defaultConflictResolver?: ConflictResolver
  /** Changes sent in one batch. */
  batchSize?: number
  /** Milliseconds between batches. */
  batchIntervalMs?: number
  /** Batches allowed in flight to one peer before this node stops sending. */
  maxPendingBatches?: number
  /** Change-log distance beyond which a joining node takes a full copy instead of catching up. */
  snapshotThreshold?: number
  /** Clock difference, in milliseconds, beyond which this node reports a peer's stamps as suspect. */
  maxClockDriftMs?: number
  /** Changes allowed in one batch, whatever the byte size. */
  maxBatchChanges?: number
  /** Milliseconds this node waits for a batch acknowledgement before it retries. */
  ackTimeoutMs?: number
  /** Runs on the primary before it executes a write a replica forwarded. Throw to refuse it. */
  onBeforeForwardedQuery?: (sql: string, params?: unknown[] | Record<string, unknown>) => void
  /** How far a peer may fall behind before this node reports it. */
  flowControl?: {
    maxLagSeconds?: number
    onLagExceeded?: (peerId: string, lagMs: number) => void
  }
  /** Pulls a full copy of the database before serving reads. Default: true. */
  initialSync?: boolean
  /** Rows sent per first-sync page. */
  syncBatchSize?: number
  /** First syncs this node serves at once. */
  maxConcurrentSyncs?: number
  /** Milliseconds a first sync may run before this node abandons it. */
  maxSyncDurationMs?: number
  /** Change-log distance a joining node may still be behind by and be declared ready. */
  maxSyncLagBeforeReady?: number
  /** Milliseconds the source waits for a first-sync page acknowledgement. */
  syncAckTimeoutMs?: number
  /** Milliseconds a joining node may spend catching up before it gives up. */
  catchUpDeadlineMs?: number
  /** Change-log position to start from, for a node seeded by copying the database file. */
  resumeFromSeq?: bigint
  /** Opens the read-only connection this node streams first-sync data from, so serving a joiner never blocks writes. */
  snapshotConnectionFactory?: () => Promise<SQLiteConnection>
  /** Records the local changes this node replicates. */
  changeTracker?: ChangeTracker
  /** Turns on coordinator-backed failover and names where group state is stored. */
  coordinator?: CoordinatorModeConfig
}

/**
 * Where one node stands: its role, its peers, its progress, and its health.
 *
 * @public
 */
export interface ReplicationStatus {
  /** Identifier of this node. */
  nodeId: string
  /** Whether this node accepts writes or serves reads. */
  role: TopologyRole
  /** Where each peer stands from this node's point of view. */
  peers: PeerState[]
  /** Highest change-log position this node has sent. */
  localSeq: bigint
  /** Whether the engine is running. */
  replicating: boolean
  /** What this node can do right now, and the condition behind it. */
  health: NodeHealth
  /** Where this node stands in first sync. */
  syncState?: SyncState
  /** Group state as this node last read it from the coordinator. */
  coordinator?: CoordinatorRuntimeStatus
}

/**
 * Group state as one node last read it from the cluster coordinator.
 *
 * @public
 */
export interface CoordinatorRuntimeStatus {
  /** Identifier of the cluster. */
  clusterId: string
  /** Identifier of the replication group. */
  groupId: string
  /** The primary the group currently names. */
  currentPrimary: ReplicationGroupState['currentPrimary']
  /** Term that primary holds. */
  primaryTerm: bigint
  /** Nodes the group counts as in sync. */
  inSyncNodeIds: string[]
  /** Nodes being taken out of service. */
  drainingNodeIds: string[]
  /** Nodes being rebuilt. */
  repairingNodeIds: string[]
  /** Nodes the group has quarantined. */
  faultedNodeIds: string[]
  /** Nodes that count towards majority. */
  votingDataBearingNodeIds: string[]
  /** Whether this node holds write authority for the current term. */
  authority: boolean
  /** Whether this node reaches the coordinator. */
  connected: boolean
  /** Whether this node runs the group's controller loop. */
  controllerState: 'disabled' | 'standby' | 'active' | 'lost'
}

/**
 * How far a joining node has got through first sync.
 *
 * @public
 */
export type SyncPhase = 'pending' | 'syncing' | 'catching-up' | 'ready'

/**
 * Where a joining node stands in first sync.
 *
 * @public
 */
export interface SyncState {
  /** How far the node has got. */
  phase: SyncPhase
  /** Peer streaming the copy, or null when no sync is running. */
  sourcePeerId: string | null
  /** Change-log position the copy was taken at. */
  snapshotSeq: bigint | null
  /** Tables already copied in full. */
  completedTables: string[]
  /** Tables the copy covers. */
  totalTables: number
  /** Milliseconds since the Unix epoch, taken when the sync started. */
  startedAt: number | null
  /** Why the sync failed, or null while it is going well. */
  error: string | null
}

/**
 * Asks a peer to stream a full copy of the database.
 *
 * @public
 */
export interface SyncRequest {
  /** Identifier every message of this sync carries. */
  requestId: string
  /** Identifier of the node asking for the copy. */
  joinerNodeId: string
  /** Tables the joiner already holds in full, so a resumed sync skips them. */
  completedTables: string[]
  /** Whether the joiner verifies the stream with chained batch digests. */
  supportsStreamVerification?: boolean
  /** Replication group the joiner belongs to. */
  groupId?: string
  /** Primary term the joiner believes is current. */
  primaryTerm?: bigint
}

/**
 * One page of first-sync table data.
 *
 * @public
 */
export interface SyncBatch {
  /** Identifier of the sync this page belongs to. */
  requestId: string
  /** Table these rows come from. */
  table: string
  /** Position of this page in the table's stream, counting from zero. */
  batchIndex: number
  /** The rows themselves. */
  rows: Record<string, unknown>[]
  /** Schema statements, sent with the first page so the joiner can build the tables. */
  schema?: string[]
  /** Checksum of the rows, which the joiner verifies before it writes them. */
  checksum: string
  /** Set on the last page of a table. */
  isLastBatchForTable: boolean
  /** Tables the whole copy covers. */
  totalTables?: number
  /** Replication group the source belongs to. */
  groupId?: string
  /** Primary term the source held. */
  primaryTerm?: bigint
}

/**
 * Tells a joining node that first sync has finished, and carries what it needs to verify the copy.
 *
 * @public
 */
export interface SyncComplete {
  /** Identifier of the sync that finished. */
  requestId: string
  /** Change-log position the copy was taken at, which the joiner resumes from. */
  snapshotSeq: bigint
  /** One manifest per table, so the joiner can check what it received. */
  manifests: SyncTableManifest[]
  /** Replication group the source belongs to. */
  groupId?: string
  /** Primary term the source held. */
  primaryTerm?: bigint
}

/**
 * Confirms to the source that a joining node stored one first-sync page, or says why it could not.
 *
 * @public
 */
export interface SyncAck {
  /** Identifier of the sync this acknowledgement belongs to. */
  requestId: string
  /** Identifier of the node that received the page. */
  joinerNodeId: string
  /** Table the page came from. */
  table: string
  /** Position of the page in that table's stream. */
  batchIndex: number
  /** Whether the joiner stored the page. */
  success: boolean
  /** Why the joiner could not store it. */
  error?: string
  /** Replication group the joiner belongs to. */
  groupId?: string
  /** Primary term the joiner believes is current. */
  primaryTerm?: bigint
}

/**
 * A replication failure, delivered to listeners of the engine's `replication-error` event.
 *
 * @public
 */
export interface ReplicationErrorEvent {
  /** The failure itself. */
  error: Error
  /** What the engine was doing when it failed. */
  operation: string
  /** Peer the operation involved, when it involved one. */
  peerId?: string
  /** Whether the engine carries on after this failure. */
  recoverable: boolean
}
