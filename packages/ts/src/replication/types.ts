import type { ChangeTracker } from '../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import type { ConflictResolver, ReplicationBatch } from '../core/sync/types.js'
import type { NodeHealth } from '../core/types.js'
import type {
  ClusterCoordinator,
  CoordinatorCompatibilityMetadata,
  ReplicationGroupState,
} from './coordinator/types.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  PeerState,
  ReplicationAck,
} from './peer-types.js'
import type { SyncAck, SyncBatch, SyncComplete, SyncRequest, SyncState } from './sync-types.js'

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
export type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  InFlightBatch,
  NodeInfo,
  PeerState,
  ReplicationAck,
} from './peer-types.js'
export type { SyncAck, SyncBatch, SyncComplete, SyncPhase, SyncRequest, SyncState } from './sync-types.js'

/**
 * Names a node's role: 'primary' accepts writes, and 'replica' serves reads.
 *
 * @public
 */
export type TopologyRole = 'primary' | 'replica'

/**
 * Sets the rules for which peers exchange changes with this node, and whether this node writes.
 *
 * @public
 */
export interface Topology {
  /** Records whether this node is the primary or a replica. */
  role: TopologyRole
  /** Reports whether this node accepts writes. */
  canWrite(): boolean
  /** Reports whether this node sends its changes to a given peer. */
  shouldReplicateTo(peerId: string, peerRole: TopologyRole): boolean
  /** Reports whether this node applies the changes that a given peer sends. */
  shouldAcceptFrom(peerId: string, peerRole: TopologyRole): boolean
  /** Reports whether an incoming change can conflict with a local write, so that the engine has to resolve it. */
  requiresConflictResolution(): boolean
}

/**
 * Sets what the transport announces about this node when it connects, and the endpoints that it dials.
 *
 * @public
 */
export interface TransportConfig {
  /** Lists the addresses of the peers that this node dials. */
  endpoints?: string[]
  /** Sets the role that this node announces. The replication engine fills it in when it starts. */
  localRole?: TopologyRole
  /** Sets the replication group that this node announces. */
  groupId?: string
  /** Sets the primary term that this node announces. */
  primaryTerm?: bigint
  /** Sets the replication protocol version that this node announces. */
  protocolVersion?: string
  /** Holds any other data for the transport to announce about this node. The bundled transports ignore it. */
  metadata?: Record<string, unknown>
}

/**
 * Sends change batches, acknowledgements, forwarded writes, and first-sync
 * messages between nodes. Implement this interface to replicate over a
 * protocol other than the bundled gRPC and in-memory transports.
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
  /** Tells a joining node that first sync is complete, and sends the manifests that the node checks its copy against. */
  sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void>
  /** Tells the source whether this joining node stored one first-sync page. */
  sendSyncAck(peerId: string, ack: SyncAck): Promise<void>
  /** Registers the handler that applies incoming change batches. */
  onBatchReceived(handler: (batch: ReplicationBatch, fromPeerId: string) => Promise<void>): void
  /** Registers the handler that records incoming acknowledgements. */
  onAckReceived(handler: (ack: ReplicationAck, fromPeerId: string) => void): void
  /** Registers the handler that executes a write that a replica forwards. */
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
  /** Registers the handler that the transport calls when a peer connects. */
  onPeerConnected(handler: (peer: NodeInfo) => void): void
  /** Registers the handler that the transport calls when a peer disconnects. */
  onPeerDisconnected(handler: (peerId: string) => void): void
  /** Returns every connected peer, keyed by node ID. */
  peers(): ReadonlyMap<string, NodeInfo>
}

/**
 * Configures this node's part in the group's controller loop and the lease that makes a node the controller.
 *
 * @public
 */
export interface CoordinatorControllerConfig {
  /** Is true when this node competes for the controller lease, which it does by default. */
  enabled?: boolean
  /** Sets the ID that this node holds the lease under. Defaults to the node ID. */
  holderId?: string
  /** Sets how many milliseconds the lease lasts before the holder has to renew it. */
  leaseTtlMs?: number
  /** Sets how many milliseconds pass between controller loop ticks. */
  tickIntervalMs?: number
}

/**
 * Configures coordinator-backed failover: the coordinator that stores group state, the nodes that vote, and how this
 * node registers.
 *
 * @public
 */
export interface CoordinatorModeConfig {
  /** Identifies the cluster that contains this group. */
  clusterId: string
  /** Identifies the replication group. */
  groupId: string
  /** Sets the address that clients use to reach this node, which the coordinator stores with the node's session. */
  endpoint?: string
  /** Lists the nodes that count towards the majority, which the engine uses to create the group when the coordinator has no state for it. Automatic failover requires at least three. */
  votingDataBearingNodeIds?: string[]
  /** Holds the coordinator that stores primary authority, node sessions, group state, and the in-sync set. */
  coordinator: ClusterCoordinator
  /** Sets how many milliseconds this node's session lasts before the node has to renew it. */
  sessionTtlMs?: number
  /** Turns this node's controller loop on or off, or configures it. The loop is on by default. */
  controller?: boolean | CoordinatorControllerConfig
  /** Sets the versions that this node publishes, which the engine also records as the group's requirement when it creates the group. */
  compatibility?: CoordinatorCompatibilityMetadata
}

/**
 * Configures how one node replicates: its identity, topology, transport,
 * batching, first sync, and coordinator-backed failover.
 *
 * @public
 */
export interface ReplicationConfig {
  /** Identifies this node, and defaults to a random ID. Coordinator mode requires one, so keep it the same across restarts. */
  nodeId?: string
  /** Sets which peers exchange changes with this node, and whether this node writes. */
  topology: Topology
  /** Sends batches, acknowledgements, forwarded writes, and first-sync messages between nodes. */
  transport: ReplicationTransport
  /** Sets what the transport announces about this node, and the endpoints that it dials. */
  transportConfig?: TransportConfig
  /** Makes a node that cannot accept writes forward `execute` and `executeBatch` calls to the primary. */
  writeForwarding?: boolean
  /** Maps table names to the resolvers that replace the default for those tables. */
  conflictResolvers?: Record<string, ConflictResolver>
  /** Sets the resolver for every table that has no resolver of its own. Defaults to last-writer-wins. */
  defaultConflictResolver?: ConflictResolver
  /** Sets the most changes that one outgoing batch holds. */
  batchSize?: number
  /** Sets how many milliseconds the sender loop waits between passes. */
  batchIntervalMs?: number
  /** Sets how many unacknowledged batches one peer can have before this node pauses sending to it. */
  maxPendingBatches?: number
  /** Reserved, and the engine ignores it. */
  snapshotThreshold?: number
  /** Sets the largest difference, in milliseconds, between this node's clock and the newest timestamp in an incoming batch. This node rejects a batch that exceeds it. */
  maxClockDriftMs?: number
  /** Sets the most changes that this node accepts in one incoming batch. This node rejects a larger batch. */
  maxBatchChanges?: number
  /** Sets how many milliseconds this node waits for a batch acknowledgement before it resends the batch. */
  ackTimeoutMs?: number
  /** Receives each forwarded statement on the primary before the primary executes it. Throw to refuse the whole forwarded write. */
  onBeforeForwardedQuery?: (sql: string, params?: unknown[] | Record<string, unknown>) => void
  /** Sets how far a peer can fall behind before this node calls `onLagExceeded`. */
  flowControl?: {
    maxLagSeconds?: number
    onLagExceeded?: (peerId: string, lagMs: number) => void
  }
  /** Makes a replica copy the full database from a source peer before it serves reads. Defaults to true. */
  initialSync?: boolean
  /** Sets how many rows the source sends in each first-sync page. */
  syncBatchSize?: number
  /** Sets how many first syncs this node serves at once. */
  maxConcurrentSyncs?: number
  /** Sets how many milliseconds this node waits for the joiner to acknowledge each first-sync page before it aborts the sync. */
  maxSyncDurationMs?: number
  /** Sets how many change-log positions a joining node can trail its source by when the node marks itself ready. */
  maxSyncLagBeforeReady?: number
  /** Sets how many milliseconds the source waits for the acknowledgement of one first-sync page. */
  syncAckTimeoutMs?: number
  /** Sets how many milliseconds a joining node spends catching up before it marks itself ready anyway. */
  catchUpDeadlineMs?: number
  /** Sets the change-log position to start from, for a node that you seed by copying the database file. The engine uses it only when `initialSync` is false. */
  resumeFromSeq?: bigint
  /** Opens the read-only connection that this node streams first-sync data from, so that writes continue while the node serves a joiner. */
  snapshotConnectionFactory?: () => Promise<SQLiteConnection>
  /** Records the local changes that this node replicates. */
  changeTracker?: ChangeTracker
  /** Turns on coordinator-backed failover and names the coordinator that stores the group state. */
  coordinator?: CoordinatorModeConfig
}

/**
 * Reports one node's role, peers, progress, and health.
 *
 * @public
 */
export interface ReplicationStatus {
  /** Identifies this node. */
  nodeId: string
  /** Records whether this node is the primary or a replica. */
  role: TopologyRole
  /** Describes each peer's progress, as this node tracks it. */
  peers: PeerState[]
  /** Holds this node's local change-log position as of the sender loop's last pass. */
  localSeq: bigint
  /** Is true between the engine's `start` and `stop` calls. */
  replicating: boolean
  /** Reports what this node can do at the moment, and the reason. */
  health: NodeHealth
  /** Describes this node's progress through first sync. */
  syncState?: SyncState
  /** Holds the group state that this node last read from the coordinator. */
  coordinator?: CoordinatorRuntimeStatus
}

/**
 * Holds the group state that one node last read from the cluster coordinator.
 *
 * @public
 */
export interface CoordinatorRuntimeStatus {
  /** Identifies the cluster. */
  clusterId: string
  /** Identifies the replication group. */
  groupId: string
  /** Identifies the group's current primary. */
  currentPrimary: ReplicationGroupState['currentPrimary']
  /** Holds the current primary's term. */
  primaryTerm: bigint
  /** Lists the nodes that are in sync with the primary. */
  inSyncNodeIds: string[]
  /** Lists the draining nodes, which are leaving service. */
  drainingNodeIds: string[]
  /** Lists the repairing nodes, which are rebuilding their copy of the data. */
  repairingNodeIds: string[]
  /** Lists the faulted nodes, which stay out of service until an `updateNodeMaintenance` call clears the flag. */
  faultedNodeIds: string[]
  /** Lists the nodes that count towards the majority. */
  votingDataBearingNodeIds: string[]
  /** Lists the nodes that hold a live session with the coordinator. The field is present only while this node is connected to the coordinator and the coordinator supports session watches. */
  liveNodeIds?: string[]
  /** Is true when the group state names this node as the current primary. */
  authority: boolean
  /** Is true when this node heard from the coordinator within the last session TTL. */
  connected: boolean
  /** Names the state of this node's controller loop: 'disabled', 'standby' while it waits for the lease, 'active' while it holds the lease, or 'lost' after a failed renewal or a controller failure. */
  controllerState: 'disabled' | 'standby' | 'active' | 'lost'
}

/**
 * Describes a replication failure, which the engine emits as its `replication-error` event.
 *
 * @public
 */
export interface ReplicationErrorEvent {
  /** Holds the error. */
  error: Error
  /** Names the operation that failed. */
  operation: string
  /** Identifies the peer that the operation involved, if any. */
  peerId?: string
  /** Is true when the engine keeps working after this failure. */
  recoverable: boolean
}
