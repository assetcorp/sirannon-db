import type { ChangeTracker } from '../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import type {
  ClusterCoordinator,
  CoordinatorCompatibilityMetadata,
  ReplicationGroupState,
} from './coordinator/types.js'

export interface NodeInfo {
  id: string
  groupId?: string
  role: 'primary' | 'replica'
  primaryTerm?: bigint
  protocolVersion?: string
  joinedAt: number
  lastSeenAt: number
  lastAckedSeq: bigint
  metadata?: Record<string, unknown>
}

export interface HLCTimestamp {
  wallMs: number
  logical: number
  nodeId: string
}

export interface ReplicationChange {
  table: string
  operation: 'insert' | 'update' | 'delete' | 'ddl'
  rowId: string
  primaryKey: Record<string, unknown>
  hlc: string
  txId: string
  nodeId: string
  newData: Record<string, unknown> | null
  oldData: Record<string, unknown> | null
  ddlStatement?: string
}

export interface ReplicationBatch {
  sourceNodeId: string
  batchId: string
  fromSeq: bigint
  toSeq: bigint
  hlcRange: { min: string; max: string }
  changes: ReplicationChange[]
  checksum: string
  groupId?: string
  primaryTerm?: bigint
}

export interface ReplicationAck {
  batchId: string
  ackedSeq: bigint
  nodeId: string
  groupId?: string
  primaryTerm?: bigint
}

export interface ForwardedTransaction {
  statements: Array<{ sql: string; params?: Record<string, unknown> | unknown[] }>
  requestId: string
  groupId?: string
  primaryTerm?: bigint
}

export interface ForwardedTransactionResult {
  results: Array<{ changes: number; lastInsertRowId: number | string }>
  requestId: string
  groupId?: string
  primaryTerm?: bigint
}

export interface ConflictContext {
  table: string
  rowId: string
  localChange: ReplicationChange | null
  remoteChange: ReplicationChange
  localHlc: string | null
  remoteHlc: string
}

export interface ConflictResolution {
  action: 'accept_remote' | 'keep_local' | 'merge'
  mergedData?: Record<string, unknown>
}

export interface ConflictResolver {
  resolve(ctx: ConflictContext): ConflictResolution | Promise<ConflictResolution>
}

export type TopologyRole = 'primary' | 'replica'

export interface Topology {
  role: TopologyRole
  canWrite(): boolean
  shouldReplicateTo(peerId: string, peerRole: TopologyRole): boolean
  shouldAcceptFrom(peerId: string, peerRole: TopologyRole): boolean
  requiresConflictResolution(): boolean
}

export interface TransportConfig {
  endpoints?: string[]
  localRole?: TopologyRole
  groupId?: string
  primaryTerm?: bigint
  protocolVersion?: string
  metadata?: Record<string, unknown>
}

export interface ReplicationTransport {
  connect(localNodeId: string, config: TransportConfig): Promise<void>
  disconnect(): Promise<void>
  send(peerId: string, batch: ReplicationBatch): Promise<void>
  broadcast(batch: ReplicationBatch): Promise<void>
  sendAck(peerId: string, ack: ReplicationAck): Promise<void>
  forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult>
  requestSync(peerId: string, request: SyncRequest): Promise<void>
  sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void>
  sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void>
  sendSyncAck(peerId: string, ack: SyncAck): Promise<void>
  onBatchReceived(handler: (batch: ReplicationBatch, fromPeerId: string) => Promise<void>): void
  onAckReceived(handler: (ack: ReplicationAck, fromPeerId: string) => void): void
  onForwardReceived(
    handler: (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>,
  ): void
  onSyncRequested(handler: (request: SyncRequest, fromPeerId: string) => Promise<void>): void
  onSyncBatchReceived(handler: (batch: SyncBatch, fromPeerId: string) => Promise<void>): void
  onSyncCompleteReceived(handler: (complete: SyncComplete, fromPeerId: string) => Promise<void>): void
  onSyncAckReceived(handler: (ack: SyncAck, fromPeerId: string) => void): void
  onPeerConnected(handler: (peer: NodeInfo) => void): void
  onPeerDisconnected(handler: (peerId: string) => void): void
  peers(): ReadonlyMap<string, NodeInfo>
}

export interface InFlightBatch {
  batchId: string
  fromSeq: bigint
  toSeq: bigint
  sentAt: number
}

export interface PeerState {
  nodeId: string
  lastAckedSeq: bigint
  lastSentSeq: bigint
  lastReceivedHlc: string
  connected: boolean
  pendingBatches: number
  inFlightBatches: InFlightBatch[]
}

export interface CoordinatorControllerConfig {
  enabled?: boolean
  holderId?: string
  leaseTtlMs?: number
  tickIntervalMs?: number
}

export interface CoordinatorModeConfig {
  clusterId: string
  groupId: string
  endpoint?: string
  votingDataBearingNodeIds?: string[]
  coordinator: ClusterCoordinator
  sessionTtlMs?: number
  controller?: boolean | CoordinatorControllerConfig
  compatibility?: CoordinatorCompatibilityMetadata
}

export interface ReplicationConfig {
  nodeId?: string
  topology: Topology
  transport: ReplicationTransport
  transportConfig?: TransportConfig
  writeForwarding?: boolean
  conflictResolvers?: Record<string, ConflictResolver>
  defaultConflictResolver?: ConflictResolver
  batchSize?: number
  batchIntervalMs?: number
  maxPendingBatches?: number
  snapshotThreshold?: number
  maxClockDriftMs?: number
  maxBatchChanges?: number
  ackTimeoutMs?: number
  onBeforeForwardedQuery?: (sql: string, params?: unknown[] | Record<string, unknown>) => void
  flowControl?: {
    maxLagSeconds?: number
    onLagExceeded?: (peerId: string, lagMs: number) => void
  }
  initialSync?: boolean
  syncBatchSize?: number
  maxConcurrentSyncs?: number
  maxSyncDurationMs?: number
  maxSyncLagBeforeReady?: number
  syncAckTimeoutMs?: number
  catchUpDeadlineMs?: number
  resumeFromSeq?: bigint
  snapshotConnectionFactory?: () => Promise<SQLiteConnection>
  changeTracker?: ChangeTracker
  coordinator?: CoordinatorModeConfig
}

export interface ReplicationStatus {
  nodeId: string
  role: TopologyRole
  peers: PeerState[]
  localSeq: bigint
  replicating: boolean
  syncState?: SyncState
  coordinator?: CoordinatorRuntimeStatus
}

export interface CoordinatorRuntimeStatus {
  clusterId: string
  groupId: string
  currentPrimary: ReplicationGroupState['currentPrimary']
  primaryTerm: bigint
  inSyncNodeIds: string[]
  drainingNodeIds: string[]
  repairingNodeIds: string[]
  faultedNodeIds: string[]
  votingDataBearingNodeIds: string[]
  authority: boolean
  controllerState: 'disabled' | 'standby' | 'active' | 'lost'
}

export interface ApplyResult {
  applied: number
  skipped: number
  conflicts: number
  droppedTables: string[]
}

export type SyncPhase = 'pending' | 'syncing' | 'catching-up' | 'ready'

export interface SyncState {
  phase: SyncPhase
  sourcePeerId: string | null
  snapshotSeq: bigint | null
  completedTables: string[]
  totalTables: number
  startedAt: number | null
  error: string | null
}

export interface SyncRequest {
  requestId: string
  joinerNodeId: string
  completedTables: string[]
  groupId?: string
  primaryTerm?: bigint
}

export interface SyncBatch {
  requestId: string
  table: string
  batchIndex: number
  rows: Record<string, unknown>[]
  schema?: string[]
  checksum: string
  isLastBatchForTable: boolean
  totalTables?: number
  groupId?: string
  primaryTerm?: bigint
}

export interface SyncTableManifest {
  table: string
  rowCount: number
  pkHash: string
}

export interface SyncComplete {
  requestId: string
  snapshotSeq: bigint
  manifests: SyncTableManifest[]
  groupId?: string
  primaryTerm?: bigint
}

export interface SyncAck {
  requestId: string
  joinerNodeId: string
  table: string
  batchIndex: number
  success: boolean
  error?: string
  groupId?: string
  primaryTerm?: bigint
}

export interface ReplicationErrorEvent {
  error: Error
  operation: string
  peerId?: string
  recoverable: boolean
}
