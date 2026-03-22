export interface NodeInfo {
  id: string
  role: 'primary' | 'replica' | 'peer'
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
}

export interface ReplicationAck {
  batchId: string
  ackedSeq: bigint
  nodeId: string
}

export interface ForwardedTransaction {
  statements: Array<{ sql: string; params?: Record<string, unknown> | unknown[] }>
  requestId: string
}

export interface ForwardedTransactionResult {
  results: Array<{ changes: number; lastInsertRowId: number | string }>
  requestId: string
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

export type TopologyRole = 'primary' | 'replica' | 'peer'

export interface Topology {
  role: TopologyRole
  canWrite(): boolean
  shouldReplicateTo(peerId: string, peerRole: TopologyRole): boolean
  shouldAcceptFrom(peerId: string, peerRole: TopologyRole): boolean
  requiresConflictResolution(): boolean
}

export interface TransportConfig {
  endpoints?: string[]
  metadata?: Record<string, unknown>
}

export interface ReplicationTransport {
  connect(localNodeId: string, config: TransportConfig): Promise<void>
  disconnect(): Promise<void>
  send(peerId: string, batch: ReplicationBatch): Promise<void>
  broadcast(batch: ReplicationBatch): Promise<void>
  sendAck(peerId: string, ack: ReplicationAck): Promise<void>
  forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult>
  onBatchReceived(handler: (batch: ReplicationBatch, fromPeerId: string) => Promise<void>): void
  onAckReceived(handler: (ack: ReplicationAck, fromPeerId: string) => void): void
  onForwardReceived(
    handler: (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>,
  ): void
  sendRaftMessage(peerId: string, message: RaftMessage): Promise<void>
  broadcastRaftMessage(message: RaftMessage): Promise<void>
  onRaftMessage(handler: (message: RaftMessage, fromPeerId: string) => void): void
  onPeerConnected(handler: (peer: NodeInfo) => void): void
  onPeerDisconnected(handler: (peerId: string) => void): void
  peers(): ReadonlyMap<string, NodeInfo>
}

export type RaftMessageType =
  | 'request_vote'
  | 'vote_response'
  | 'pre_vote'
  | 'pre_vote_response'
  | 'append_entries'
  | 'append_response'
  | 'heartbeat'

export interface RaftMessage {
  type: RaftMessageType
  term: number
  candidateId?: string
  leaderId?: string
  voteGranted?: boolean
  entries?: unknown[]
  commitIndex?: number
  prevLogIndex?: number
  prevLogTerm?: number
  success?: boolean
}

export interface PeerState {
  nodeId: string
  lastAckedSeq: bigint
  lastSentSeq: bigint
  lastReceivedHlc: string
  connected: boolean
  pendingBatches: number
}

export interface ReplicationConfig {
  nodeId?: string
  topology: Topology
  transport: ReplicationTransport
  transportConfig?: TransportConfig
  writeForwarding?: boolean
  raft?: RaftConfig
  conflictResolvers?: Record<string, ConflictResolver>
  defaultConflictResolver?: ConflictResolver
  batchSize?: number
  batchIntervalMs?: number
  maxPendingBatches?: number
  snapshotThreshold?: number
  maxClockDriftMs?: number
  maxBatchChanges?: number
  batchSigningKey?: string
  onBeforeForwardedQuery?: (sql: string, params?: unknown[] | Record<string, unknown>) => void
  flowControl?: {
    maxLagSeconds?: number
    onLagExceeded?: (peerId: string, lagMs: number) => void
  }
}

export interface ReplicationStatus {
  nodeId: string
  role: TopologyRole
  peers: PeerState[]
  localSeq: bigint
  replicating: boolean
}

export interface RaftConfig {
  electionTimeoutMin?: number
  electionTimeoutMax?: number
  heartbeatInterval?: number
}

export interface ApplyResult {
  applied: number
  skipped: number
  conflicts: number
}

const BIGINT_PREFIX = '\x00sirannon:bigint:'

function bigintReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    return `${BIGINT_PREFIX}${value.toString()}`
  }
  return value
}

function bigintReviver(_key: string, value: unknown): unknown {
  if (typeof value === 'string' && value.startsWith(BIGINT_PREFIX)) {
    return BigInt(value.slice(BIGINT_PREFIX.length))
  }
  return value
}

export function serializeBatch(batch: ReplicationBatch): string {
  return JSON.stringify(batch, bigintReplacer)
}

export function deserializeBatch(raw: string): ReplicationBatch {
  return JSON.parse(raw, bigintReviver) as ReplicationBatch
}
