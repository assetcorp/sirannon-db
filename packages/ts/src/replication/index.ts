export { FieldMergeResolver } from './conflict/field-merge.js'
export { LWWResolver } from './conflict/lww.js'
export { PrimaryWinsResolver } from './conflict/primary-wins.js'
export { ReplicationEngine } from './engine.js'
export {
  BatchValidationError,
  ConflictError,
  RaftError,
  ReplicationError,
  SyncError,
  TopologyError,
  TransportError,
  WriteConcernError,
} from './errors.js'
export { HLC } from './hlc.js'
export { ReplicationLog } from './log.js'
export { generateNodeId, validateNodeId } from './node-id.js'
export { PeerTracker } from './peer-tracker.js'
export { RaftLog } from './raft/raft-log.js'
export { RaftNode } from './raft/raft-node.js'
export { MultiPrimaryTopology } from './topology/multi-primary.js'
export { PrimaryReplicaTopology } from './topology/primary-replica.js'
export type {
  ApplyResult,
  ConflictContext,
  ConflictResolution,
  ConflictResolver,
  ForwardedTransaction,
  ForwardedTransactionResult,
  HLCTimestamp,
  NodeInfo,
  PeerState,
  RaftConfig,
  RaftMessage,
  RaftMessageType,
  ReplicationAck,
  ReplicationBatch,
  ReplicationChange,
  ReplicationConfig,
  ReplicationStatus,
  ReplicationTransport,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncPhase,
  SyncRequest,
  SyncState,
  SyncTableManifest,
  Topology,
  TopologyRole,
  TransportConfig,
} from './types.js'
export { deserializeBatch, deserializeSyncComplete, serializeBatch, serializeSyncComplete } from './types.js'
