export { FieldMergeResolver } from '../core/sync/conflict/field-merge.js'
export { LWWResolver } from '../core/sync/conflict/lww.js'
export { PrimaryWinsResolver } from '../core/sync/conflict/primary-wins.js'
export { HLC } from '../core/sync/hlc.js'
export type {
  AcquireControllerLeaseInput,
  AcquireControllerLeaseResult,
  AdmitNodeToInSyncSetInput,
  ClusterCoordinator,
  CompareAndAdvancePrimaryTermInput,
  CompareAndAdvancePrimaryTermResult,
  CoordinatorCompatibilityMetadata,
  CoordinatorLease,
  CoordinatorNodeSession,
  CoordinatorPrimary,
  CoordinatorWatchDisposer,
  PromoteEligibleReplicaInput,
  RegisterNodeSessionInput,
  ReplicationGroupState,
  ReplicationGroupWatcher,
  SetReplicationGroupStateInput,
  UpdateInSyncSetInput,
  UpdateNodeMaintenanceInput,
} from './coordinator/types.js'
export { ReplicationEngine } from './engine.js'
export {
  AuthorityError,
  BatchValidationError,
  ConflictError,
  CoordinatorError,
  FailoverError,
  NodeDrainingError,
  NodeNotInSyncError,
  NoSafePrimaryError,
  ProtocolVersionMismatchError,
  ReadConcernError,
  ReplicationError,
  StalePrimaryError,
  SyncError,
  TopologyError,
  TransportError,
  UnsafeRecoveryRequiredError,
  WriteConcernError,
} from './errors.js'
export { ReplicationLog } from './log.js'
export { generateNodeId, validateNodeId } from './node-id.js'
export { PeerTracker } from './peer-tracker.js'
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
  ReplicationAck,
  ReplicationBatch,
  ReplicationChange,
  ReplicationConfig,
  ReplicationErrorEvent,
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
