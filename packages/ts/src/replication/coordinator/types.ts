export type CoordinatorLeaseKind = 'controller' | 'node-session'

export type CoordinatorWatchDisposer = () => void | Promise<void>

export interface CoordinatorLease {
  id: string
  kind: CoordinatorLeaseKind
  clusterId: string
  holderId: string
  ttlMs: number
  grantedAtMs: number
  expiresAtMs: number
  metadata?: Record<string, unknown>
}

export interface AcquireControllerLeaseInput {
  clusterId: string
  holderId: string
  ttlMs: number
  metadata?: Record<string, unknown>
}

export type AcquireControllerLeaseResult =
  | { acquired: true; lease: CoordinatorLease }
  | { acquired: false; lease: CoordinatorLease | null }

export interface CoordinatorCompatibilityMetadata {
  packageVersion?: string
  specVersion?: string
  protocolVersion?: string
}

export interface RegisterNodeSessionInput {
  clusterId: string
  nodeId: string
  ttlMs: number
  endpoint?: string
  groupIds?: string[]
  dataBearing?: boolean
  voting?: boolean
  compatibility?: CoordinatorCompatibilityMetadata
  metadata?: Record<string, unknown>
}

export interface CoordinatorNodeSession {
  clusterId: string
  nodeId: string
  lease: CoordinatorLease
  endpoint?: string
  groupIds: string[]
  dataBearing: boolean
  voting: boolean
  compatibility?: CoordinatorCompatibilityMetadata
  metadata?: Record<string, unknown>
}

export interface CoordinatorPrimary {
  nodeId: string
  endpoint?: string
}

export interface ReplicationGroupState {
  clusterId: string
  groupId: string
  votingDataBearingNodeIds: string[]
  currentPrimary: CoordinatorPrimary | null
  primaryTerm: bigint
  inSyncNodeIds: string[]
  drainingNodeIds: string[]
  repairingNodeIds: string[]
  faultedNodeIds: string[]
  compatibility?: CoordinatorCompatibilityMetadata
  updatedAtMs: number
}

export interface SetReplicationGroupStateInput {
  clusterId: string
  groupId: string
  votingDataBearingNodeIds: string[]
  currentPrimary?: CoordinatorPrimary | null
  primaryTerm?: bigint
  inSyncNodeIds?: string[]
  drainingNodeIds?: string[]
  repairingNodeIds?: string[]
  faultedNodeIds?: string[]
  compatibility?: CoordinatorCompatibilityMetadata
}

export interface CompareAndAdvancePrimaryTermInput {
  clusterId: string
  groupId: string
  expectedPrimaryTerm: bigint
  nextPrimary: CoordinatorPrimary
}

export interface CompareAndAdvancePrimaryTermResult {
  advanced: boolean
  state: ReplicationGroupState | null
}

export interface UpdateInSyncSetInput {
  clusterId: string
  groupId: string
  inSyncNodeIds: string[]
}

export interface UpdateNodeMaintenanceInput {
  clusterId: string
  groupId: string
  nodeId: string
  draining?: boolean
  repairing?: boolean
  faulted?: boolean
}

export interface PromoteEligibleReplicaInput {
  clusterId: string
  groupId: string
  excludeNodeIds?: string[]
}

export type ReplicationGroupWatcher = (state: ReplicationGroupState) => void

export interface ClusterCoordinator {
  tryAcquireControllerLease(input: AcquireControllerLeaseInput): Promise<AcquireControllerLeaseResult>
  renewLease(leaseId: string, ttlMs: number): Promise<boolean>
  releaseLease(leaseId: string): Promise<boolean>
  registerNodeSession(input: RegisterNodeSessionInput): Promise<CoordinatorNodeSession>
  getLiveNodeSession(clusterId: string, nodeId: string): Promise<CoordinatorNodeSession | null>
  deregisterNodeSession(clusterId: string, nodeId: string): Promise<void>
  setReplicationGroupState(input: SetReplicationGroupStateInput): Promise<ReplicationGroupState>
  getReplicationGroupState(clusterId: string, groupId: string): Promise<ReplicationGroupState | null>
  watchReplicationGroup(
    clusterId: string,
    groupId: string,
    watcher: ReplicationGroupWatcher,
  ): CoordinatorWatchDisposer | Promise<CoordinatorWatchDisposer>
  compareAndAdvancePrimaryTerm(input: CompareAndAdvancePrimaryTermInput): Promise<CompareAndAdvancePrimaryTermResult>
  updateInSyncSet(input: UpdateInSyncSetInput): Promise<ReplicationGroupState | null>
  updateNodeMaintenance(input: UpdateNodeMaintenanceInput): Promise<ReplicationGroupState | null>
  promoteEligibleReplica(input: PromoteEligibleReplicaInput): Promise<ReplicationGroupState>
  close?(): Promise<void>
}
