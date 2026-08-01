import type { ClusterStatusInfo, ReplicationStatusInfo } from '@delali/sirannon-db'
import type { ReplicationStatus } from '@delali/sirannon-db/replication'

export interface ClusterStatusContext {
  databaseId: string
  nodeId: string
  httpEndpoints: Record<string, string>
}

export function toReplicationStatusInfo(status: ReplicationStatus): ReplicationStatusInfo {
  return {
    role: status.role,
    writeForwarding: true,
    peers: status.peers.length,
    localSeq: BigInt(status.localSeq),
    replicationGroupId: status.coordinator?.groupId,
    primaryTerm: status.coordinator?.primaryTerm,
    currentPrimary: status.coordinator?.currentPrimary?.nodeId,
    coordinator: status.coordinator
      ? {
          connected: status.coordinator.connected,
          authority: status.coordinator.authority,
        }
      : undefined,
    controller: status.coordinator
      ? {
          state: status.coordinator.controllerState,
        }
      : undefined,
    inSyncReplicas: status.coordinator?.inSyncNodeIds.filter(
      inSyncNodeId => inSyncNodeId !== status.coordinator?.currentPrimary?.nodeId,
    ),
    laggingReplicas: status.coordinator?.votingDataBearingNodeIds.filter(
      votingNodeId => !status.coordinator?.inSyncNodeIds.includes(votingNodeId),
    ),
    syncState: status.syncState?.phase,
    readAvailability: readAvailability(status),
    writeAvailability: writeAvailability(status),
  }
}

export function toClusterStatusInfo(
  id: string,
  status: ReplicationStatus,
  context: ClusterStatusContext,
): ClusterStatusInfo | null {
  if (id !== context.databaseId) return null
  const coordinatorState = status.coordinator
  return {
    databaseId: context.databaseId,
    replicationGroupId: coordinatorState?.groupId,
    role: status.role,
    currentPrimary: coordinatorState?.currentPrimary
      ? { ...coordinatorState.currentPrimary }
      : (coordinatorState?.currentPrimary ?? null),
    primaryTerm: coordinatorState?.primaryTerm,
    readEndpoints: coordinatorState && readEndpoints(coordinatorState, context),
    health: clusterHealth(status, context.nodeId),
  }
}

type CoordinatorState = NonNullable<ReplicationStatus['coordinator']>

function readEndpoints(
  coordinatorState: CoordinatorState,
  context: ClusterStatusContext,
): NonNullable<ClusterStatusInfo['readEndpoints']> {
  const serving = coordinatorState.votingDataBearingNodeIds.filter(
    nodeId =>
      !coordinatorState.faultedNodeIds.includes(nodeId) &&
      !coordinatorState.drainingNodeIds.includes(nodeId) &&
      !coordinatorState.repairingNodeIds.includes(nodeId),
  )

  return serving.map(nodeId => ({
    nodeId,
    endpoint: context.httpEndpoints[nodeId] ?? '',
    readConcerns: coordinatorState.inSyncNodeIds.includes(nodeId) ? ['local', 'majority'] : ['local'],
  }))
}

function clusterHealth(status: ReplicationStatus, nodeId: string): ClusterStatusInfo['health'] {
  if (status.syncState?.phase === 'syncing' || status.syncState?.phase === 'catching-up') return 'syncing'
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(nodeId)) return 'repairing'
  if (coordinatorState.authority && writeAvailability(status) === 'unavailable') return 'failing_over'
  if (readAvailability(status) === 'unavailable') return 'unavailable'
  if (!coordinatorState.connected) return 'degraded'
  if (coordinatorState.faultedNodeIds.length > 0 || coordinatorState.drainingNodeIds.length > 0) return 'degraded'
  if (!coordinatorState.inSyncNodeIds.includes(nodeId)) return 'degraded'
  return 'healthy'
}

function readAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (coordinatorState.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
}

function writeAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (!coordinatorState.authority) return 'unavailable'
  if (coordinatorState.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
}
