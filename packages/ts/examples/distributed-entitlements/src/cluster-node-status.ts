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
          connected: true,
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
    readEndpoints: coordinatorState?.inSyncNodeIds.map(inSyncNodeId => ({
      nodeId: inSyncNodeId,
      endpoint: context.httpEndpoints[inSyncNodeId] ?? '',
      readConcerns: ['local', 'majority'],
    })),
    health: clusterHealth(status, context.nodeId),
  }
}

function clusterHealth(status: ReplicationStatus, nodeId: string): ClusterStatusInfo['health'] {
  if (status.syncState?.phase === 'syncing' || status.syncState?.phase === 'catching-up') return 'syncing'
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(nodeId)) return 'repairing'
  if (coordinatorState.authority && writeAvailability(status) === 'unavailable') return 'failing_over'
  if (readAvailability(status) === 'unavailable' && writeAvailability(status) === 'unavailable') return 'unavailable'
  if (coordinatorState.faultedNodeIds.length > 0 || coordinatorState.drainingNodeIds.length > 0) return 'degraded'
  return 'healthy'
}

function readAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinatorState = status.coordinator
  if (!coordinatorState) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (coordinatorState.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinatorState.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return coordinatorState.inSyncNodeIds.includes(status.nodeId) ? 'available' : 'unavailable'
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
