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
    health: status.health,
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
    health: status.health.state,
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
