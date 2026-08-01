import type { ClusterStatusInfo, ReplicationStatusInfo } from '../../core/types.js'
import type { ReplicationStatus } from '../../replication/types.js'
import type { FailoverNodeConfig } from './node-process.js'

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
      nodeId => nodeId !== status.coordinator?.currentPrimary?.nodeId,
    ),
    laggingReplicas: status.coordinator?.votingDataBearingNodeIds.filter(
      nodeId => !status.coordinator?.inSyncNodeIds.includes(nodeId),
    ),
    syncState: status.syncState?.phase,
  }
}

export function toClusterStatusInfo(
  config: FailoverNodeConfig,
  requestedDatabaseId: string,
  status: ReplicationStatus,
): ClusterStatusInfo | null {
  const databaseId = config.groupId
  if (requestedDatabaseId !== databaseId) return null
  const coordinator = status.coordinator
  return {
    databaseId,
    replicationGroupId: coordinator?.groupId,
    role: status.role,
    currentPrimary: coordinator?.currentPrimary
      ? { ...coordinator.currentPrimary }
      : (coordinator?.currentPrimary ?? null),
    primaryTerm: coordinator?.primaryTerm,
    readEndpoints: coordinator?.inSyncNodeIds.map(nodeId => ({
      nodeId,
      endpoint: config.httpEndpoints[nodeId] ?? '',
      readConcerns: ['local', 'majority'],
    })),
    health: status.health.state,
    healthReason: status.health.reason,
  }
}
