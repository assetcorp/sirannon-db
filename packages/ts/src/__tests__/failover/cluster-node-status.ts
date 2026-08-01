import type { ClusterStatusInfo, ReplicationStatusInfo } from '../../core/types.js'
import type { ReplicationStatus } from '../../replication/types.js'
import type { FailoverNodeConfig } from './node-process.js'

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
      nodeId => nodeId !== status.coordinator?.currentPrimary?.nodeId,
    ),
    laggingReplicas: status.coordinator?.votingDataBearingNodeIds.filter(
      nodeId => !status.coordinator?.inSyncNodeIds.includes(nodeId),
    ),
    syncState: status.syncState?.phase,
    readAvailability: readAvailability(status),
    writeAvailability: writeAvailability(status),
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
    health: clusterHealth(config, status),
  }
}

function clusterHealth(config: FailoverNodeConfig, status: ReplicationStatus): ClusterStatusInfo['health'] {
  if (status.syncState?.phase === 'syncing' || status.syncState?.phase === 'catching-up') return 'syncing'
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(config.nodeId)) return 'repairing'
  if (coordinator.authority && writeAvailability(status) === 'unavailable') return 'failing_over'
  if (readAvailability(status) === 'unavailable') return 'unavailable'
  if (!coordinator.connected) return 'degraded'
  if (coordinator.faultedNodeIds.length > 0 || coordinator.drainingNodeIds.length > 0) return 'degraded'
  if (!coordinator.inSyncNodeIds.includes(status.nodeId)) return 'degraded'
  return 'healthy'
}

function readAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (coordinator.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
}

function writeAvailability(status: ReplicationStatus): 'available' | 'unavailable' {
  const coordinator = status.coordinator
  if (!coordinator) return 'unavailable'
  if (status.syncState?.phase !== 'ready') return 'unavailable'
  if (!coordinator.authority) return 'unavailable'
  if (coordinator.drainingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.repairingNodeIds.includes(status.nodeId)) return 'unavailable'
  if (coordinator.faultedNodeIds.includes(status.nodeId)) return 'unavailable'
  return 'available'
}
