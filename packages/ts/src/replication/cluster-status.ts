import type { ReplicationStatusInfo } from '../core/server-options.js'
import type { ClusterReadEndpointInfo, ClusterStatusInfo } from '../core/types.js'
import type { CoordinatorRuntimeStatus, ReplicationStatus } from './types.js'

/**
 * What a node needs to know beyond its own engine state to describe its group.
 *
 * @public
 */
export interface ClusterStatusOptions {
  /** Identifier of the database the reported status describes. */
  databaseId: string
  /** Address a client reaches each node on, keyed by node id. */
  endpoints: Readonly<Record<string, string>>
}

/**
 * Turns one node's engine status into the figures its readiness endpoint reports.
 *
 * @param status - Status the replication engine reports for this node.
 * @returns The replication figures, ready to return from `getReplicationStatus`.
 *
 * @public
 */
export function toReplicationStatusInfo(status: ReplicationStatus): ReplicationStatusInfo {
  const coordinator = status.coordinator
  return {
    role: status.role,
    writeForwarding: true,
    peers: status.peers.length,
    localSeq: status.localSeq,
    health: status.health,
    replicationGroupId: coordinator?.groupId,
    primaryTerm: coordinator?.primaryTerm,
    currentPrimary: coordinator?.currentPrimary?.nodeId,
    coordinator: coordinator && { connected: coordinator.connected, authority: coordinator.authority },
    controller: coordinator && { state: coordinator.controllerState },
    inSyncReplicas: coordinator?.inSyncNodeIds.filter(nodeId => nodeId !== coordinator.currentPrimary?.nodeId),
    laggingReplicas: coordinator?.votingDataBearingNodeIds.filter(
      nodeId => !coordinator.inSyncNodeIds.includes(nodeId),
    ),
    syncState: status.syncState?.phase,
  }
}

/**
 * Turns one node's engine status into what `GET /db/{id}/cluster` reports about its group.
 *
 * @param status - Status the replication engine reports for this node.
 * @param options - The database this status describes and the address of each node.
 * @returns The group status, ready to return from `getClusterStatus`.
 *
 * @public
 */
export function toClusterStatusInfo(status: ReplicationStatus, options: ClusterStatusOptions): ClusterStatusInfo {
  const coordinator = status.coordinator
  return {
    databaseId: options.databaseId,
    replicationGroupId: coordinator?.groupId,
    role: status.role,
    currentPrimary: coordinator?.currentPrimary
      ? { ...coordinator.currentPrimary }
      : (coordinator?.currentPrimary ?? null),
    primaryTerm: coordinator?.primaryTerm,
    readEndpoints: coordinator && toClusterReadEndpoints(coordinator, options.endpoints),
    health: status.health.state,
    healthReason: status.health.reason,
  }
}

/**
 * Lists every node a client can read from, with the read concerns each one serves.
 *
 * A node counts towards majority and is neither quarantined, being taken out of
 * service, nor being rebuilt to appear at all. A node the group counts as in
 * sync serves both `local` and `majority`; one that has fallen behind serves
 * `local` alone, because the engine answers a `local` read without any in-sync
 * check.
 *
 * @param coordinator - Group state this node last read from the coordinator.
 * @param endpoints - Address a client reaches each node on, keyed by node id.
 * @returns One entry per node a client can read from.
 *
 * @public
 */
export function toClusterReadEndpoints(
  coordinator: CoordinatorRuntimeStatus,
  endpoints: Readonly<Record<string, string>>,
): ClusterReadEndpointInfo[] {
  return coordinator.votingDataBearingNodeIds
    .filter(
      nodeId =>
        !coordinator.faultedNodeIds.includes(nodeId) &&
        !coordinator.drainingNodeIds.includes(nodeId) &&
        !coordinator.repairingNodeIds.includes(nodeId),
    )
    .map(nodeId => ({
      nodeId,
      endpoint: endpoints[nodeId] ?? '',
      readConcerns: coordinator.inSyncNodeIds.includes(nodeId) ? ['local', 'majority'] : ['local'],
    }))
}
