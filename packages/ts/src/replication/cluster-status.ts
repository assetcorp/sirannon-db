import type { ReplicationStatusInfo } from '../core/server-options.js'
import type { ClusterReadEndpointInfo, ClusterStatusInfo } from '../core/types.js'
import { COORDINATOR_ENDPOINTS_REFUSAL } from './engine/constants.js'
import { TopologyError } from './errors.js'
import type { CoordinatorRuntimeStatus, ReplicationStatus } from './types.js'

/**
 * Holds the database ID and the node addresses that {@link toClusterStatusInfo} takes in addition to the engine
 * status.
 *
 * @public
 */
export interface ClusterStatusOptions {
  /** Identifies the database that the status reports on. */
  databaseId: string
  /** Maps each node ID to the address that a client uses to reach that node. Coordinator mode refuses it with a `TopologyError` and takes each address from the node's coordinator session. */
  endpoints?: Readonly<Record<string, string>>
}

/**
 * Converts one node's engine status into the replication figures that the node's readiness endpoint reports.
 *
 * @param status - The status that the replication engine reports for this node.
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
 * Converts one node's engine status into the group status that `GET /db/{id}/cluster` returns.
 *
 * @param status - The status that the replication engine reports for this node.
 * @param options - The database that this status reports on.
 * @returns The group status, ready to return from `getClusterStatus`, whose read endpoints carry the client address in
 * each node's coordinator session.
 * @throws TopologyError when `options.endpoints` is set for a node in coordinator mode.
 *
 * @public
 */
export function toClusterStatusInfo(status: ReplicationStatus, options: ClusterStatusOptions): ClusterStatusInfo {
  const coordinator = status.coordinator
  if (coordinator && options.endpoints !== undefined) {
    throw new TopologyError(COORDINATOR_ENDPOINTS_REFUSAL)
  }
  return {
    databaseId: options.databaseId,
    replicationGroupId: coordinator?.groupId,
    role: status.role,
    currentPrimary: coordinator?.currentPrimary
      ? { ...coordinator.currentPrimary }
      : (coordinator?.currentPrimary ?? null),
    primaryTerm: coordinator?.primaryTerm,
    readEndpoints: coordinator && toClusterReadEndpoints(coordinator, coordinator.nodeEndpoints),
    health: status.health.state,
    healthReason: status.health.reason,
  }
}

/**
 * Returns every node that a client can read from, with the read concerns that each node serves.
 *
 * The list holds the group's voting data-bearing nodes, minus any node that the
 * group state marks as faulted, draining, or repairing. When the coordinator
 * status includes the IDs of the nodes with a live session, the list keeps only
 * those nodes, and it keeps only the nodes whose client address this node has
 * read, because a client cannot reach a node without one. A node in the in-sync
 * set serves both `local` and `majority` reads, while any other node serves
 * `local` reads alone, because the engine
 * answers a `local` read without checking the in-sync set.
 *
 * @param coordinator - The group state that this node last read from the coordinator.
 * @param endpoints - Maps each node ID to the address that a client uses to reach that node.
 * @returns One entry per node that a client can read from.
 *
 * @public
 */
export function toClusterReadEndpoints(
  coordinator: CoordinatorRuntimeStatus,
  endpoints: Readonly<Record<string, string>>,
): ClusterReadEndpointInfo[] {
  const live = coordinator.liveNodeIds
  const entries: ClusterReadEndpointInfo[] = []
  for (const nodeId of coordinator.votingDataBearingNodeIds) {
    const endpoint = endpoints[nodeId]
    if (endpoint === undefined || endpoint === '') continue
    if (coordinator.faultedNodeIds.includes(nodeId) || coordinator.drainingNodeIds.includes(nodeId)) continue
    if (coordinator.repairingNodeIds.includes(nodeId) || (live !== undefined && !live.includes(nodeId))) continue
    entries.push({
      nodeId,
      endpoint,
      readConcerns: coordinator.inSyncNodeIds.includes(nodeId) ? ['local', 'majority'] : ['local'],
    })
  }
  return entries
}
