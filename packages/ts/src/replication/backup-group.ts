import type { BackupGroupMembership, BackupGroupSource } from '../core/backup/preferred-node.js'
import type { ClusterCoordinator } from './coordinator/types.js'
import { CoordinatorError } from './errors.js'

/**
 * Which group a node reads its backup membership from, and where that group's
 * state is kept.
 *
 * @public
 */
export interface CoordinatorBackupGroupOptions {
  /** Where primary authority, node sessions, and group state are stored. */
  coordinator: ClusterCoordinator
  /** Identifier of the cluster the group belongs to. */
  clusterId: string
  /** Identifier of the replication group. */
  groupId: string
  /** Identifier of this node, which must match the one it replicates under. */
  nodeId: string
}

/**
 * Builds the group source a backup cycle asks before it copies anything, over
 * the coordinator the group already uses for failover.
 *
 * Every node of the group passes one of these to its `backups` option, and each
 * scheduled turn reads the same membership from it. One node finds itself named
 * and takes the backup; the others stand down. A failover changes which node is
 * named, but it changes no schedule.
 *
 * This offers the nodes the group counts as in sync, less any node being
 * drained, rebuilt, or held out as faulted.
 *
 * @param options - The coordinator, the cluster, the group, and this node's identifier.
 * @returns The source, ready to pass as `replicationGroup`.
 *
 * @public
 */
export function coordinatorBackupGroup(options: CoordinatorBackupGroupOptions): BackupGroupSource {
  const { clusterId, coordinator, groupId, nodeId } = options
  return {
    nodeId,
    async readMembership(): Promise<BackupGroupMembership> {
      const state = await coordinator.getReplicationGroupState(clusterId, groupId)
      if (!state) {
        throw new CoordinatorError(`The coordinator holds no state for replication group '${groupId}'`, {
          replicationGroupId: groupId,
        })
      }
      const unavailable = new Set([...state.drainingNodeIds, ...state.repairingNodeIds, ...state.faultedNodeIds])
      return {
        primaryNodeId: state.currentPrimary?.nodeId ?? null,
        nodeIds: state.inSyncNodeIds.filter(candidate => !unavailable.has(candidate)),
      }
    },
  }
}
