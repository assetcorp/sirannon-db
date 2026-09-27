import type { BackupGroupMembership, BackupGroupSource } from '../core/backup/preferred-node.js'
import type { ClusterCoordinator } from './coordinator/types.js'
import { CoordinatorError } from './errors.js'

/**
 * Configures {@link coordinatorBackupGroup} with the coordinator that stores the
 * group's state, the replication group to read backup membership from, and this
 * node's ID.
 *
 * @public
 */
export interface CoordinatorBackupGroupOptions {
  /** Holds the coordinator that stores primary authority, node sessions, and the group state. */
  coordinator: ClusterCoordinator
  /** Identifies the cluster that contains the group. */
  clusterId: string
  /** Identifies the replication group. */
  groupId: string
  /** Identifies this node. Pass the same node ID that this node's replication engine uses. */
  nodeId: string
}

/**
 * Returns a backup group source that reads the group's membership from the
 * coordinator that the group already uses for failover. The backup cycle calls
 * `readMembership` on the source before the cycle copies anything.
 *
 * Every node of the group passes one of these sources to its `backups` option.
 * On each scheduled turn, every node reads the same membership and picks the
 * same node to take the backup, while the other nodes skip that turn. A
 * failover can change which node takes the backups, but you keep the same
 * schedule on every node.
 *
 * The membership lists the nodes in the group's in-sync set, minus any node
 * that the group state marks as draining, repairing, or faulted.
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
