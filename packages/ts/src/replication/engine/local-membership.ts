import { selectMaxAssignedChangeSeq, selectPrimaryTermStart } from '../../core/system-catalog/index.js'
import { isGroupMember } from '../coordinator/group-rules.js'
import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError, TopologyError } from '../errors.js'
import { applyCoordinatorState, errorDetails, hasCurrentPrimaryAuthorityFor } from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'
import { updateInSyncSetFromCurrent } from './in-sync-updates.js'

/**
 * Returns true when the group state that this node last read counts the node among the group's voters or pending
 * voters.
 *
 * @param engine - The node's engine.
 * @returns False before the node reads a group state, and for a node outside the group.
 *
 * @internal
 */
export function isLocalNodeMember(engine: ReplicationEngine): boolean {
  const state = engine.coordinatorState
  return state !== null && isGroupMember(state, engine.nodeId)
}

/**
 * Returns true when the group state that this node last read shows that an operator removed the node from the group,
 * so that the node refuses to serve reads and writes.
 *
 * @param engine - The node's engine.
 * @returns False before the node reads a group state, and for a member.
 *
 * @internal
 */
export function isLocalNodeRemoved(engine: ReplicationEngine): boolean {
  return engine.coordinatorState !== null && !isLocalNodeMember(engine)
}

/**
 * Records a group state on the engine without passing it to the demotion handler, for a state that this node wrote
 * itself.
 *
 * @param engine - The node's engine.
 * @param state - The group state that the coordinator returned.
 *
 * @internal
 */
export function keepCoordinatorState(engine: ReplicationEngine, state: ReplicationGroupState): void {
  engine.coordinatorState = state
  engine.coordinatorAuthority = hasCurrentPrimaryAuthorityFor(engine, state)
}

/**
 * Returns the group state after this replica leaves the in-sync set. The current primary never leaves it this way.
 *
 * @param engine - The replica's engine.
 * @param state - A group state that the replica read, or undefined to read the group first.
 * @param purpose - What the update records, as a phrase that completes 'while this node recorded'.
 * @returns The group state after the update.
 * @throws CoordinatorError when the coordinator holds no state for the group, or when the set changes on every
 * attempt.
 *
 * @internal
 */
export async function removeLocalNodeFromInSyncSet(
  engine: ReplicationEngine,
  state: ReplicationGroupState | undefined,
  purpose: string,
): Promise<ReplicationGroupState> {
  const next = await updateInSyncSetFromCurrent(engine, {
    purpose,
    known: state,
    change: current =>
      current.inSyncNodeIds.includes(engine.nodeId) && current.currentPrimary?.nodeId !== engine.nodeId
        ? { inSyncNodeIds: current.inSyncNodeIds.filter(nodeId => nodeId !== engine.nodeId) }
        : null,
  })
  applyCoordinatorState(engine, next)
  return next
}

/**
 * Returns the group state after this node marks itself repairing, which also takes it out of the in-sync set, so that
 * it rebuilds through a full copy before it counts towards the majority again.
 *
 * @param engine - The node's engine.
 * @param state - The group state that the node read.
 * @returns The group state after the update, or `state` when it already shows the node repairing or faulted.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function markLocalNodeRepairing(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  if (state.repairingNodeIds.includes(engine.nodeId) || state.faultedNodeIds.includes(engine.nodeId)) return state
  const updated = await writeLocalRepairing(engine, state)
  if (updated) keepCoordinatorState(engine, updated)
  return updated ?? state
}

async function writeLocalRepairing(
  engine: ReplicationEngine,
  known: ReplicationGroupState,
): Promise<ReplicationGroupState | null> {
  const config = engine.config.coordinator
  if (!config) return null
  const updated = await config.coordinator.updateNodeMaintenance({
    clusterId: config.clusterId,
    groupId: config.groupId,
    nodeId: engine.nodeId,
    repairing: true,
  })
  if (!updated) {
    throw new CoordinatorError(
      'The coordinator holds no state for this replication group, so this node cannot mark itself for repair',
      errorDetails(engine, known),
    )
  }
  return updated
}

async function fileReachesDurabilityPoint(engine: ReplicationEngine, state: ReplicationGroupState): Promise<boolean> {
  const termStart = await selectPrimaryTermStart(engine.writerConn)
  if (termStart === null || termStart.term !== state.primaryTerm) return false
  return (await selectMaxAssignedChangeSeq(engine.writerConn)) >= state.durabilityPointSeq
}

/**
 * Returns the group state after a restarted node applies the rules that make it prove its copy again. The primary
 * keeps its duty only while its file records the start of the current term and holds the group's durability point,
 * and marks itself repairing otherwise. A replica without a complete copy marks itself repairing, and every other
 * replica leaves the in-sync set until it proves that it holds every acknowledged write.
 *
 * @param engine - The restarted node's engine.
 * @param state - The group state that the node read at startup.
 * @returns The group state after the rules, or `state` for a node outside the group and for a node that the rules
 * leave as it is.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function applyCoordinatorRestartRules(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
): Promise<ReplicationGroupState> {
  const nodeId = engine.nodeId
  if (!isGroupMember(state, nodeId)) return state
  if (state.currentPrimary?.nodeId === nodeId) {
    if (await fileReachesDurabilityPoint(engine, state)) return state
    return markLocalNodeRepairing(engine, state)
  }
  const outOfService = state.repairingNodeIds.includes(nodeId) || state.faultedNodeIds.includes(nodeId)
  if (!outOfService && engine.initialSync && !(await engine.log.isSyncCompleted())) {
    return markLocalNodeRepairing(engine, state)
  }
  return removeLocalNodeFromInSyncSet(engine, state, 'its departure from the in-sync set when it restarts')
}

/**
 * Marks this replica repairing before it asks for a full copy, so that the group never promotes the replica while the
 * copy replaces its rows and admits it again only once the copy has finished in the current term.
 *
 * @param engine - The replica's engine.
 * @returns A promise that settles once the coordinator shows the replica repairing, or at once outside coordinator
 * mode, on the current primary, and on a faulted node.
 * @throws TopologyError when the group's state shows that this node is not a member.
 * @throws CoordinatorError when the coordinator holds no state for the group.
 *
 * @internal
 */
export async function markRepairingBeforeCopy(engine: ReplicationEngine): Promise<void> {
  const config = engine.config.coordinator
  const known = engine.coordinatorState
  if (!config || !known) return
  if (known.currentPrimary?.nodeId === engine.nodeId || known.faultedNodeIds.includes(engine.nodeId)) return
  if (!isGroupMember(known, engine.nodeId)) {
    throw new TopologyError(`Node '${engine.nodeId}' is not a member of replication group '${config.groupId}'`)
  }
  const updated = await writeLocalRepairing(engine, known)
  if (updated) applyCoordinatorState(engine, updated)
}
