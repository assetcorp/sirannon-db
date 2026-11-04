import {
  assertNonEmpty,
  isGroupMember,
  normaliseNodeIds,
  removeNodeId,
  sameNodeIdSet,
  setMembership,
} from './group-rules.js'
import type { AddVoterInput, RemoveVoterInput, ReplicationGroupState } from './types.js'

/**
 * Checks the fields that both voter changes carry before a coordinator reads the group.
 *
 * @param input - The voter change.
 * @throws TypeError for an empty cluster, group, or node ID.
 * @throws RangeError when the expected voters hold an empty or a duplicate node ID.
 */
export function assertVoterChangeInput(input: AddVoterInput | RemoveVoterInput): void {
  assertNonEmpty(input.clusterId, 'clusterId')
  assertNonEmpty(input.groupId, 'groupId')
  assertNonEmpty(input.nodeId, 'nodeId')
  normaliseNodeIds(input.expectedVoterNodeIds, 'expectedVoterNodeIds')
}

/**
 * Returns true when a group's voters hold exactly the node IDs that a caller read, in any order.
 *
 * @param state - The group state that the coordinator holds.
 * @param expectedVoterNodeIds - The voters that the caller read.
 * @returns True when both lists hold the same node IDs.
 * @throws RangeError when the expected list holds an empty or a duplicate node ID.
 */
export function votersMatch(state: ReplicationGroupState, expectedVoterNodeIds: string[]): boolean {
  return sameNodeIdSet(normaliseNodeIds(expectedVoterNodeIds, 'expectedVoterNodeIds'), state.votingDataBearingNodeIds)
}

/**
 * Returns the group state after a node joins as a pending voter, which also marks it repairing so that it finishes a
 * full copy in the current term before it counts towards the majority. The function returns the state that the
 * caller passed in when the group's voters differ from the ones that the caller read, which leaves the group as it is.
 *
 * @param state - The group state that the coordinator holds.
 * @param input - The node to add and the voters that the caller read.
 * @param nowMs - The time to record as the state's last change.
 * @returns The new state, or `state` itself when the voters differ.
 * @throws RangeError while the group has a pending voter, and for a node that is already a member.
 */
export function nextAddVoterState(
  state: ReplicationGroupState,
  input: AddVoterInput,
  nowMs: number,
): ReplicationGroupState {
  if (!votersMatch(state, input.expectedVoterNodeIds)) return state
  if (state.pendingVoterNodeIds.length > 0) {
    throw new RangeError(
      `Replication group '${state.groupId}' already has pending voter '${state.pendingVoterNodeIds[0]}', and accepts no other voter change until that node joins or an operator removes it`,
    )
  }
  if (isGroupMember(state, input.nodeId)) {
    throw new RangeError(`Node '${input.nodeId}' is already a member of replication group '${state.groupId}'`)
  }
  return {
    ...state,
    pendingVoterNodeIds: [input.nodeId],
    repairingNodeIds: setMembership(state.repairingNodeIds, input.nodeId, true),
    drainingNodeIds: removeNodeId(state.drainingNodeIds, input.nodeId),
    faultedNodeIds: removeNodeId(state.faultedNodeIds, input.nodeId),
    inSyncNodeIds: removeNodeId(state.inSyncNodeIds, input.nodeId),
    updatedAtMs: nowMs,
  }
}

/**
 * Returns the group state after a voter or a pending voter leaves the group, which removes the node from every set
 * that the group records for it. The function returns the state that the caller passed in when the group's voters
 * differ from the ones that the caller read, which leaves the group as it is.
 *
 * @param state - The group state that the coordinator holds.
 * @param input - The node to remove and the voters that the caller read.
 * @param nowMs - The time to record as the state's last change.
 * @returns The new state, or `state` itself when the voters differ.
 * @throws RangeError for a node outside the group, for a voter while another node is pending, for the current
 * primary, and for a removal after which the in-sync voters are fewer than a majority of the voters.
 */
export function nextRemoveVoterState(
  state: ReplicationGroupState,
  input: RemoveVoterInput,
  nowMs: number,
): ReplicationGroupState {
  if (!votersMatch(state, input.expectedVoterNodeIds)) return state
  const nodeId = input.nodeId
  if (!isGroupMember(state, nodeId)) {
    throw new RangeError(`Node '${nodeId}' is not a member of replication group '${state.groupId}'`)
  }
  const removingPendingVoter = state.pendingVoterNodeIds.includes(nodeId)
  if (!removingPendingVoter && state.pendingVoterNodeIds.length > 0) {
    throw new RangeError(
      `Replication group '${state.groupId}' has pending voter '${state.pendingVoterNodeIds[0]}', and accepts only the removal of that node until it joins`,
    )
  }
  if (state.currentPrimary?.nodeId === nodeId) {
    throw new RangeError(`Node '${nodeId}' is the current primary of replication group '${state.groupId}'`)
  }
  const votingDataBearingNodeIds = removeNodeId(state.votingDataBearingNodeIds, nodeId)
  const inSyncNodeIds = removeNodeId(state.inSyncNodeIds, nodeId)
  const majority = Math.floor(votingDataBearingNodeIds.length / 2) + 1
  if (inSyncNodeIds.length < majority) {
    throw new RangeError(
      `Removing node '${nodeId}' would leave ${inSyncNodeIds.length} in-sync voters of ${votingDataBearingNodeIds.length}, fewer than the majority of ${majority}`,
    )
  }
  return {
    ...state,
    votingDataBearingNodeIds,
    pendingVoterNodeIds: removeNodeId(state.pendingVoterNodeIds, nodeId),
    inSyncNodeIds,
    drainingNodeIds: removeNodeId(state.drainingNodeIds, nodeId),
    repairingNodeIds: removeNodeId(state.repairingNodeIds, nodeId),
    faultedNodeIds: removeNodeId(state.faultedNodeIds, nodeId),
    updatedAtMs: nowMs,
  }
}
