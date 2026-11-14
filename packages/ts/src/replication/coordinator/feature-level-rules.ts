import { assertFeatureLevel, assertNonEmpty, FIRST_FEATURE_LEVEL, groupMemberIds } from './group-rules.js'
import type { CoordinatorNodeSession, RaiseFeatureLevelInput, ReplicationGroupState } from './types.js'

/**
 * Checks the fields of a feature level raise before a coordinator reads the group.
 *
 * @param input - The raise.
 * @throws TypeError for an empty cluster or group ID.
 * @throws RangeError for a level that is not a whole number of at least 1.
 *
 * @internal
 */
export function assertRaiseFeatureLevelInput(input: RaiseFeatureLevelInput): void {
  assertNonEmpty(input.clusterId, 'clusterId')
  assertNonEmpty(input.groupId, 'groupId')
  assertFeatureLevel(input.featureLevel)
}

/**
 * Returns the highest group feature level that a session supports.
 *
 * @param session - The node's live session.
 * @returns The level that the session registers, or 1 for a session that registers none.
 *
 * @internal
 */
export function supportedFeatureLevel(session: CoordinatorNodeSession): number {
  return session.featureLevel ?? FIRST_FEATURE_LEVEL
}

/**
 * Returns the group state after a raise of its feature level, which needs a live session from every member that
 * supports the new level.
 *
 * @param state - The group state that the coordinator holds.
 * @param input - The level to raise the group to.
 * @param sessionFor - Returns a member's live session, or null when the member holds none.
 * @param nowMs - The time to record as the state's last change.
 * @returns The new state, or `state` itself when the group is already at that level.
 * @throws RangeError for a level lower than the group's, and for a member without a live session that supports it.
 *
 * @internal
 */
export function nextFeatureLevelState(
  state: ReplicationGroupState,
  input: RaiseFeatureLevelInput,
  sessionFor: (nodeId: string) => CoordinatorNodeSession | null,
  nowMs: number,
): ReplicationGroupState {
  if (input.featureLevel < state.featureLevel) {
    throw new RangeError(
      `Replication group '${state.groupId}' is at feature level ${state.featureLevel}, and a feature level is never lowered`,
    )
  }
  if (input.featureLevel === state.featureLevel) return state
  for (const nodeId of groupMemberIds(state)) {
    const session = sessionFor(nodeId)
    if (!session) {
      throw new RangeError(
        `Node '${nodeId}' holds no live session, so the coordinator cannot show that it supports feature level ${input.featureLevel}`,
      )
    }
    const supported = supportedFeatureLevel(session)
    if (supported < input.featureLevel) {
      throw new RangeError(
        `Node '${nodeId}' supports feature level ${supported}, lower than the requested level ${input.featureLevel}`,
      )
    }
  }
  return { ...state, featureLevel: input.featureLevel, updatedAtMs: nowMs }
}
