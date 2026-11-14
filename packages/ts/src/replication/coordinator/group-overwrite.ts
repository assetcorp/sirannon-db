import type { SetReplicationGroupStateInput } from './group-inputs.js'

/**
 * Returns the input of an operator's overwrite of a group's state with the in-sync majority term set to the term
 * being written when the input omits it, so that a restarted primary assumes that the term may hold acknowledged
 * writes and proves its copy before it keeps its duty.
 *
 * @param input - The group state that the operator writes.
 * @returns `input` with `inSyncMajorityTerm` filled in.
 *
 * @internal
 */
export function overwriteGroupInput(input: SetReplicationGroupStateInput): SetReplicationGroupStateInput {
  if (input.inSyncMajorityTerm !== undefined) return input
  return { ...input, inSyncMajorityTerm: input.primaryTerm ?? 0n }
}
