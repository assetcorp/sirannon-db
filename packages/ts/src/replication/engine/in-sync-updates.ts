import type { ReplicationGroupState } from '../coordinator/types.js'
import { CoordinatorError, StalePrimaryError } from '../errors.js'
import {
  applyCoordinatorState,
  errorDetails,
  hasCurrentPrimaryAuthorityFor,
  refreshCoordinatorState,
} from './coordinator-authority.js'
import type { ReplicationEngine } from './engine.js'

const IN_SYNC_UPDATE_ATTEMPTS = 3

/**
 * Describes the in-sync set and the durability point that one update records.
 *
 * @internal
 */
export interface InSyncChange {
  /** Lists the nodes that the update leaves in the in-sync set, which must be a subset of the current set. */
  inSyncNodeIds: string[]
  /** Holds the durability point that the update raises the group's to, when it raises one. */
  durabilityPointSeq?: bigint
}

/**
 * Describes one in-sync set update and the authority under which this node makes it.
 *
 * @internal
 */
export interface InSyncUpdate {
  /** Names what the update records, as a phrase that completes 'while this node recorded', for the error messages. */
  purpose: string
  /** Returns the change to make to a group state that the coordinator holds, or null when that state needs none. */
  change: (state: ReplicationGroupState) => InSyncChange | null
  /** Holds a group state that this node read for this update, which the first attempt uses before it reads again. */
  known?: ReplicationGroupState
  /** Holds the primary term under which this node makes the update as primary, so that the coordinator applies it only while this node still holds primary duty in that term. A replica leaves it unset. */
  primaryTerm?: bigint
}

/**
 * Returns only while a group state still shows this node as the primary in the given term, and throws otherwise.
 *
 * @param engine - The engine of the node that acts as primary.
 * @param state - The group state to check.
 * @param primaryTerm - The term in which the node acts as primary.
 * @param purpose - What the node was recording, as a phrase that completes 'before it recorded'.
 * @throws StalePrimaryError when another term or another primary holds the group.
 *
 * @internal
 */
export function assertPrimaryInTerm(
  engine: ReplicationEngine,
  state: ReplicationGroupState,
  primaryTerm: bigint,
  purpose: string,
): void {
  if (state.primaryTerm === primaryTerm && hasCurrentPrimaryAuthorityFor(engine, state)) return
  throw new StalePrimaryError(
    `This node lost primary authority for term ${primaryTerm} before it recorded ${purpose}`,
    errorDetails(engine, state),
  )
}

/**
 * Returns the group state that the coordinator holds after this node records an in-sync set change, which this node
 * computes from the group state that the coordinator holds.
 *
 * The coordinator refuses an in-sync set that adds a node. When another writer changes the set between this node's
 * read and its update, this function therefore reads the group again and recomputes the change, for at most three
 * attempts. A primary passes its term, so that the coordinator applies the update only while this node is still the
 * primary in that term.
 *
 * @param engine - The engine of the node that makes the update.
 * @param update - The change to compute, what it records, and the term under which a primary makes it.
 * @returns The group state after the update, or the state that this node read when that state needed no change.
 * @throws StalePrimaryError when a primary's term or primary duty has passed to another node.
 * @throws CoordinatorError when the coordinator holds no state for the group, or when the set changes on every
 * attempt.
 *
 * @internal
 */
export async function updateInSyncSetFromCurrent(
  engine: ReplicationEngine,
  update: InSyncUpdate,
): Promise<ReplicationGroupState> {
  const config = engine.config.coordinator
  if (!config) {
    throw new CoordinatorError(`This node has no coordinator configured, so it cannot record ${update.purpose}`)
  }
  const term = update.primaryTerm
  let known = update.known ?? null
  for (let attempt = 1; ; attempt++) {
    const base = known ?? (await refreshCoordinatorState(engine))
    known = null
    if (!base) {
      throw new CoordinatorError(
        'The coordinator holds no state for this replication group',
        errorDetails(engine, null),
      )
    }
    if (term !== undefined) assertPrimaryInTerm(engine, base, term, update.purpose)
    const change = update.change(base)
    if (change === null) return base
    try {
      const updated = await config.coordinator.updateInSyncSet({
        clusterId: config.clusterId,
        groupId: config.groupId,
        ...change,
        ...(term === undefined ? {} : { expectedPrimaryTerm: term, expectedPrimaryNodeId: engine.nodeId }),
      })
      if (!updated) {
        throw new CoordinatorError(
          'The coordinator holds no state for this replication group',
          errorDetails(engine, base),
        )
      }
      applyCoordinatorState(engine, updated)
      if (term !== undefined) assertPrimaryInTerm(engine, updated, term, update.purpose)
      return updated
    } catch (err: unknown) {
      if (!(err instanceof RangeError)) throw err
      if (attempt >= IN_SYNC_UPDATE_ATTEMPTS) {
        throw new CoordinatorError(`The in-sync set kept changing while this node recorded ${update.purpose}`, {
          ...errorDetails(engine, base),
          cause: err.message,
        })
      }
    }
  }
}
