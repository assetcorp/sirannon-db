import type { RowCounts } from '../driver/row-counts.js'
import type { GroupRunOutcome } from '../driver/types.js'

/**
 * Resolves to the row counts of each statement that a request executed after its caller stopped waiting for it, in
 * order, once the writer worker answers. It resolves to `null` when the worker exits, crashes, or closes before it
 * answers, because Sirannon can then never learn what SQLite did.
 *
 * @internal
 */
export type LateRowCounts = Promise<readonly (RowCounts | undefined)[] | null>

/**
 * Resolves to the outcome of each unit of a group commit whose caller stopped waiting for it, once the writer worker
 * answers, or to `null` when the worker exits, crashes, or closes first.
 *
 * @internal
 */
export type LateGroupOutcomes = Promise<readonly GroupRunOutcome[] | null>

const lateRowCounts = new WeakMap<object, LateRowCounts>()
const lateGroupOutcomes = new WeakMap<object, LateGroupOutcomes>()

/**
 * Records on an error the row counts that will arrive once the writer worker answers the request that the error
 * failed, so that the metrics can bill that work when it ends.
 *
 * @param error - The error that the caller receives.
 * @param counts - The counts that will arrive.
 * @returns `error` itself.
 * @internal
 */
export function withLateRowCounts<E extends object>(error: E, counts: LateRowCounts): E {
  lateRowCounts.set(error, counts)
  return error
}

/**
 * Marks the work that an error failed as none of the caller's own statements, so that the metrics add nothing for it
 * when the writer worker answers, as for Sirannon's own bookkeeping statements around a write.
 *
 * @param error - The error that the bookkeeping statement raised.
 * @returns `error` itself.
 * @internal
 */
export function withoutLateRowCounts(error: unknown): unknown {
  if (lateRowCountsOf(error) !== undefined) withLateRowCounts(error as object, Promise.resolve([]))
  return error
}

/**
 * Returns the row counts that will arrive for the request that an error failed.
 *
 * @param error - The error that the caller received.
 * @returns The counts that will arrive, or `undefined` when the error carries none.
 * @internal
 */
export function lateRowCountsOf(error: unknown): LateRowCounts | undefined {
  return typeof error === 'object' && error !== null ? lateRowCounts.get(error) : undefined
}

/**
 * Records on an error the unit outcomes that will arrive once the writer worker answers the group commit that the
 * error failed.
 *
 * @param error - The error that the group committer receives.
 * @param outcomes - The outcomes that will arrive.
 * @returns `error` itself.
 * @internal
 */
export function withLateGroupOutcomes<E extends object>(error: E, outcomes: LateGroupOutcomes): E {
  lateGroupOutcomes.set(error, outcomes)
  return error
}

/**
 * Returns the unit outcomes that will arrive for the group commit that an error failed.
 *
 * @param error - The error that the group committer received.
 * @returns The outcomes that will arrive, or `undefined` when the error carries none.
 * @internal
 */
export function lateGroupOutcomesOf(error: unknown): LateGroupOutcomes | undefined {
  return typeof error === 'object' && error !== null ? lateGroupOutcomes.get(error) : undefined
}
