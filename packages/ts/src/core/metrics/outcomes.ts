import { type Metered, rowCountsOf, totalRowCounts } from '../driver/row-counts.js'
import type { ExecuteResult } from '../types.js'
import type { QueryOutcome } from './collector.js'

const CODES_OF_AN_UNKNOWN_OUTCOME = new Set([
  'WRITER_WORKER_TIMEOUT',
  'WRITER_WORKER_EXIT',
  'WRITER_WORKER_ERROR',
  'WRITER_WORKER_FATAL',
])

/**
 * The metrics of a statement whose row counts nobody can know, because the writer worker stopped answering while it executed the statement.
 *
 * @internal
 */
export const ROW_COUNTS_UNKNOWN: QueryOutcome = Object.freeze({})

/**
 * Returns `true` when an error leaves unknown whether SQLite executed the statement, as when the writer worker times out or exits.
 *
 * @param error - The error that the statement raised.
 * @returns `true` for the writer worker's timeout, exit, error, and fatal codes.
 * @internal
 */
export function leavesOutcomeUnknown(error: unknown): boolean {
  const code = typeof error === 'object' && error !== null ? (error as { code?: unknown }).code : undefined
  return typeof code === 'string' && CODES_OF_AN_UNKNOWN_OUTCOME.has(code)
}

/**
 * Returns the metrics of a failed statement, which hold the rows that it read and wrote before it failed.
 *
 * @param error - The error that the statement raised.
 * @returns The row counts that the error carries, {@link ROW_COUNTS_UNKNOWN} when the outcome is unknown, or no fields otherwise.
 * @internal
 */
export function failedStatementOutcome(error: unknown): QueryOutcome {
  return rowCountsOf(error) ?? (leavesOutcomeUnknown(error) ? ROW_COUNTS_UNKNOWN : {})
}

/**
 * Returns the metrics of a read that returns every row.
 *
 * @param result - The rows, with the read's row counts when the driver reports them.
 * @returns The number of rows returned and the row counts.
 * @internal
 */
export function readOutcome(result: Metered<readonly unknown[]>): QueryOutcome {
  return { rowsReturned: result.value.length, ...rowCountsOf(result) }
}

/**
 * Returns the metrics of a read that returns its first row.
 *
 * @param result - The row, with the read's row counts when the driver reports them.
 * @returns One row returned, or none when the read found nothing, and the row counts.
 * @internal
 */
export function readOneOutcome(result: Metered<unknown>): QueryOutcome {
  return { rowsReturned: result.value === undefined ? 0 : 1, ...rowCountsOf(result) }
}

/**
 * Returns the metrics of one write.
 *
 * @param result - The write's result, with its row counts when the driver reports them.
 * @returns The number of rows changed and the row counts.
 * @internal
 */
export function writeOutcome(result: { changes: number }): QueryOutcome {
  return { changes: result.changes, ...rowCountsOf(result) }
}

/**
 * Returns the metrics of one statement that executed once for each parameter set.
 *
 * @param results - One result per parameter set.
 * @returns The total rows changed and the total row counts.
 * @internal
 */
export function batchWriteOutcome(results: readonly ExecuteResult[]): QueryOutcome {
  let changes = 0
  for (const result of results) changes += result.changes
  return { changes, ...totalRowCounts(results) }
}
