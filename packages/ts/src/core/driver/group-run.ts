import type { RowCounts } from './row-counts.js'
import type { RunResult } from './types.js'

/**
 * Describes the error from one failed unit of a grouped write.
 *
 * @internal
 */
export interface GroupRunError {
  /** The message that SQLite or the driver raised. */
  message: string
  /** The name of the error class. */
  name?: string
  /** The machine-readable code, when the driver supplies one. */
  code?: string
  /** The statement that failed, when the error is a `QUERY_ERROR` that names one. */
  sql?: string
  /** The number of rows that SQLite read before the statement failed, when the driver counts rows. */
  rowsRead?: number
  /** The number of rows that SQLite wrote before the statement failed, when the driver counts rows. */
  rowsWritten?: number
}

/**
 * Holds one statement of a grouped write, with the values to bind to it.
 *
 * @internal
 */
export interface GroupRunStatement {
  /** The statement text. */
  sql: string
  /** The values to bind to the statement, in order. */
  params?: readonly unknown[]
  /** `true` for a statement that Sirannon writes itself, which may name Sirannon's internal tables. */
  trusted?: boolean
}

/**
 * Describes the outcome of one unit in a grouped write. When the unit fails
 * and the driver counts rows, `statementRowCounts` holds the row counts of
 * each statement that the unit executed, with the failed statement last.
 *
 * @internal
 */
export type GroupRunOutcome =
  | { ok: true; results: RunResult[] }
  | { ok: false; error: GroupRunError; statementRowCounts?: RowCounts[] }
