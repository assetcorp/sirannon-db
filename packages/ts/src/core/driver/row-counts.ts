import type { SQLiteStatement } from './types.js'

/**
 * The number of rows that SQLite read and the number that it wrote while it executed one statement or a group of statements.
 *
 * @internal
 */
export interface RowCounts {
  rowsRead: number
  rowsWritten: number
}

/**
 * A value that a statement produced, with the statement's row counts when the driver reports them.
 *
 * @internal
 */
export type Metered<V> = { value: V } & Partial<RowCounts>

/**
 * Executes a statement and returns every row, with the statement's row counts when the driver reports them.
 *
 * @param stmt - The prepared statement.
 * @param params - The values to bind.
 * @returns The rows and, from a driver reporting `rowCounts`, the counts of this call.
 * @internal
 */
export async function allMetered<T>(stmt: SQLiteStatement, params: readonly unknown[]): Promise<Metered<T[]>> {
  if (stmt.allCounted) return stmt.allCounted<T>(...params)
  return { value: await stmt.all<T>(...params) }
}

/**
 * Returns the row counts that a result or an error holds, or `undefined` when it holds none.
 *
 * @param source - A driver result, a Sirannon result, or a thrown error.
 * @returns Both counts, or `undefined` unless `source` holds a numeric `rowsRead` and `rowsWritten`.
 * @internal
 */
export function rowCountsOf(source: unknown): RowCounts | undefined {
  if (typeof source !== 'object' || source === null) return undefined
  const { rowsRead, rowsWritten } = source as Partial<RowCounts>
  if (typeof rowsRead !== 'number' || typeof rowsWritten !== 'number') return undefined
  return { rowsRead, rowsWritten }
}

/**
 * Sets `rowsRead` and `rowsWritten` on a result or an error, and leaves it unchanged when `counts` is `undefined`.
 *
 * @param target - The object that receives the counts.
 * @param counts - The counts to set.
 * @returns `target`.
 * @internal
 */
export function withRowCounts<T>(target: T, counts: RowCounts | undefined): T {
  if (counts === undefined || typeof target !== 'object' || target === null) return target
  const writable = target as T & RowCounts
  writable.rowsRead = counts.rowsRead
  writable.rowsWritten = counts.rowsWritten
  return target
}

/**
 * Returns the sum of two sets of row counts, treating `undefined` as a set that the driver never reported.
 *
 * @param total - The running total.
 * @param next - The counts to add.
 * @returns The sum, or `undefined` when neither set exists.
 * @internal
 */
export function addRowCounts(total: RowCounts | undefined, next: RowCounts | undefined): RowCounts | undefined {
  if (next === undefined) return total
  if (total === undefined) return { rowsRead: next.rowsRead, rowsWritten: next.rowsWritten }
  return { rowsRead: total.rowsRead + next.rowsRead, rowsWritten: total.rowsWritten + next.rowsWritten }
}

/**
 * Returns the total row counts of a list of results, or `undefined` when none of them holds counts.
 *
 * @param results - The results of the statements.
 * @returns The sum of their counts.
 * @internal
 */
export function totalRowCounts(results: readonly unknown[]): RowCounts | undefined {
  let total: RowCounts | undefined
  for (const result of results) total = addRowCounts(total, rowCountsOf(result))
  return total
}
