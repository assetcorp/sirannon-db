import type BetterSqlite3 from 'better-sqlite3'
import { type RowCounts, withRowCounts } from '../../core/driver/row-counts.js'
import type { BatchSummary, Counted, RunResult, SQLiteStatement } from '../../core/driver/types.js'
import { bindableParams, narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'
import type { SirannonSqliteFeature } from './sirannon-sqlite.js'

type CountingStatement = BetterSqlite3.Statement & { takeRowCounts(counts: Float64Array): void }

/**
 * The row counters that the `rowCounts` option needs from Sirannon's SQLite build.
 *
 * @internal
 */
export const ROW_COUNTS_FEATURE: SirannonSqliteFeature = {
  option: 'rowCounts',
  code: 'ROW_COUNTS_UNSUPPORTED',
  description: 'row counters',
  presentIn: addon => typeof addon.Statement?.prototype?.takeRowCounts === 'function',
}

/**
 * Takes the row counts that SQLite accumulated on a statement and resets them, through one buffer that a connection reuses for every statement.
 *
 * @internal
 */
export class RowCountTaker {
  private readonly counts = new Float64Array(2)

  /**
   * Returns the statement's row counts since the last call, and resets them to zero.
   *
   * @param stmt - A statement prepared on Sirannon's SQLite build.
   * @returns The rows read and the rows written.
   */
  take(stmt: BetterSqlite3.Statement): RowCounts {
    ;(stmt as CountingStatement).takeRowCounts(this.counts)
    return { rowsRead: this.counts[0], rowsWritten: this.counts[1] }
  }

  /**
   * Calls a statement and returns its value with the row counts of that call, setting the counts on the error when the call throws.
   *
   * @param stmt - The statement that `call` executes.
   * @param call - The synchronous better-sqlite3 call.
   * @returns The call's value and its row counts.
   */
  measure<V>(stmt: BetterSqlite3.Statement, call: () => V): Counted<V> {
    let value: V
    try {
      value = call()
    } catch (err) {
      throw withRowCounts(err, this.takeAfterFailure(stmt))
    }
    const counts = this.take(stmt)
    return { value, rowsRead: counts.rowsRead, rowsWritten: counts.rowsWritten }
  }

  private takeAfterFailure(stmt: BetterSqlite3.Statement): RowCounts | undefined {
    try {
      return this.take(stmt)
    } catch {
      return undefined
    }
  }
}

function toRunResult(result: BetterSqlite3.RunResult, counts: RowCounts): RunResult {
  return {
    changes: Number(result.changes),
    lastInsertRowId: narrowSafeBigInt(result.lastInsertRowid) as number | bigint,
    rowsRead: counts.rowsRead,
    rowsWritten: counts.rowsWritten,
  }
}

/**
 * Returns a statement whose every call reports its row counts, and whose uncounted calls reset the counters so that no call inherits another call's counts.
 *
 * @param stmt - A statement prepared on Sirannon's SQLite build.
 * @param taker - The connection's row count taker.
 * @returns The driver statement.
 * @internal
 */
export function countingStatement(stmt: BetterSqlite3.Statement, taker: RowCountTaker): SQLiteStatement {
  return {
    async all<T = unknown>(...params: unknown[]): Promise<T[]> {
      return taker.measure(stmt, () => narrowRowsIntegers(stmt.all(...bindableParams(params)) as T[])).value
    },
    async allRaw<T = unknown>(...params: unknown[]): Promise<T[]> {
      return taker.measure(stmt, () => stmt.all(...bindableParams(params)) as T[]).value
    },
    async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
      return taker.measure(stmt, () => narrowRowIntegers(stmt.get(...bindableParams(params)) as T | undefined)).value
    },
    async allCounted<T = unknown>(...params: unknown[]): Promise<Counted<T[]>> {
      return taker.measure(stmt, () => narrowRowsIntegers(stmt.all(...bindableParams(params)) as T[]))
    },
    async allRawCounted<T = unknown>(...params: unknown[]): Promise<Counted<T[]>> {
      return taker.measure(stmt, () => stmt.all(...bindableParams(params)) as T[])
    },
    async getCounted<T = unknown>(...params: unknown[]): Promise<Counted<T | undefined>> {
      return taker.measure(stmt, () => narrowRowIntegers(stmt.get(...bindableParams(params)) as T | undefined))
    },
    async run(...params: unknown[]): Promise<RunResult> {
      const measured = taker.measure(stmt, () => stmt.run(...bindableParams(params)))
      return toRunResult(measured.value, measured)
    },
  }
}

function runEach(
  stmt: BetterSqlite3.Statement,
  paramsBatch: readonly unknown[][],
  taker: RowCountTaker,
  sink: (result: RunResult) => void,
): RowCounts {
  const total: RowCounts = { rowsRead: 0, rowsWritten: 0 }
  for (let i = 0; i < paramsBatch.length; i++) {
    let measured: Counted<BetterSqlite3.RunResult>
    try {
      measured = taker.measure(stmt, () => stmt.run(...bindableParams(paramsBatch[i])))
    } catch (err) {
      const failed = err as Partial<RowCounts>
      throw withRowCounts(err, {
        rowsRead: total.rowsRead + (failed.rowsRead ?? 0),
        rowsWritten: total.rowsWritten + (failed.rowsWritten ?? 0),
      })
    }
    total.rowsRead += measured.rowsRead
    total.rowsWritten += measured.rowsWritten
    sink(toRunResult(measured.value, measured))
  }
  return total
}

/**
 * Executes a statement once for each parameter set and returns one result per set, each with its own row counts.
 *
 * @param stmt - A statement prepared on Sirannon's SQLite build.
 * @param paramsBatch - One parameter set for each execution.
 * @param taker - The connection's row count taker.
 * @returns One result per parameter set, in order. When a set fails, the error holds the counts of every set executed up to and including it.
 * @internal
 */
export function runCountedBatch(
  stmt: BetterSqlite3.Statement,
  paramsBatch: readonly unknown[][],
  taker: RowCountTaker,
): RunResult[] {
  const results: RunResult[] = new Array(paramsBatch.length)
  let index = 0
  runEach(stmt, paramsBatch, taker, result => {
    results[index++] = result
  })
  return results
}

/**
 * Executes a statement once for each parameter set and returns the totals, including the row counts of every set.
 *
 * @param stmt - A statement prepared on Sirannon's SQLite build.
 * @param paramsBatch - One parameter set for each execution.
 * @param taker - The connection's row count taker.
 * @returns The parameter sets applied, the rows changed, and the total row counts. When a set fails, the error holds the counts of every set executed up to and including it.
 * @internal
 */
export function runCountedBatchSummary(
  stmt: BetterSqlite3.Statement,
  paramsBatch: readonly unknown[][],
  taker: RowCountTaker,
): BatchSummary {
  let changes = 0
  const total = runEach(stmt, paramsBatch, taker, result => {
    changes += result.changes
  })
  return { rowsLoaded: paramsBatch.length, changes, rowsRead: total.rowsRead, rowsWritten: total.rowsWritten }
}
