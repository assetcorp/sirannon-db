import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import type BetterSqlite3 from 'better-sqlite3'
import { type RowCounts, withRowCounts } from '../../core/driver/row-counts.js'
import type { BatchSummary, Counted, RunResult, SQLiteStatement } from '../../core/driver/types.js'
import { narrowRowIntegers, narrowRowsIntegers, narrowSafeBigInt } from '../../core/driver/values.js'
import { SirannonError } from '../../core/errors.js'
import { optionalDependencyError } from '../../core/optional-dependency.js'

const ROW_COUNTING_PACKAGE = '@delali/sirannon-sqlite'
const ROW_COUNTS_UNSUPPORTED = 'ROW_COUNTS_UNSUPPORTED'

type CountingStatement = BetterSqlite3.Statement & { takeRowCounts(counts: Float64Array): void }

interface RowCountingModule {
  default: typeof BetterSqlite3 & { bindingPath(): string | null }
}

/**
 * The better-sqlite3 constructor from `@delali/sirannon-sqlite`, with the path of the native build that counts rows.
 *
 * @internal
 */
export interface RowCountingSqlite {
  Database: typeof BetterSqlite3
  bindingPath: string
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

/**
 * Loads better-sqlite3 from `@delali/sirannon-sqlite` together with its row-counting native build, and fails with `ROW_COUNTS_UNSUPPORTED` when either is missing.
 *
 * @param bindingPath - The path of a native build that you compiled, which replaces the build in this platform's package.
 * @returns The constructor and the path of the native build, which the constructor receives as `nativeBinding`.
 * @internal
 */
export async function loadRowCountingSqlite(bindingPath?: string): Promise<RowCountingSqlite> {
  let Database: RowCountingModule['default']
  try {
    Database = ((await import('@delali/sirannon-sqlite')) as RowCountingModule).default
  } catch (err) {
    throw optionalDependencyError(
      {
        packageName: ROW_COUNTING_PACKAGE,
        neededBy: "The better-sqlite3 driver's rowCounts option",
        installCommand: `pnpm add -E ${ROW_COUNTING_PACKAGE}`,
        code: ROW_COUNTS_UNSUPPORTED,
      },
      err,
    )
  }
  const target = `${process.platform}-${process.arch}`
  const located = bindingPath === undefined ? Database.bindingPath() : resolve(bindingPath)
  if (located === null) {
    throw new SirannonError(
      `${ROW_COUNTING_PACKAGE} found no native build for ${target}. Install ${ROW_COUNTING_PACKAGE}-${target}, or set rowCountsBindingPath to a build that you compile with native/sqlite/build.mjs.`,
      ROW_COUNTS_UNSUPPORTED,
    )
  }
  let addon: { Statement?: { prototype?: { takeRowCounts?: unknown } } }
  try {
    addon = createRequire(import.meta.url)(located)
  } catch (err) {
    throw new SirannonError(
      `The row-counting SQLite build at ${located} failed to load: ${errorMessage(err)}`,
      ROW_COUNTS_UNSUPPORTED,
    )
  }
  if (typeof addon.Statement?.prototype?.takeRowCounts !== 'function') {
    throw new SirannonError(
      `The SQLite build at ${located} has no row counters, so it cannot report rows read and rows written. Point rowCountsBindingPath at a build from native/sqlite/build.mjs.`,
      ROW_COUNTS_UNSUPPORTED,
    )
  }
  return { Database, bindingPath: located }
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
   * @param stmt - A statement prepared on the row-counting build.
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
 * @param stmt - A statement prepared on the row-counting build.
 * @param taker - The connection's row count taker.
 * @returns The driver statement.
 * @internal
 */
export function countingStatement(stmt: BetterSqlite3.Statement, taker: RowCountTaker): SQLiteStatement {
  return {
    async all<T = unknown>(...params: unknown[]): Promise<T[]> {
      return taker.measure(stmt, () => narrowRowsIntegers(stmt.all(...params) as T[])).value
    },
    async allRaw<T = unknown>(...params: unknown[]): Promise<T[]> {
      return taker.measure(stmt, () => stmt.all(...params) as T[]).value
    },
    async get<T = unknown>(...params: unknown[]): Promise<T | undefined> {
      return taker.measure(stmt, () => narrowRowIntegers(stmt.get(...params) as T | undefined)).value
    },
    async allCounted<T = unknown>(...params: unknown[]): Promise<Counted<T[]>> {
      return taker.measure(stmt, () => narrowRowsIntegers(stmt.all(...params) as T[]))
    },
    async allRawCounted<T = unknown>(...params: unknown[]): Promise<Counted<T[]>> {
      return taker.measure(stmt, () => stmt.all(...params) as T[])
    },
    async getCounted<T = unknown>(...params: unknown[]): Promise<Counted<T | undefined>> {
      return taker.measure(stmt, () => narrowRowIntegers(stmt.get(...params) as T | undefined))
    },
    async run(...params: unknown[]): Promise<RunResult> {
      const measured = taker.measure(stmt, () => stmt.run(...params))
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
      measured = taker.measure(stmt, () => stmt.run(...paramsBatch[i]))
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
 * @param stmt - A statement prepared on the row-counting build.
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
 * @param stmt - A statement prepared on the row-counting build.
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
