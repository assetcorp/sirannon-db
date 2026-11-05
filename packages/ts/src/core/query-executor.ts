import { encodeWireRowsInPlace } from './cdc/encoding.js'
import {
  addRowCounts,
  allMetered,
  type Metered,
  type RowCounts,
  rowCountsOf,
  withRowCounts,
} from './driver/row-counts.js'
import type { BatchSummary, SQLiteConnection } from './driver/types.js'
import { QueryError, SirannonError } from './errors.js'
import { assertSqlAllowed } from './internal-tables.js'
import { guardedStatement, trustedStatement } from './prepared-statements.js'
import type { BulkLoadResult, ExecuteResult, Params } from './types.js'

export function bindParams(params?: Params): unknown[] {
  if (params === undefined) return []
  if (Array.isArray(params)) return params
  return [params]
}

/**
 * Returns the error that Sirannon raises for a failed statement, keeping the row counts that the driver set on the original error.
 *
 * @param err - The error that the driver raised.
 * @param sql - The statement that failed.
 * @returns `err` itself when it is a `SirannonError`, and otherwise a `QueryError` naming the statement.
 * @internal
 */
export function asQueryError(err: unknown, sql: string): Error {
  if (err instanceof SirannonError) return err
  return withRowCounts(new QueryError(err instanceof Error ? err.message : String(err), sql), rowCountsOf(err))
}

/**
 * Returns a result holding only the change count and the last inserted row id, so that row counts stay out of what a caller receives.
 *
 * @param result - A result that can hold row counts.
 * @returns `result` itself when it holds no row counts, and otherwise a copy without them.
 * @internal
 */
export function withoutRowCounts(result: ExecuteResult): ExecuteResult {
  if (rowCountsOf(result) === undefined) return result
  return { changes: result.changes, lastInsertRowId: result.lastInsertRowId }
}

function metered<V>(value: V, counts: RowCounts | undefined): Metered<V> {
  return counts === undefined ? { value } : { value, rowsRead: counts.rowsRead, rowsWritten: counts.rowsWritten }
}

/**
 * Executes a read on one connection and returns the rows with their values decoded.
 *
 * @internal
 */
export async function query<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<T[]> {
  try {
    const handle = guardedStatement(conn, sql)
    const stmt = handle instanceof Promise ? await handle : handle
    return await stmt.all<T>(...bindParams(params))
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

/**
 * Executes a read on one connection and returns the rows, with the statement's row counts when the driver reports them.
 *
 * @param conn - The connection that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @returns The rows, with `rowsRead` and `rowsWritten` from a driver that reports `rowCounts`.
 * @internal
 */
export async function queryMetered<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<Metered<T[]>> {
  try {
    return await allMetered<T>(await guardedStatement(conn, sql), bindParams(params))
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

/**
 * Executes a read on one connection and returns the rows encoded for a network response, with the statement's row counts when the driver reports them.
 *
 * @param conn - The connection that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @returns The encoded rows, with `rowsRead` and `rowsWritten` from a driver that reports `rowCounts`.
 * @internal
 */
export async function queryForWire(conn: SQLiteConnection, sql: string, params?: Params): Promise<Metered<unknown[]>> {
  try {
    const stmt = await guardedStatement(conn, sql)
    const bound = bindParams(params)
    const counted = stmt.allRaw ? stmt.allRawCounted : stmt.allCounted
    if (counted) {
      const result = await counted.apply(stmt, bound)
      return metered(encodeWireRowsInPlace(result.value as unknown[]), result)
    }
    const rows = (stmt.allRaw ? await stmt.allRaw(...bound) : await stmt.all(...bound)) as unknown[]
    return { value: encodeWireRowsInPlace(rows) }
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

/**
 * Executes a read on one connection and returns the rows encoded for a network response, for a caller that records no
 * row counts.
 *
 * @param conn - The connection that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @returns The encoded rows.
 * @internal
 */
export async function queryRowsForWire(conn: SQLiteConnection, sql: string, params?: Params): Promise<unknown[]> {
  try {
    const handle = guardedStatement(conn, sql)
    const stmt = handle instanceof Promise ? await handle : handle
    const bound = bindParams(params)
    const rows = (stmt.allRaw ? await stmt.allRaw(...bound) : await stmt.all(...bound)) as unknown[]
    return encodeWireRowsInPlace(rows)
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

/**
 * Executes a read on one connection and returns the first row, or undefined when the read returns none.
 *
 * @internal
 */
export async function queryOne<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<T | undefined> {
  try {
    const handle = guardedStatement(conn, sql)
    const stmt = handle instanceof Promise ? await handle : handle
    return await stmt.get<T>(...bindParams(params))
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

/**
 * Executes a read on one connection and returns the first row, with the statement's row counts when the driver reports them.
 *
 * @param conn - The connection that executes the read.
 * @param sql - The statement to execute.
 * @param params - The values to bind to the statement, named or positional.
 * @returns The first row, or undefined when the read finds none, with `rowsRead` and `rowsWritten` from a driver that reports `rowCounts`.
 * @internal
 */
export async function queryOneMetered<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<Metered<T | undefined>> {
  try {
    const stmt = await guardedStatement(conn, sql)
    const bound = bindParams(params)
    if (stmt.getCounted) return await stmt.getCounted<T>(...bound)
    return { value: await stmt.get<T>(...bound) }
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

/**
 * Executes one write on a connection and returns the change count and the row id of the last inserted row.
 *
 * When the driver counts rows, the result and any error also hold `rowsRead` and `rowsWritten`.
 *
 * @internal
 */
export async function execute(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
  trusted = false,
): Promise<ExecuteResult> {
  try {
    const handle = trusted ? trustedStatement(conn, sql) : guardedStatement(conn, sql)
    const stmt = handle instanceof Promise ? await handle : handle
    const result = await stmt.run(...bindParams(params))
    return withRowCounts({ changes: result.changes, lastInsertRowId: result.lastInsertRowId }, rowCountsOf(result))
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

async function forEachBatchRow(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
  sink: (result: ExecuteResult) => void,
): Promise<void> {
  let counts: RowCounts | undefined
  try {
    const stmt = await guardedStatement(conn, sql)
    for (const params of paramsBatch) {
      const result = await stmt.run(...bindParams(params))
      counts = addRowCounts(counts, rowCountsOf(result))
      sink(withRowCounts({ changes: result.changes, lastInsertRowId: result.lastInsertRowId }, rowCountsOf(result)))
    }
  } catch (err) {
    throw withRowCounts(asQueryError(err, sql), addRowCounts(counts, rowCountsOf(err)))
  }
}

/**
 * Executes one statement on a connection once for each parameter set, and returns one result per set.
 *
 * @internal
 */
export async function executeBatch(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
): Promise<ExecuteResult[]> {
  if (conn.runBatch) {
    assertSqlAllowed(sql)
    try {
      return await conn.runBatch(sql, paramsBatch.map(bindParams))
    } catch (err) {
      throw asQueryError(err, sql)
    }
  }
  const results: ExecuteResult[] = []
  await forEachBatchRow(conn, sql, paramsBatch, result => {
    results.push(result)
  })
  return results
}

/**
 * Executes one statement on a connection once for each parameter set, and returns the totals with the row counts when the driver reports them.
 *
 * @param conn - The connection that executes the statement.
 * @param sql - The statement to execute for each parameter set.
 * @param paramsBatch - One parameter set for each execution.
 * @returns The parameter sets applied and the rows changed, with the total `rowsRead` and `rowsWritten` from a driver that reports `rowCounts`.
 * @internal
 */
export async function executeBatchSummary(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
): Promise<BulkLoadResult & Partial<RowCounts>> {
  if (conn.runBatchSummary) {
    assertSqlAllowed(sql)
    try {
      return await conn.runBatchSummary(sql, paramsBatch.map(bindParams))
    } catch (err) {
      throw asQueryError(err, sql)
    }
  }
  let changes = 0
  let counts: RowCounts | undefined
  await forEachBatchRow(conn, sql, paramsBatch, result => {
    changes += result.changes
    counts = addRowCounts(counts, rowCountsOf(result))
  })
  const summary: BatchSummary = { rowsLoaded: paramsBatch.length, changes }
  return withRowCounts(summary, counts)
}
