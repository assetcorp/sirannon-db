import { encodeTaggedValues } from './cdc/encoding.js'
import type { SQLiteConnection, SQLiteStatement } from './driver/types.js'
import { QueryError } from './errors.js'
import { assertSqlAllowed } from './internal-tables.js'
import type { BulkLoadResult, ExecuteResult, Params } from './types.js'

const STATEMENT_CACHE_CAPACITY = 128
const statementCaches = new WeakMap<SQLiteConnection, Map<string, Promise<SQLiteStatement>>>()

async function getStatement(conn: SQLiteConnection, sql: string): Promise<SQLiteStatement> {
  let cache = statementCaches.get(conn)
  if (!cache) {
    cache = new Map()
    statementCaches.set(conn, cache)
  }

  const cached = cache.get(sql)
  if (cached) {
    cache.delete(sql)
    cache.set(sql, cached)
    return cached
  }

  const pending = conn.prepare(sql)
  cache.set(sql, pending)

  if (cache.size > STATEMENT_CACHE_CAPACITY) {
    const oldest = cache.keys().next().value
    if (oldest !== undefined) {
      cache.delete(oldest)
    }
  }

  try {
    return await pending
  } catch (err) {
    cache.delete(sql)
    throw err
  }
}

export function bindParams(params?: Params): unknown[] {
  if (params === undefined) return []
  if (Array.isArray(params)) return params
  return [params]
}

export async function query<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<T[]> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    return await stmt.all<T>(...bindParams(params))
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

/**
 * Reads rows already encoded for the wire in a single walk. Pulls raw BigInt
 * rows from the driver ({@link SQLiteStatement.allRaw}) and lets
 * {@link encodeTaggedValues} both narrow safe-range integers and tag the rest,
 * so the server never narrows on the driver and then re-scans to tag.
 */
export async function queryForWire(conn: SQLiteConnection, sql: string, params?: Params): Promise<unknown[]> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    const bound = bindParams(params)
    const rows = stmt.allRaw ? await stmt.allRaw(...bound) : await stmt.all(...bound)
    return encodeTaggedValues(rows) as unknown[]
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export async function queryOne<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<T | undefined> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    return await stmt.get<T>(...bindParams(params))
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export async function execute(conn: SQLiteConnection, sql: string, params?: Params): Promise<ExecuteResult> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    const result = await stmt.run(...bindParams(params))
    return {
      changes: result.changes,
      lastInsertRowId: result.lastInsertRowId,
    }
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

async function forEachBatchRow(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
  sink: (changes: number, lastInsertRowId: number | bigint) => void,
): Promise<void> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    for (const params of paramsBatch) {
      const result = await stmt.run(...bindParams(params))
      sink(result.changes, result.lastInsertRowId)
    }
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export async function executeBatch(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
): Promise<ExecuteResult[]> {
  const results: ExecuteResult[] = []
  await forEachBatchRow(conn, sql, paramsBatch, (changes, lastInsertRowId) => {
    results.push({ changes, lastInsertRowId })
  })
  return results
}

/**
 * Sums changes instead of returning one result per row, so a million-row load
 * never materialises a million-element array.
 */
export async function executeBatchSummary(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
): Promise<BulkLoadResult> {
  let changes = 0
  await forEachBatchRow(conn, sql, paramsBatch, rowChanges => {
    changes += rowChanges
  })
  return { rowsLoaded: paramsBatch.length, changes }
}
