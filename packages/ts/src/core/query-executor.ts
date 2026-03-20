import type { SQLiteConnection, SQLiteStatement } from './driver/types.js'
import { QueryError } from './errors.js'
import type { ExecuteResult, Params } from './types.js'

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
  try {
    const stmt = await getStatement(conn, sql)
    return await stmt.all<T>(...bindParams(params))
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export async function queryOne<T = Record<string, unknown>>(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
): Promise<T | undefined> {
  try {
    const stmt = await getStatement(conn, sql)
    return await stmt.get<T>(...bindParams(params))
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export async function execute(conn: SQLiteConnection, sql: string, params?: Params): Promise<ExecuteResult> {
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

export async function executeBatch(
  conn: SQLiteConnection,
  sql: string,
  paramsBatch: Params[],
): Promise<ExecuteResult[]> {
  try {
    const stmt = await getStatement(conn, sql)
    const results: ExecuteResult[] = []
    for (const params of paramsBatch) {
      const result = await stmt.run(...bindParams(params))
      results.push({
        changes: result.changes,
        lastInsertRowId: result.lastInsertRowId,
      })
    }
    return results
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}
