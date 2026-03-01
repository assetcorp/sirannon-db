import type Database from 'better-sqlite3'
import { QueryError } from './errors.js'
import type { ExecuteResult, Params } from './types.js'

type SqliteDb = InstanceType<typeof Database>

interface PreparedStatement {
  all(...params: unknown[]): unknown[]
  get(...params: unknown[]): unknown
  run(...params: unknown[]): {
    changes: number
    lastInsertRowid: number | bigint
  }
}

const STATEMENT_CACHE_CAPACITY = 128
const statementCaches = new WeakMap<SqliteDb, Map<string, PreparedStatement>>()

function getStatement(db: SqliteDb, sql: string): PreparedStatement {
  let cache = statementCaches.get(db)
  if (!cache) {
    cache = new Map()
    statementCaches.set(db, cache)
  }

  const cached = cache.get(sql)
  if (cached) {
    cache.delete(sql)
    cache.set(sql, cached)
    return cached
  }

  const stmt = db.prepare(sql)
  cache.set(sql, stmt)

  if (cache.size > STATEMENT_CACHE_CAPACITY) {
    const oldest = cache.keys().next().value
    if (oldest !== undefined) {
      cache.delete(oldest)
    }
  }

  return stmt
}

function bindParams(params?: Params): unknown[] {
  if (params === undefined) return []
  if (Array.isArray(params)) return params
  return [params]
}

export function query<T = Record<string, unknown>>(db: SqliteDb, sql: string, params?: Params): T[] {
  try {
    const stmt = getStatement(db, sql)
    return stmt.all(...bindParams(params)) as T[]
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export function queryOne<T = Record<string, unknown>>(db: SqliteDb, sql: string, params?: Params): T | undefined {
  try {
    const stmt = getStatement(db, sql)
    return stmt.get(...bindParams(params)) as T | undefined
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export function execute(db: SqliteDb, sql: string, params?: Params): ExecuteResult {
  try {
    const stmt = getStatement(db, sql)
    const result = stmt.run(...bindParams(params))
    return {
      changes: result.changes,
      lastInsertRowId: result.lastInsertRowid,
    }
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}

export function executeBatch(db: SqliteDb, sql: string, paramsBatch: Params[]): ExecuteResult[] {
  try {
    const stmt = getStatement(db, sql)
    const run = db.transaction(() => {
      const results: ExecuteResult[] = []
      for (const params of paramsBatch) {
        const result = stmt.run(...bindParams(params))
        results.push({
          changes: result.changes,
          lastInsertRowId: result.lastInsertRowid,
        })
      }
      return results
    })
    return run()
  } catch (err) {
    throw new QueryError(err instanceof Error ? err.message : String(err), sql)
  }
}
