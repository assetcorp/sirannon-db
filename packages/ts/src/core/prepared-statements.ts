import type { SQLiteConnection, SQLiteStatement } from './driver/types.js'
import { assertSqlAllowed } from './internal-tables.js'

const STATEMENT_CACHE_CAPACITY = 128

interface CachedStatement {
  readonly pending: Promise<SQLiteStatement>
  prepared: SQLiteStatement | undefined
  passedSqlGuard: boolean
}

interface StatementCache {
  readonly entries: Map<string, CachedStatement>
  newest: string | undefined
}

const statementCaches = new WeakMap<SQLiteConnection, StatementCache>()

function cacheOf(conn: SQLiteConnection): StatementCache {
  let cache = statementCaches.get(conn)
  if (cache === undefined) {
    cache = { entries: new Map(), newest: undefined }
    statementCaches.set(conn, cache)
  }
  return cache
}

function lookUp(cache: StatementCache, sql: string): CachedStatement | undefined {
  const entry = cache.entries.get(sql)
  if (entry === undefined || cache.newest === sql) return entry
  cache.entries.delete(sql)
  cache.entries.set(sql, entry)
  cache.newest = sql
  return entry
}

function prepareInto(cache: StatementCache, conn: SQLiteConnection, sql: string): CachedStatement {
  const entry: CachedStatement = { pending: conn.prepare(sql), prepared: undefined, passedSqlGuard: false }
  cache.entries.set(sql, entry)
  cache.newest = sql
  if (cache.entries.size > STATEMENT_CACHE_CAPACITY) {
    const oldest = cache.entries.keys().next().value
    if (oldest !== undefined) cache.entries.delete(oldest)
  }
  entry.pending.then(
    statement => {
      entry.prepared = statement
    },
    () => {
      if (cache.entries.get(sql) === entry) cache.entries.delete(sql)
      if (cache.newest === sql) cache.newest = undefined
    },
  )
  return entry
}

function statementOf(entry: CachedStatement): SQLiteStatement | Promise<SQLiteStatement> {
  return entry.prepared ?? entry.pending
}

/**
 * Returns the connection's prepared statement for `sql`, preparing and caching it on first use, without checking the
 * statement against the reserved identifiers.
 *
 * @param conn - The connection that owns the statement.
 * @param sql - The statement text.
 * @returns The prepared statement once it is ready, and otherwise the promise that resolves to it.
 * @internal
 */
export function trustedStatement(conn: SQLiteConnection, sql: string): SQLiteStatement | Promise<SQLiteStatement> {
  const cache = cacheOf(conn)
  return statementOf(lookUp(cache, sql) ?? prepareInto(cache, conn, sql))
}

/**
 * Returns the connection's prepared statement for `sql` after the statement passes the reserved identifier check,
 * which runs once for each cached statement, so that a repeated statement skips the scan.
 *
 * @param conn - The connection that owns the statement.
 * @param sql - The statement text that a caller supplied.
 * @returns The prepared statement once it is ready, and otherwise the promise that resolves to it.
 * @throws ForbiddenSqlError when the statement names a reserved identifier.
 * @internal
 */
export function guardedStatement(conn: SQLiteConnection, sql: string): SQLiteStatement | Promise<SQLiteStatement> {
  const cache = cacheOf(conn)
  const cached = lookUp(cache, sql)
  if (cached?.passedSqlGuard) return statementOf(cached)
  assertSqlAllowed(sql)
  const entry = cached ?? prepareInto(cache, conn, sql)
  entry.passedSqlGuard = true
  return statementOf(entry)
}
