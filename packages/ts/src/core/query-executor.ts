import { encodeWireRowsInPlace } from './cdc/encoding.js'
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
 * {@link encodeWireRowsInPlace} narrow safe-range integers and tag the rest
 * without cloning a row, so the server never narrows on the driver and then
 * re-scans to tag. The rows are freshly materialised for this response alone,
 * so in-place mutation is safe here.
 */
export async function queryForWire(conn: SQLiteConnection, sql: string, params?: Params): Promise<unknown[]> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    const bound = bindParams(params)
    const rows = (stmt.allRaw ? await stmt.allRaw(...bound) : await stmt.all(...bound)) as unknown[]
    return encodeWireRowsInPlace(rows)
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

export interface GroupStatement {
  sql: string
  params?: Params
}

/** The unit, not the statement, is the boundary a savepoint wraps and a failure is confined to. */
export interface GroupUnit {
  statements: readonly GroupStatement[]
}

export type GroupOutcome = { ok: true; values: ExecuteResult[] } | { ok: false; error: unknown }

/**
 * Runs independent units in one transaction so a single commit fsync covers the
 * whole group. On any error the group is retried with a savepoint per unit, so
 * one failing unit fails only itself and leaves its neighbours committed. Units
 * run to completion in turn, so a unit only ever observes an earlier unit that
 * succeeded. Settles after the commit, so an acknowledged unit is as durable as
 * one committed alone.
 */
export async function executeGroup(conn: SQLiteConnection, units: readonly GroupUnit[]): Promise<GroupOutcome[]> {
  try {
    await conn.exec('BEGIN')
    const outcomes: GroupOutcome[] = new Array(units.length)
    for (let i = 0; i < units.length; i++) {
      outcomes[i] = { ok: true, values: await runUnit(conn, units[i]) }
    }
    await conn.exec('COMMIT')
    return outcomes
  } catch {
    try {
      await conn.exec('ROLLBACK')
    } catch {
      /* nothing durable to preserve; the isolated retry re-runs the whole group */
    }
    return executeGroupIsolated(conn, units)
  }
}

async function runUnit(conn: SQLiteConnection, unit: GroupUnit): Promise<ExecuteResult[]> {
  const values: ExecuteResult[] = new Array(unit.statements.length)
  for (let i = 0; i < unit.statements.length; i++) {
    const statement = unit.statements[i]
    values[i] = await execute(conn, statement.sql, statement.params)
  }
  return values
}

async function executeGroupIsolated(conn: SQLiteConnection, units: readonly GroupUnit[]): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(units.length)
  await conn.exec('BEGIN')
  try {
    for (let i = 0; i < units.length; i++) {
      outcomes[i] = await runIsolatedUnit(conn, units[i], `sirannon_gc_${i}`)
    }
    await conn.exec('COMMIT')
    return outcomes
  } catch (commitErr) {
    try {
      await conn.exec('ROLLBACK')
    } catch {
      /* the commit already failed; nothing is durable */
    }
    const error = new QueryError(commitErr instanceof Error ? commitErr.message : String(commitErr), 'COMMIT')
    return outcomes.map(outcome => (!outcome || outcome.ok ? { ok: false, error } : outcome))
  }
}

async function runIsolatedUnit(conn: SQLiteConnection, unit: GroupUnit, savepoint: string): Promise<GroupOutcome> {
  for (const statement of unit.statements) {
    try {
      assertSqlAllowed(statement.sql)
    } catch (err) {
      return { ok: false, error: err }
    }
  }
  let opened = false
  try {
    await conn.exec(`SAVEPOINT ${savepoint}`)
    opened = true
    const values = await runUnit(conn, unit)
    await conn.exec(`RELEASE ${savepoint}`)
    return { ok: true, values }
  } catch (err) {
    if (opened) {
      try {
        await conn.exec(`ROLLBACK TO ${savepoint}`)
        await conn.exec(`RELEASE ${savepoint}`)
      } catch {
        /* the enclosing transaction will roll back if this savepoint cannot be unwound */
      }
    }
    return { ok: false, error: err }
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
  if (conn.runBatch) {
    assertSqlAllowed(sql)
    try {
      return await conn.runBatch(sql, paramsBatch.map(bindParams))
    } catch (err) {
      throw new QueryError(err instanceof Error ? err.message : String(err), sql)
    }
  }
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
  if (conn.runBatchSummary) {
    assertSqlAllowed(sql)
    try {
      return await conn.runBatchSummary(sql, paramsBatch.map(bindParams))
    } catch (err) {
      throw new QueryError(err instanceof Error ? err.message : String(err), sql)
    }
  }
  let changes = 0
  await forEachBatchRow(conn, sql, paramsBatch, rowChanges => {
    changes += rowChanges
  })
  return { rowsLoaded: paramsBatch.length, changes }
}
