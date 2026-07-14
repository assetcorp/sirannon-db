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

export type GroupOutcome = { ok: true; value: ExecuteResult } | { ok: false; error: unknown }

/**
 * Runs independent writes in one transaction so a single commit fsync covers
 * the whole group. On any statement error the group is retried with a savepoint
 * per statement, so one failing write fails only itself. Settles after the
 * commit, so an acknowledged write is as durable as a lone write.
 */
export async function executeGroup(
  conn: SQLiteConnection,
  batch: readonly { sql: string; params?: Params }[],
): Promise<GroupOutcome[]> {
  try {
    await conn.exec('BEGIN')
    const values: ExecuteResult[] = []
    for (const job of batch) {
      assertSqlAllowed(job.sql)
      const stmt = await getStatement(conn, job.sql)
      const result = await stmt.run(...bindParams(job.params))
      values.push({ changes: result.changes, lastInsertRowId: result.lastInsertRowId })
    }
    await conn.exec('COMMIT')
    return values.map(value => ({ ok: true, value }))
  } catch {
    try {
      await conn.exec('ROLLBACK')
    } catch {
      /* nothing durable to preserve; the isolated retry re-runs the whole group */
    }
    return executeGroupIsolated(conn, batch)
  }
}

async function executeGroupIsolated(
  conn: SQLiteConnection,
  batch: readonly { sql: string; params?: Params }[],
): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(batch.length)
  await conn.exec('BEGIN')
  try {
    for (let i = 0; i < batch.length; i++) {
      outcomes[i] = await runIsolatedStatement(conn, batch[i], `sirannon_gc_${i}`)
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
    return outcomes.map(outcome => (outcome?.ok ? { ok: false, error } : outcome))
  }
}

async function runIsolatedStatement(
  conn: SQLiteConnection,
  job: { sql: string; params?: Params },
  savepoint: string,
): Promise<GroupOutcome> {
  try {
    assertSqlAllowed(job.sql)
  } catch (err) {
    return { ok: false, error: err }
  }
  let opened = false
  try {
    await conn.exec(`SAVEPOINT ${savepoint}`)
    opened = true
    const stmt = await getStatement(conn, job.sql)
    const result = await stmt.run(...bindParams(job.params))
    await conn.exec(`RELEASE ${savepoint}`)
    return { ok: true, value: { changes: result.changes, lastInsertRowId: result.lastInsertRowId } }
  } catch (err) {
    if (opened) {
      try {
        await conn.exec(`ROLLBACK TO ${savepoint}`)
        await conn.exec(`RELEASE ${savepoint}`)
      } catch {
        /* the enclosing transaction will roll back if this savepoint cannot be unwound */
      }
    }
    return { ok: false, error: new QueryError(err instanceof Error ? err.message : String(err), job.sql) }
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
