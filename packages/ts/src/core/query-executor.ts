import { encodeWireRowsInPlace } from './cdc/encoding.js'
import type { SQLiteConnection, SQLiteStatement } from './driver/types.js'
import { QueryError, SirannonError } from './errors.js'
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

function asQueryError(err: unknown, sql: string): Error {
  if (err instanceof SirannonError) return err
  return new QueryError(err instanceof Error ? err.message : String(err), sql)
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
    throw asQueryError(err, sql)
  }
}

export async function queryForWire(conn: SQLiteConnection, sql: string, params?: Params): Promise<unknown[]> {
  assertSqlAllowed(sql)
  try {
    const stmt = await getStatement(conn, sql)
    const bound = bindParams(params)
    const rows = (stmt.allRaw ? await stmt.allRaw(...bound) : await stmt.all(...bound)) as unknown[]
    return encodeWireRowsInPlace(rows)
  } catch (err) {
    throw asQueryError(err, sql)
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
    throw asQueryError(err, sql)
  }
}

export async function execute(
  conn: SQLiteConnection,
  sql: string,
  params?: Params,
  trusted = false,
): Promise<ExecuteResult> {
  if (!trusted) {
    assertSqlAllowed(sql)
  }
  try {
    const stmt = await getStatement(conn, sql)
    const result = await stmt.run(...bindParams(params))
    return {
      changes: result.changes,
      lastInsertRowId: result.lastInsertRowId,
    }
  } catch (err) {
    throw asQueryError(err, sql)
  }
}

export interface GroupStatement {
  sql: string
  params?: Params
  trusted?: boolean
}

export interface GroupUnit {
  statements: readonly GroupStatement[]
}

export type GroupOutcome = { ok: true; values: ExecuteResult[] } | { ok: false; error: unknown }

export async function executeGroup(conn: SQLiteConnection, units: readonly GroupUnit[]): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(units.length)
  try {
    await conn.exec('BEGIN')
    for (let i = 0; i < units.length; i++) {
      outcomes[i] = { ok: true, values: await runUnit(conn, units[i]) }
    }
  } catch {
    // Only a pre-commit failure is safe to retry. A COMMIT that reports an
    // error may still have reached the disk, so replaying the group would
    // apply every write twice.
    await rollbackQuietly(conn)
    return executeGroupIsolated(conn, units)
  }

  const failure = await execControl(conn, 'COMMIT')
  if (!failure) return outcomes
  await rollbackQuietly(conn)
  return outcomes.map(() => ({ ok: false, error: failure }))
}

async function runUnit(conn: SQLiteConnection, unit: GroupUnit): Promise<ExecuteResult[]> {
  const values: ExecuteResult[] = new Array(unit.statements.length)
  for (let i = 0; i < unit.statements.length; i++) {
    const statement = unit.statements[i]
    values[i] = await execute(conn, statement.sql, statement.params, statement.trusted === true)
  }
  return values
}

function controlError(err: unknown, sql: string): Error {
  return asQueryError(err, sql)
}

async function execControl(conn: SQLiteConnection, sql: string): Promise<Error | null> {
  try {
    await conn.exec(sql)
    return null
  } catch (err) {
    return controlError(err, sql)
  }
}

async function rollbackQuietly(conn: SQLiteConnection): Promise<void> {
  try {
    await conn.exec('ROLLBACK')
  } catch {}
}

interface UnitAttempt {
  outcome: GroupOutcome
  contained: boolean
}

async function executeGroupIsolated(conn: SQLiteConnection, units: readonly GroupUnit[]): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(units.length)
  await conn.exec('BEGIN')
  for (let i = 0; i < units.length; i++) {
    const attempt = await runIsolatedUnit(conn, units[i], `sirannon_gc_${i}`)
    outcomes[i] = attempt.outcome
    if (!attempt.contained) return rerunSurvivorsAlone(conn, units, outcomes, i)
  }

  const failure = await execControl(conn, 'COMMIT')
  if (!failure) return outcomes
  await rollbackQuietly(conn)
  return outcomes.map(outcome => (outcome.ok ? { ok: false, error: failure } : outcome))
}

async function runIsolatedUnit(conn: SQLiteConnection, unit: GroupUnit, savepoint: string): Promise<UnitAttempt> {
  const notOpened = await execControl(conn, `SAVEPOINT ${savepoint}`)
  if (notOpened) return { outcome: { ok: false, error: notOpened }, contained: false }

  let values: ExecuteResult[]
  try {
    values = await runUnit(conn, unit)
  } catch (err) {
    return { outcome: { ok: false, error: err }, contained: await unwind(conn, savepoint) }
  }

  const notReleased = await execControl(conn, `RELEASE ${savepoint}`)
  if (notReleased) return { outcome: { ok: false, error: notReleased }, contained: false }
  return { outcome: { ok: true, values }, contained: true }
}

async function unwind(conn: SQLiteConnection, savepoint: string): Promise<boolean> {
  if (await execControl(conn, `ROLLBACK TO ${savepoint}`)) return false
  return (await execControl(conn, `RELEASE ${savepoint}`)) === null
}

async function rerunSurvivorsAlone(
  conn: SQLiteConnection,
  units: readonly GroupUnit[],
  outcomes: GroupOutcome[],
  failed: number,
): Promise<GroupOutcome[]> {
  await rollbackQuietly(conn)
  for (let i = 0; i < units.length; i++) {
    if (i === failed) continue
    outcomes[i] = await runUnitAlone(conn, units[i])
  }
  return outcomes
}

async function runUnitAlone(conn: SQLiteConnection, unit: GroupUnit): Promise<GroupOutcome> {
  const notBegun = await execControl(conn, 'BEGIN')
  if (notBegun) return { ok: false, error: notBegun }

  let values: ExecuteResult[]
  try {
    values = await runUnit(conn, unit)
  } catch (err) {
    await rollbackQuietly(conn)
    return { ok: false, error: err }
  }

  const notCommitted = await execControl(conn, 'COMMIT')
  if (!notCommitted) return { ok: true, values }
  await rollbackQuietly(conn)
  return { ok: false, error: notCommitted }
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
    throw asQueryError(err, sql)
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
      throw asQueryError(err, sql)
    }
  }
  const results: ExecuteResult[] = []
  await forEachBatchRow(conn, sql, paramsBatch, (changes, lastInsertRowId) => {
    results.push({ changes, lastInsertRowId })
  })
  return results
}

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
      throw asQueryError(err, sql)
    }
  }
  let changes = 0
  await forEachBatchRow(conn, sql, paramsBatch, rowChanges => {
    changes += rowChanges
  })
  return { rowsLoaded: paramsBatch.length, changes }
}
