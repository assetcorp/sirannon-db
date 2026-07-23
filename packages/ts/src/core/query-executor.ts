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

/**
 * A driver reports a failed statement as a plain Error, so only those become a
 * QueryError. Anything this package already raised carries a code the server
 * maps to a status, and rewrapping it would report a crashed writer or a
 * rejected table as an ordinary SQL error.
 */
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

/** The unit, not the statement, is the boundary a savepoint wraps and a failure is confined to. */
export interface GroupUnit {
  statements: readonly GroupStatement[]
}

export type GroupOutcome = { ok: true; values: ExecuteResult[] } | { ok: false; error: unknown }

/**
 * Runs independent units in one transaction so a single commit fsync covers the
 * whole group. A unit that fails is retried with a savepoint per unit, so one
 * failing unit fails only itself and leaves its neighbours committed. Units run
 * to completion in turn, so a unit only ever observes an earlier unit that
 * succeeded. Settles after the commit, so an acknowledged unit is as durable as
 * one committed alone.
 *
 * Only a failure before the commit is retried. A commit that reports an error
 * may still have reached the disk, and re-running the units would apply every
 * one of them twice.
 */
export async function executeGroup(conn: SQLiteConnection, units: readonly GroupUnit[]): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(units.length)
  try {
    await conn.exec('BEGIN')
    for (let i = 0; i < units.length; i++) {
      outcomes[i] = { ok: true, values: await runUnit(conn, units[i]) }
    }
  } catch {
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
  } catch {
    /* already rolled back */
  }
}

/**
 * `ON CONFLICT ROLLBACK`, a `RAISE(ROLLBACK)` trigger, `SQLITE_FULL` and I/O
 * errors roll back the whole transaction, emptying the savepoint stack and
 * leaving the connection in autocommit, where a later SAVEPOINT/RELEASE pair
 * would commit a transaction of its own. `contained` is false once that is
 * possible.
 */
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

/** `failed` keeps its own error; the rest were rolled back undurably and are re-run. */
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
      throw asQueryError(err, sql)
    }
  }
  let changes = 0
  await forEachBatchRow(conn, sql, paramsBatch, rowChanges => {
    changes += rowChanges
  })
  return { rowsLoaded: paramsBatch.length, changes }
}
