import { type RowCounts, rowCountsOf } from './driver/row-counts.js'
import type { SQLiteConnection } from './driver/types.js'
import { asQueryError, execute } from './query-executor.js'
import type { ExecuteResult, Params } from './types.js'
import { commitFailure } from './writer-transaction.js'

export interface GroupStatement {
  sql: string
  params?: Params
  trusted?: boolean
}

export interface GroupUnit {
  statements: readonly GroupStatement[]
}

export type GroupOutcome =
  | { ok: true; values: ExecuteResult[] }
  | { ok: false; error: unknown; statementRowCounts?: RowCounts[] }

/**
 * Executes several independent units in one transaction and returns one outcome for each unit, in order.
 *
 * A failed unit leaves the other units' writes intact, because Sirannon rolls
 * the group back and executes each unit inside a savepoint of its own. Each
 * outcome holds the row counts of the attempt that took effect.
 *
 * Sirannon executes `opening` once at the start of every transaction that it
 * begins for the group, outside every unit's savepoint, so a unit that rolls
 * back leaves the opening's write in place. When the group falls back to one
 * transaction per unit, each of those transactions executes the opening again.
 *
 * @param conn - The writer connection.
 * @param units - The units to execute, each a list of statements.
 * @param opening - The statement to execute at the start of each transaction, or `undefined` for none.
 * @returns One outcome per unit, in order, which leave out the opening's results.
 * @internal
 */
export async function executeGroup(
  conn: SQLiteConnection,
  units: readonly GroupUnit[],
  opening?: GroupStatement,
): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(units.length)
  try {
    await conn.exec('BEGIN')
  } catch {
    await rollbackQuietly(conn)
    return executeGroupIsolated(conn, units, opening)
  }
  const notOpened = await runOpening(conn, opening)
  if (notOpened) {
    await rollbackQuietly(conn)
    return units.map(() => ({ ok: false, error: notOpened }))
  }
  for (let i = 0; i < units.length; i++) {
    const attempt = await runUnit(conn, units[i])
    if (!attempt.ok) {
      await rollbackQuietly(conn)
      return executeGroupIsolated(conn, units, opening)
    }
    outcomes[i] = attempt
  }

  const failure = await execControl(conn, 'COMMIT')
  if (!failure) return outcomes
  return settleRefusedCommit(conn, units, outcomes, failure, opening)
}

async function runOpening(conn: SQLiteConnection, opening: GroupStatement | undefined): Promise<unknown> {
  if (opening === undefined) return null
  try {
    await execute(conn, opening.sql, opening.params, opening.trusted === true)
    return null
  } catch (err) {
    return err
  }
}

function countsOfEach(results: readonly unknown[]): RowCounts[] | undefined {
  const counts = results.map(rowCountsOf)
  if (counts.every(statementCounts => statementCounts === undefined)) return undefined
  return counts.map(statementCounts => statementCounts ?? { rowsRead: 0, rowsWritten: 0 })
}

function failedUnit(error: unknown, executed: readonly unknown[]): GroupOutcome {
  const statementRowCounts = countsOfEach(executed)
  return statementRowCounts ? { ok: false, error, statementRowCounts } : { ok: false, error }
}

async function runUnit(conn: SQLiteConnection, unit: GroupUnit): Promise<GroupOutcome> {
  const values: ExecuteResult[] = new Array(unit.statements.length)
  for (let i = 0; i < unit.statements.length; i++) {
    const statement = unit.statements[i]
    try {
      values[i] = await execute(conn, statement.sql, statement.params, statement.trusted === true)
    } catch (err) {
      return failedUnit(err, [...values.slice(0, i), err])
    }
  }
  return { ok: true, values }
}

function controlError(err: unknown, sql: string): Error {
  return sql === 'COMMIT' ? commitFailure(err) : asQueryError(err, sql)
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

async function executeGroupIsolated(
  conn: SQLiteConnection,
  units: readonly GroupUnit[],
  opening: GroupStatement | undefined,
): Promise<GroupOutcome[]> {
  const outcomes: GroupOutcome[] = new Array(units.length)
  await conn.exec('BEGIN')
  const notOpened = await runOpening(conn, opening)
  if (notOpened) {
    await rollbackQuietly(conn)
    return units.map(() => ({ ok: false, error: notOpened }))
  }
  for (let i = 0; i < units.length; i++) {
    const attempt = await runIsolatedUnit(conn, units[i], `sirannon_gc_${i}`)
    outcomes[i] = attempt.outcome
    if (!attempt.contained) return rerunSurvivorsAlone(conn, units, outcomes, i, opening)
  }

  const failure = await execControl(conn, 'COMMIT')
  if (!failure) return outcomes
  return settleRefusedCommit(conn, units, outcomes, failure, opening)
}

async function settleRefusedCommit(
  conn: SQLiteConnection,
  units: readonly GroupUnit[],
  outcomes: GroupOutcome[],
  failure: Error,
  opening: GroupStatement | undefined,
): Promise<GroupOutcome[]> {
  const transactionStillOpen = (await execControl(conn, 'ROLLBACK')) === null
  const rerunEachAlone = transactionStillOpen && succeededUnitCount(outcomes) > 1
  for (let i = 0; i < units.length; i++) {
    const outcome = outcomes[i]
    if (!outcome.ok) continue
    outcomes[i] = rerunEachAlone ? await runUnitAlone(conn, units[i], opening) : failedUnit(failure, outcome.values)
  }
  return outcomes
}

function succeededUnitCount(outcomes: readonly GroupOutcome[]): number {
  let count = 0
  for (const outcome of outcomes) if (outcome.ok) count++
  return count
}

async function runIsolatedUnit(conn: SQLiteConnection, unit: GroupUnit, savepoint: string): Promise<UnitAttempt> {
  const notOpened = await execControl(conn, `SAVEPOINT ${savepoint}`)
  if (notOpened) return { outcome: { ok: false, error: notOpened }, contained: false }

  const attempt = await runUnit(conn, unit)
  if (!attempt.ok) return { outcome: attempt, contained: await unwind(conn, savepoint) }

  const notReleased = await execControl(conn, `RELEASE ${savepoint}`)
  if (notReleased) return { outcome: failedUnit(notReleased, attempt.values), contained: false }
  return { outcome: attempt, contained: true }
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
  opening: GroupStatement | undefined,
): Promise<GroupOutcome[]> {
  await rollbackQuietly(conn)
  for (let i = 0; i < units.length; i++) {
    if (i === failed) continue
    outcomes[i] = await runUnitAlone(conn, units[i], opening)
  }
  return outcomes
}

async function runUnitAlone(
  conn: SQLiteConnection,
  unit: GroupUnit,
  opening: GroupStatement | undefined,
): Promise<GroupOutcome> {
  const notBegun = await execControl(conn, 'BEGIN')
  if (notBegun) return { ok: false, error: notBegun }
  const notOpened = await runOpening(conn, opening)
  if (notOpened) {
    await rollbackQuietly(conn)
    return { ok: false, error: notOpened }
  }

  const attempt = await runUnit(conn, unit)
  if (!attempt.ok) {
    await rollbackQuietly(conn)
    return attempt
  }

  const notCommitted = await execControl(conn, 'COMMIT')
  if (!notCommitted) return attempt
  await rollbackQuietly(conn)
  return failedUnit(notCommitted, attempt.values)
}
