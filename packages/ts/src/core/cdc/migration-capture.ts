import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { CDC_TRIGGER_PREFIX, CHANGES_TABLE } from '../internal-tables.js'
import { isKeyword, type SqlToken, tokenizeSql } from '../live/sql-tokens.js'
import { selectCapturedTableNames } from '../system-catalog/index.js'
import { ChangeTracker } from './change-tracker.js'
import { columnDropAwareConnection, splitScript, statementCallsAround } from './column-drop.js'
import { isCdcRelevantDdl } from './ddl-handler.js'
import { cdcTriggersMatch, dropCdcTriggers, installCdcTriggers } from './trigger-sql.js'

const SCHEMA_WORD_RE = /\b(?:CREATE|ALTER|DROP)\b/i

/**
 * Returns a change tracker that covers, for the length of one migration or rollback, every table whose writes reach
 * the change log: each table that `tracker` watches, and each table that carries change-capture triggers from another
 * process or an earlier session.
 *
 * The returned tracker belongs to the migration alone, so the database's own tracker keeps its watched set.
 *
 * @param conn - The writer connection.
 * @param tracker - The database's change tracker, or `null` when this process has watched nothing.
 * @returns The tracker, or `null` when no table's writes reach the change log.
 * @internal
 */
export async function migrationCaptureTracker(
  conn: SQLiteConnection,
  tracker: ChangeTracker | null,
): Promise<ChangeTracker | null> {
  const changesTable = tracker?.changeLogTable ?? CHANGES_TABLE
  const tables = new Set(tracker?.watchedTables ?? [])
  for (const table of await selectCapturedTableNames(conn, CDC_TRIGGER_PREFIX, changesTable)) tables.add(table)
  if (tables.size === 0) return null

  const capture = new ChangeTracker({ changesTable })
  for (const table of tables) await capture.adoptTable(conn, table)
  return capture.watchedTables.size === 0 ? null : capture
}

/**
 * Puts back the change-capture triggers of every table that `capture` covers wherever a schema change removed them
 * or left them out of date, and drops the triggers of a covered table that no longer exists under its name, so that
 * a renamed table stops recording its writes under the old name.
 *
 * @param capture - The tracker from {@link migrationCaptureTracker}.
 * @param conn - The writer connection inside the migration's transaction.
 * @param created - A table that the last statement created, whose triggers wait for the next schema change or the
 * end of the migration, so that the change log leaves out the rows that the migration copies into it.
 * @returns A promise that settles once every covered table carries matching triggers.
 * @internal
 */
export async function restoreMigrationCapture(
  capture: ChangeTracker,
  conn: SQLiteConnection,
  created?: string,
): Promise<void> {
  const changesTable = capture.changeLogTable
  const waiting = created?.toLowerCase()
  for (const table of capture.watchedTables) {
    const shape = await capture.adoptTable(conn, table)
    if (shape.columns.length === 0) {
      await dropCdcTriggers(conn, table)
      continue
    }
    if (table.toLowerCase() === waiting) continue
    if (await cdcTriggersMatch(conn, changesTable, table, shape)) continue
    await dropCdcTriggers(conn, table)
    await installCdcTriggers(conn, changesTable, table, shape)
  }
}

/**
 * Returns a view of the writer connection for a migration or a rollback, which keeps change capture on every table
 * that `capture` covers.
 *
 * A script that changes the schema runs one statement at a time. Before a statement drops a column of a covered
 * table, the view lifts that table's triggers as {@link columnDropAwareConnection} does, and after each statement
 * that changes the schema, it calls {@link restoreMigrationCapture}, so that every later write in the same migration
 * reaches the change log. A script without a schema change reaches SQLite in one call, unchanged.
 *
 * @param capture - The tracker from {@link migrationCaptureTracker}.
 * @param conn - The writer connection.
 * @returns A connection whose statements, scripts, and transactions all pass through `conn`.
 * @internal
 */
export function captureKeepingConnection(capture: ChangeTracker, conn: SQLiteConnection): SQLiteConnection {
  const aware = columnDropAwareConnection(capture, conn)
  return {
    ...aware,
    exec: sql => execKeepingCapture(capture, conn, aware, sql),
    prepare: async sql => {
      const stmt = await aware.prepare(sql)
      return isCdcRelevantDdl(sql) ? statementKeepingCapture(capture, conn, sql, stmt) : stmt
    },
    transaction: fn => conn.transaction(txConn => fn(captureKeepingConnection(capture, txConn))),
  }
}

async function execKeepingCapture(
  capture: ChangeTracker,
  conn: SQLiteConnection,
  aware: SQLiteConnection,
  sql: string,
): Promise<void> {
  if (!SCHEMA_WORD_RE.test(sql)) return aware.exec(sql)
  for (const statement of splitScript(sql)) {
    await aware.exec(statement.text)
    if (isCdcRelevantDdl(statement.text)) {
      await restoreMigrationCapture(capture, conn, createdTable(statement.tokens))
    }
  }
}

function statementKeepingCapture(
  capture: ChangeTracker,
  conn: SQLiteConnection,
  sql: string,
  stmt: SQLiteStatement,
): SQLiteStatement {
  const created = createdTable(tokenizeSql(sql))
  return statementCallsAround(stmt, async call => {
    const result = await call()
    await restoreMigrationCapture(capture, conn, created)
    return result
  })
}

function createdTable(tokens: readonly SqlToken[]): string | undefined {
  if (!isKeyword(tokens[0], 'create') || !isKeyword(tokens[1], 'table')) return undefined
  let at = 2
  if (isKeyword(tokens[at], 'if') && isKeyword(tokens[at + 1], 'not') && isKeyword(tokens[at + 2], 'exists')) at += 3
  const qualified = tokens[at + 1]?.kind === 'punct' && tokens[at + 1]?.value === '.'
  if (qualified && tokens[at]?.lower !== 'main') return undefined
  const name = tokens[qualified ? at + 2 : at]
  return name?.kind === 'word' ? name.value : undefined
}
