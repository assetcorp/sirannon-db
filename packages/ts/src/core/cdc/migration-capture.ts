import type { SQLiteConnection, SQLiteStatement } from '../driver/types.js'
import { CDC_TRIGGER_PREFIX, CHANGES_TABLE } from '../internal-tables.js'
import { isKeyword, type SqlToken, tokenizeSql } from '../live/sql-tokens.js'
import { ensureReplacedRowsTable, selectCapturedTableNames } from '../system-catalog/index.js'
import { ChangeTracker } from './change-tracker.js'
import { executeAllowingColumnDrop } from './column-drop.js'
import { splitMigrationScript, statementCallsAround } from './migration-statements.js'
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
  conn = migrationSchemaConnection(conn)
  const changesTable = tracker?.changeLogTable ?? CHANGES_TABLE
  const tables = new Set(tracker?.watchedTables ?? [])
  for (const table of await selectCapturedTableNames(conn, CDC_TRIGGER_PREFIX, changesTable)) tables.add(table)
  if (tables.size === 0) return null

  await ensureReplacedRowsTable(conn)
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
  conn = migrationSchemaConnection(conn)
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
  const schema = migrationSchemaConnection(conn)
  const wrapped: SQLiteConnection = {
    ...conn,
    exec: sql => execKeepingCapture(capture, conn, schema, sql),
    prepare: async sql => {
      const stmt = await conn.prepare(sql)
      return changesSchema(tokenizeSql(sql)) ? statementKeepingCapture(capture, schema, sql, stmt) : stmt
    },
    transaction: fn => conn.transaction(txConn => fn(captureKeepingConnection(capture, txConn))),
  }
  if (conn.runBatch) {
    const runBatch = conn.runBatch
    wrapped.runBatch = async (sql, paramsBatch) => {
      if (!changesSchema(tokenizeSql(sql))) return runBatch.call(conn, sql, paramsBatch)
      const stmt = await wrapped.prepare(sql)
      const results = []
      for (const params of paramsBatch) results.push(await stmt.run(...params))
      return results
    }
  }
  return wrapped
}

async function execKeepingCapture(
  capture: ChangeTracker,
  conn: SQLiteConnection,
  schema: SQLiteConnection,
  sql: string,
): Promise<void> {
  if (!SCHEMA_WORD_RE.test(sql)) return conn.exec(sql)
  for (const statement of splitMigrationScript(sql)) {
    await executeAllowingColumnDrop(capture, schema, columnDropSql(statement.text), () => conn.exec(statement.text))
    if (changesSchema(statement.tokens)) {
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
    const result = await executeAllowingColumnDrop(capture, conn, columnDropSql(sql), call)
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
  return identifier(name)
}

function changesSchema(tokens: readonly SqlToken[]): boolean {
  return ['create', 'alter', 'drop'].some(word => isKeyword(tokens[0], word))
}

function identifier(token: SqlToken | undefined): string | undefined {
  if (token?.kind === 'word') return token.value
  if (token?.kind === 'string' && token.value.startsWith("'")) return token.value.slice(1, -1).replaceAll("''", "'")
  return undefined
}

function columnDropSql(sql: string): string {
  const tokens = tokenizeSql(sql)
  if (!isKeyword(tokens[0], 'alter') || !isKeyword(tokens[1], 'table')) return sql
  const qualified = tokens[3]?.kind === 'punct' && tokens[3].value === '.'
  const table = tokens[qualified ? 4 : 2]
  if (table === undefined) return sql
  const name = identifier(table)
  if (name === undefined) return sql
  const schema = qualified ? identifier(tokens[2]) : undefined
  if (qualified && schema === undefined) return sql
  const prefix = schema === undefined ? '' : `"${schema.replaceAll('"', '""')}".`
  return `ALTER TABLE ${prefix}"${name.replaceAll('"', '""')}"${sql.slice(table.end)}`
}

export function migrationSchemaConnection(conn: SQLiteConnection): SQLiteConnection {
  return {
    ...conn,
    prepare: sql =>
      conn.prepare(
        sql
          .replace(/^PRAGMA (table_info|table_xinfo)\(/, 'PRAGMA main.$1(')
          .replace(/(pragma_index_list|pragma_index_xinfo)\(\?\)/, "$1(?, 'main')"),
      ),
    exec: sql => conn.exec(sql.replace(/^(CREATE TRIGGER IF NOT EXISTS |DROP TRIGGER IF EXISTS )(?=")/, '$1main.')),
  }
}
