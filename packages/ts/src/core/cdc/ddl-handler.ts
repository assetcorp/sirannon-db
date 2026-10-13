import type { SQLiteConnection } from '../driver/types.js'
import type { ChangeTracker } from './change-tracker.js'

const LEADING_COMMENTS_RE = /^(?:\s|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/)*/

const DDL_PREFIX_RE = /^(CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX)\b/i

const DROP_TABLE_RE = /^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*;?\s*$/i

const RENAME_TABLE_RE =
  /^ALTER\s+TABLE\s+"?([A-Za-z_][A-Za-z0-9_]*)"?\s+RENAME\s+TO\s+"?[A-Za-z_][A-Za-z0-9_]*"?\s*;?\s*$/i

function statementStart(sql: string): string {
  return sql.replace(LEADING_COMMENTS_RE, '')
}

/**
 * Returns whether a statement can change the columns or unique keys of a watched table or remove it, so that the change-capture triggers have to be checked after it.
 *
 * @param sql - The statement, which can open with whitespace and comments.
 * @returns `true` for `CREATE TABLE`, every form of `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, `CREATE UNIQUE INDEX`, and `DROP INDEX`.
 */
export function isCdcRelevantDdl(sql: string): boolean {
  return DDL_PREFIX_RE.test(statementStart(sql))
}

/**
 * Returns the name of the table that a statement removes from the schema, either by dropping it or by renaming it to another name.
 *
 * @param sql - The statement, which can open with whitespace and comments.
 * @returns The table that no longer exists under that name after the statement, or `null` for every other statement.
 */
export function extractDroppedTable(sql: string): string | null {
  const start = statementStart(sql)
  const m = DROP_TABLE_RE.exec(start) ?? RENAME_TABLE_RE.exec(start)
  return m?.[1] ?? null
}

export async function applyDdlSideEffects(
  tracker: ChangeTracker,
  writerConn: SQLiteConnection,
  sql: string,
): Promise<void> {
  if (tracker.watchedTables.size === 0) {
    return
  }

  await tracker.refreshAllTriggersUsingConnection(writerConn)

  const dropped = extractDroppedTable(sql)
  if (dropped !== null) {
    await tracker.pruneDroppedTables(writerConn, [dropped])
  }
}

export function applyDdlSideEffectsIfRelevant(
  tracker: ChangeTracker | null,
  writerConn: SQLiteConnection,
  sql: string,
): Promise<void> {
  if (!tracker || !isCdcRelevantDdl(sql)) {
    return Promise.resolve()
  }
  return applyDdlSideEffects(tracker, writerConn, sql)
}
