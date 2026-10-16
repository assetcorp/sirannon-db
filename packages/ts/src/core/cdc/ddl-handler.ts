import type { SQLiteConnection } from '../driver/types.js'
import { withoutLeadingComments } from '../sql-statement-start.js'
import type { ChangeTracker } from './change-tracker.js'

const DDL_PREFIX_RE = /^(CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX)\b/i

const DROP_TABLE_RE = /^DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*;?\s*$/i

const RENAME_TABLE_RE =
  /^ALTER\s+TABLE\s+"?([A-Za-z_][A-Za-z0-9_]*)"?\s+RENAME\s+TO\s+"?[A-Za-z_][A-Za-z0-9_]*"?\s*;?\s*$/i

/**
 * Returns whether a statement can change the columns or unique keys of a watched table or remove it, so that the change-capture triggers have to be checked after it.
 *
 * @param sql - The statement, which can open with whitespace and comments.
 * @returns `true` for `CREATE TABLE`, every form of `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, `CREATE UNIQUE INDEX`, and `DROP INDEX`.
 */
export function isCdcRelevantDdl(sql: string): boolean {
  return DDL_PREFIX_RE.test(withoutLeadingComments(sql))
}

/**
 * Returns the name of the table that a statement removes from the schema, either by dropping it or by renaming it to another name.
 *
 * @param sql - The statement, which can open with whitespace and comments.
 * @returns The table that no longer exists under that name after the statement, or `null` for every other statement.
 */
export function extractDroppedTable(sql: string): string | null {
  const start = withoutLeadingComments(sql)
  const m = DROP_TABLE_RE.exec(start) ?? RENAME_TABLE_RE.exec(start)
  return m?.[1] ?? null
}

/**
 * Brings the change capture of a database up to date after a schema change: Sirannon reinstalls the triggers of each
 * watched table whose columns or keys changed, and stops watching a table that the statement dropped or renamed.
 *
 * @param tracker - The change tracker of the database.
 * @param writerConn - The writer connection on which the schema change committed.
 * @param sql - The schema change.
 * @returns A promise that settles once the triggers and the watched tables match the new schema.
 */
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

/**
 * Brings the change capture of a database up to date as {@link applyDdlSideEffects} does, but only after a statement
 * that {@link isCdcRelevantDdl} accepts, and only on a database that has a change tracker.
 *
 * @param tracker - The change tracker of the database, or `null` when it watches nothing.
 * @param writerConn - The writer connection on which the statement committed.
 * @param sql - The statement.
 * @returns A promise that settles once the change capture matches the schema, at once for every other statement.
 */
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
