import type { SQLiteConnection } from '../driver/types.js'
import type { ChangeTracker } from './change-tracker.js'

const DDL_PREFIX_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

const DROP_TABLE_RE = /^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*;?\s*$/i

export function isCdcRelevantDdl(sql: string): boolean {
  return DDL_PREFIX_RE.test(sql)
}

export function extractDroppedTable(sql: string): string | null {
  const m = DROP_TABLE_RE.exec(sql)
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
