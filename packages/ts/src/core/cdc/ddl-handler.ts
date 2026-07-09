import type { SQLiteConnection } from '../driver/types.js'
import type { ChangeTracker } from './change-tracker.js'

/**
 * Recognises DDL statements whose execution may invalidate cached column
 * metadata or remove a watched table entirely.
 *
 * The pattern is intentionally identical in shape to the replication
 * engine's DDL prefix regex so behaviour stays predictable across builds.
 * It is duplicated here (rather than imported) because the non-replicated
 * build must remain tree-shakable; `core/` is forbidden from depending on
 * `replication/`.
 */
const DDL_PREFIX_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

const DROP_TABLE_RE = /^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*;?\s*$/i

/**
 * Returns `true` when the SQL begins with a DDL keyword whose execution
 * could affect CDC trigger correctness. The check is a single regex test
 * against the leading tokens; no parser allocations.
 */
export function isCdcRelevantDdl(sql: string): boolean {
  return DDL_PREFIX_RE.test(sql)
}

/**
 * Extracts the target table name from a `DROP TABLE` statement, or returns
 * `null` if the SQL is not a `DROP TABLE` (or the name does not match the
 * project's identifier rules). Used by the post-commit pruning hook to feed
 * `ChangeTracker.pruneDroppedTables`.
 *
 * The regex deliberately rejects any `DROP TABLE` form that carries
 * unexpected trailing tokens (schema qualifiers, comments, stray
 * operators); when SQLite extends its grammar this function falls back to
 * `null`, which is the safe behaviour. The watched entry stays put and the
 * caller can issue an explicit `unwatch` to clean it up.
 */
export function extractDroppedTable(sql: string): string | null {
  const m = DROP_TABLE_RE.exec(sql)
  return m?.[1] ?? null
}

/**
 * Reacts to a DDL statement that has already executed and committed on
 * `writerConn`, keeping the supplied `ChangeTracker` in sync:
 *
 *   1. Reinstalls CDC triggers on every watched table so subsequent
 *      `INSERT`/`UPDATE`/`DELETE` statements capture the current column
 *      list. Tables whose column list is unchanged are skipped.
 *   2. If the SQL was a `DROP TABLE` against a watched table, removes the
 *      watched entry and drops any residual triggers.
 *
 * Callers must invoke this only after the DDL has committed. On rollback
 * the writer pool's transaction wrapper threw and this function is never
 * reached, so the watched map stays consistent with the on-disk schema.
 *
 * The function performs no work and produces no allocations when the
 * tracker has no watched tables.
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
