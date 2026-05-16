export const DEFAULT_BATCH_SIZE = 100
export const DEFAULT_BATCH_INTERVAL_MS = 100
export const DEFAULT_MAX_CLOCK_DRIFT_MS = 60_000
export const DEFAULT_MAX_PENDING_BATCHES = 10
export const DEFAULT_MAX_BATCH_CHANGES = 1000
export const DEFAULT_ACK_TIMEOUT_MS = 5_000
export const DEFAULT_SYNC_BATCH_SIZE = 10_000
export const DEFAULT_MAX_CONCURRENT_SYNCS = 2
export const DEFAULT_MAX_SYNC_DURATION_MS = 1_800_000
export const DEFAULT_MAX_SYNC_LAG_BEFORE_READY = 100
export const DEFAULT_SYNC_ACK_TIMEOUT_MS = 30_000
export const DEFAULT_CATCH_UP_DEADLINE_MS = 600_000

export const DDL_PREFIX_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

export const SAFE_SQL_PREFIX_RE =
  /^\s*(INSERT|UPDATE|DELETE|SELECT|CREATE\s+TABLE|ALTER\s+TABLE|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

export const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export const DDL_DENY_RE = /\b(load_extension|ATTACH|randomblob|zeroblob|writefile|readfile|fts3_tokenizer)\b/i

const DROP_TABLE_RE = /^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?\s*;?\s*$/i

/**
 * Extracts the target table name from a `DROP TABLE` statement, or returns
 * `null` if the SQL is not a `DROP TABLE` (or the name does not match the
 * project's identifier rules).
 *
 * Used by the local executor and the batch applier to record which watched
 * tables have been dropped during a transaction so the `ChangeTracker` map
 * can be pruned after the transaction commits. The SQL passed in is already
 * validated by upstream guards: no embedded semicolons, allow-listed DDL
 * shapes, no `load_extension` or other denied functions.
 *
 * The regex deliberately rejects any DROP TABLE form that contains trailing
 * tokens beyond an optional terminating semicolon (e.g. unexpected schema
 * qualifiers, comments, or stray operators). When a future SQLite version
 * adds new DROP TABLE syntax this function returns `null` for the new form,
 * which is the safe fallback: the watched entry stays put and the operator
 * can resolve it manually.
 */
export function extractDroppedTable(sql: string): string | null {
  const m = DROP_TABLE_RE.exec(sql)
  return m?.[1] ?? null
}

const SYNC_SAFE_DDL_PREFIX_RE =
  /^\s*(CREATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+ADD\s+COLUMN|DROP\s+TABLE|CREATE\s+INDEX|DROP\s+INDEX)\b/i

export function isSyncSafeDdl(sql: string): boolean {
  if (!SYNC_SAFE_DDL_PREFIX_RE.test(sql)) return false
  if (sql.includes(';')) return false
  if (/\bAS\s+SELECT\b/i.test(sql)) return false
  if (DDL_DENY_RE.test(sql)) return false
  const body = sql.replace(SYNC_SAFE_DDL_PREFIX_RE, '')
  if (/\bSELECT\b/i.test(body)) return false
  return true
}
