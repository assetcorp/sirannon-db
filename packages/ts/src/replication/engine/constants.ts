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

export { extractDroppedTable } from '../../core/sync/validators.js'

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
