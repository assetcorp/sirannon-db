import { INTERNAL_TABLE_PREFIX } from '../core/internal-tables.js'
import { IDENTIFIER_RE } from '../core/sync/validators.js'

export const SNAPSHOT_MAX_PAGE_ROWS = 1000
export const SNAPSHOT_DEFAULT_PAGE_ROWS = 500
export const SNAPSHOT_PAGE_BYTE_CAP = 8 * 1024 * 1024

export interface SnapshotManifestResponse {
  databaseId: string
  startSeq: string
  epoch: string
  schema: string[]
  tables: { name: string; rowCount: number }[]
}

export interface SnapshotPageRequest {
  table: string
  afterKey?: unknown[]
  limit?: number
}

export interface SnapshotPageResponse {
  rows: unknown[]
  checksum: string
  done: boolean
  nextKey: unknown[] | null
}

export function isDumpableTableName(candidate: unknown): candidate is string {
  return (
    typeof candidate === 'string' &&
    IDENTIFIER_RE.test(candidate) &&
    !candidate.startsWith(INTERNAL_TABLE_PREFIX) &&
    !candidate.toLowerCase().startsWith('sqlite_')
  )
}

export function snapshotPageValidationError(raw: unknown): string | null {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    return 'Request body must be an object'
  }
  const record = raw as Record<string, unknown>
  if (!isDumpableTableName(record.table)) {
    return 'Field "table" is required and must be a dumpable table name'
  }
  if (record.afterKey !== undefined) {
    if (!Array.isArray(record.afterKey) || record.afterKey.length === 0 || record.afterKey.length > 16) {
      return '"afterKey" must be a non-empty array of at most 16 key values'
    }
  }
  if (record.limit !== undefined) {
    if (typeof record.limit !== 'number' || !Number.isInteger(record.limit) || record.limit < 1) {
      return '"limit" must be a positive integer'
    }
    if (record.limit > SNAPSHOT_MAX_PAGE_ROWS) {
      return `"limit" must not exceed ${SNAPSHOT_MAX_PAGE_ROWS}`
    }
  }
  return null
}
