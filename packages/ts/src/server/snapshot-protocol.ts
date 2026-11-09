import { INTERNAL_TABLE_PREFIX } from '../core/internal-tables.js'
import { IDENTIFIER_RE } from '../core/sync/validators.js'

export const SNAPSHOT_MAX_PAGE_ROWS = 1000
export const SNAPSHOT_DEFAULT_PAGE_ROWS = 500
export const SNAPSHOT_PAGE_BYTE_CAP = 8 * 1024 * 1024

/**
 * The body of `POST /db/{id}/snapshot`, with everything that a device needs to copy the database.
 *
 * @public
 */
export interface SnapshotManifestResponse {
  /** The identifier of the database that the snapshot copies. */
  databaseId: string
  /** The change sequence from which a device subscribes once it has copied every page, as a decimal string. */
  startSeq: string
  /** The identifier of the database's change sequence space. */
  epoch: string
  /** The statements that create the database's tables, in foreign key order, followed by its other schema objects such as indexes. */
  schema: string[]
  /** Each table to copy, in foreign key order, with its row count. */
  tables: SnapshotManifestTable[]
  /** The migrations that the database has applied. */
  migrations: SnapshotManifestMigration[]
}

/**
 * One table that a snapshot copies.
 *
 * @public
 */
export interface SnapshotManifestTable {
  /** The name of the table. */
  name: string
  /** How many rows the table held when the server wrote the manifest. */
  rowCount: number
}

/**
 * One migration that the database has applied.
 *
 * @public
 */
export interface SnapshotManifestMigration {
  /** The version of the migration. */
  version: number
  /** The name of the migration. */
  name: string
  /** The checksum of the migration's statements, or null when the database recorded none. */
  checksum: string | null
}

/**
 * The body of `POST /db/{id}/snapshot/page`, the request for one page of a table's rows.
 *
 * @public
 */
export interface SnapshotPageRequest {
  /** The table to read. */
  table: string
  /** The key of the last row of the previous page, which the server reads past. Leave it out for the first page. */
  afterKey?: unknown[]
  /** The most rows to return, from 1 to 1000, which is 500 when you leave it out. */
  limit?: number
}

/**
 * One page of a table's rows, with each column value in its tagged wire form.
 *
 * @public
 */
export interface SnapshotPageResponse {
  /** The rows of this page. */
  rows: unknown[]
  /** The checksum over the rows of this page. */
  checksum: string
  /** Whether this is the table's last page. */
  done: boolean
  /** The key to send as `afterKey` for the next page, or null on the last page. */
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
