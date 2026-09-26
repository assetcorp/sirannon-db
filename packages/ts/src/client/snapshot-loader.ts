import { invokeCallerCallback } from '../core/caller-callbacks.js'
import { decodeTaggedValues } from '../core/cdc/encoding.js'
import type { DeviceSyncPort } from '../core/database-sync.js'
import { canonicaliseForChecksum } from '../core/sync/canonicalise.js'
import { sha256Hex } from '../core/sync/sha256.js'
import { IDENTIFIER_RE, SEQ_STRING_RE } from '../core/sync/validators.js'
import type { SnapshotManifestResponse, SnapshotPageResponse } from '../server/snapshot-protocol.js'
import { toBaseUrl } from './endpoint-urls.js'
import { DEFAULT_HTTP_REQUEST_TIMEOUT_MS, postJson } from './http-json.js'
import { RemoteError } from './types.js'

const DEFAULT_SNAPSHOT_PAGE_ROWS = 500

/**
 * The progress of a snapshot download, for the current table and across every table.
 *
 * @public
 */
export interface SnapshotProgress {
  /** The table that `downloadDatabaseSnapshot` is copying now. */
  table: string
  /** The number of that table's rows in the local database so far. */
  tableLoadedRows: number
  /** The total number of rows in that table, from the server's manifest. */
  tableTotalRows: number
  /** The number of rows in the local database so far, across every table. */
  loadedRows: number
  /** The total number of rows in the snapshot. */
  totalRows: number
}

/**
 * The server and database that a snapshot comes from, and the settings for reading it.
 *
 * @public
 */
export interface SnapshotDownloadOptions {
  /** The address of the server that serves the snapshot. */
  url: string
  /** The identifier of the database to copy. */
  databaseId: string
  /** Headers that `downloadDatabaseSnapshot` adds to each snapshot request. */
  headers?: Record<string, string>
  /** The number of rows to request per page. Defaults to 500. */
  pageSize?: number
  /** The time limit, in milliseconds, for each request. */
  requestTimeoutMs?: number
  /** Called after `downloadDatabaseSnapshot` loads each page that contains rows. */
  onProgress?: (progress: SnapshotProgress) => void
}

/**
 * The result of one snapshot download.
 *
 * @public
 */
export interface SnapshotDownloadResult {
  /** The change-log position from which the device resumes its subscription. */
  startSeq: bigint
  /** The epoch that identifies the sequence space of that position. */
  epoch: string
  /** The tables in the snapshot. */
  tables: string[]
  /** The number of rows that `downloadDatabaseSnapshot` wrote. */
  loadedRows: number
}

function validateManifest(raw: unknown): SnapshotManifestResponse {
  const record = raw as Partial<SnapshotManifestResponse> | null
  if (
    record === null ||
    typeof record !== 'object' ||
    typeof record.startSeq !== 'string' ||
    !SEQ_STRING_RE.test(record.startSeq) ||
    typeof record.epoch !== 'string' ||
    record.epoch.length === 0 ||
    !Array.isArray(record.schema) ||
    record.schema.some(ddl => typeof ddl !== 'string') ||
    !Array.isArray(record.tables) ||
    !Array.isArray(record.migrations)
  ) {
    throw new RemoteError('INVALID_RESPONSE', 'Snapshot manifest is malformed')
  }
  for (const migration of record.migrations) {
    if (
      typeof migration !== 'object' ||
      migration === null ||
      !Number.isSafeInteger((migration as { version?: unknown }).version) ||
      typeof (migration as { name?: unknown }).name !== 'string' ||
      ((migration as { checksum?: unknown }).checksum !== null &&
        typeof (migration as { checksum?: unknown }).checksum !== 'string')
    ) {
      throw new RemoteError('INVALID_RESPONSE', 'Snapshot manifest migration entry is malformed')
    }
  }
  for (const table of record.tables) {
    if (
      typeof table !== 'object' ||
      table === null ||
      !IDENTIFIER_RE.test(String((table as { name?: unknown }).name)) ||
      typeof (table as { rowCount?: unknown }).rowCount !== 'number'
    ) {
      throw new RemoteError('INVALID_RESPONSE', 'Snapshot manifest table entry is malformed')
    }
  }
  return record as SnapshotManifestResponse
}

function validatePage(raw: unknown): SnapshotPageResponse {
  const record = raw as Partial<SnapshotPageResponse> | null
  if (
    record === null ||
    typeof record !== 'object' ||
    !Array.isArray(record.rows) ||
    typeof record.checksum !== 'string' ||
    typeof record.done !== 'boolean' ||
    (record.nextKey !== null && !Array.isArray(record.nextKey))
  ) {
    throw new RemoteError('INVALID_RESPONSE', 'Snapshot page is malformed')
  }
  return record as SnapshotPageResponse
}

/**
 * Returns whether the local database accepts reads and writes after a snapshot download fails. A failure before the
 * wipe begins leaves the database intact, while a failure after it leaves every statement failing with
 * `SNAPSHOT_IN_PROGRESS` until a later download succeeds.
 *
 * @param port - The local database that the download wrote into.
 * @returns True when the database accepts statements.
 *
 * @internal
 */
export async function snapshotGateOpen(port: DeviceSyncPort): Promise<boolean> {
  try {
    return !(await port.snapshotLoadPending())
  } catch {
    return false
  }
}

/**
 * Copies a database from a server into a local database, and replaces every local table that the snapshot contains.
 *
 * @param port - The local database to write the snapshot into.
 * @param options - The server and database that the snapshot comes from, and the settings for reading it.
 * @returns The change-log position and epoch to resume from, the tables that this function copied, and the number of rows that it wrote.
 *
 * @public
 */
export async function downloadDatabaseSnapshot(
  port: DeviceSyncPort,
  options: SnapshotDownloadOptions,
): Promise<SnapshotDownloadResult> {
  const baseUrl = toBaseUrl(options.url)
  const encodedId = encodeURIComponent(options.databaseId)
  const pageSize = options.pageSize ?? DEFAULT_SNAPSHOT_PAGE_ROWS
  const timeoutMs = options.requestTimeoutMs ?? DEFAULT_HTTP_REQUEST_TIMEOUT_MS

  const manifest = validateManifest(
    await postJson(`${baseUrl}/db/${encodedId}/snapshot`, {}, options.headers, timeoutMs),
  )
  const tables = manifest.tables.map(table => table.name)
  const totalRows = manifest.tables.reduce((sum, table) => sum + table.rowCount, 0)

  await port.beginSnapshotLoad(tables)
  let loadedRows = 0
  try {
    await port.applySnapshotSchema(manifest.schema)

    for (const table of manifest.tables) {
      let afterKey: unknown[] | undefined
      let tableLoadedRows = 0
      for (;;) {
        const page = validatePage(
          await postJson(
            `${baseUrl}/db/${encodedId}/snapshot/page`,
            { table: table.name, ...(afterKey !== undefined ? { afterKey } : {}), limit: pageSize },
            options.headers,
            timeoutMs,
          ),
        )

        if (page.rows.length > 0) {
          const rows = decodeTaggedValues(page.rows) as Record<string, unknown>[]
          if (sha256Hex(canonicaliseForChecksum(rows)) !== page.checksum) {
            throw new RemoteError(
              'SNAPSHOT_CHECKSUM_MISMATCH',
              `Snapshot page checksum mismatch for table '${table.name}'`,
            )
          }
          await port.loadSnapshotPage(table.name, rows)
          tableLoadedRows += rows.length
          loadedRows += rows.length
          const progress = {
            table: table.name,
            tableLoadedRows,
            tableTotalRows: table.rowCount,
            loadedRows,
            totalRows,
          }
          invokeCallerCallback(() => options.onProgress?.(progress))
        }

        if (page.done) break
        if (page.nextKey === null) {
          throw new RemoteError('INVALID_RESPONSE', `Snapshot page for table '${table.name}' has no resume key`)
        }
        afterKey = page.nextKey
      }
    }

    await port.replaceMigrationHistory(manifest.migrations)
    const startSeq = BigInt(manifest.startSeq)
    await port.setPullState(startSeq, manifest.epoch)
    await port.endSnapshotLoad(tables)
    return { startSeq, epoch: manifest.epoch, tables, loadedRows }
  } catch (err) {
    try {
      await port.abortSnapshotLoad()
    } catch {}
    throw err
  }
}
