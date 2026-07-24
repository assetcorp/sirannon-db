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

export interface SnapshotProgress {
  table: string
  tableLoadedRows: number
  tableTotalRows: number
  loadedRows: number
  totalRows: number
}

export interface SnapshotDownloadOptions {
  url: string
  databaseId: string
  headers?: Record<string, string>
  pageSize?: number
  requestTimeoutMs?: number
  onProgress?: (progress: SnapshotProgress) => void
}

export interface SnapshotDownloadResult {
  startSeq: bigint
  epoch: string
  tables: string[]
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
          options.onProgress?.({
            table: table.name,
            tableLoadedRows,
            tableTotalRows: table.rowCount,
            loadedRows,
            totalRows,
          })
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
