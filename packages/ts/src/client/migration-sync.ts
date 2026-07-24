import type { Database } from '../core/database.js'
import { MigrationError } from '../core/errors.js'
import { migrationChecksum } from '../core/migrations/checksum.js'
import { MIGRATION_NAME_RE, type Migration } from '../core/migrations/types.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import type { MigrationListEntry, MigrationListRequest, MigrationListResponse } from '../server/http-migrations.js'
import { isValidSchemaVersion } from '../server/sync-protocol.js'
import { postJson } from './http-json.js'
import { RemoteError } from './types.js'

export interface MigrationSyncOptions {
  url: string
  databaseId: string
  headers?: Record<string, string>
  requestTimeoutMs?: number
}

export type MigrationSyncStatus = 'in-sync' | 'migrated' | 'ahead' | 'resync-required'

export interface MigrationSyncResult {
  status: MigrationSyncStatus
  schemaVersion: number
  serverVersion: number
}

function validateMigrationList(raw: unknown): MigrationListResponse {
  const record = raw as Partial<MigrationListResponse> | null
  if (
    record === null ||
    typeof record !== 'object' ||
    !isValidSchemaVersion(record.serverVersion) ||
    !Array.isArray(record.migrations)
  ) {
    throw new RemoteError('INVALID_RESPONSE', 'Migration list response is malformed')
  }
  for (const entry of record.migrations) {
    const candidate = entry as Partial<MigrationListEntry> | null
    if (
      candidate === null ||
      typeof candidate !== 'object' ||
      !isValidSchemaVersion(candidate.version) ||
      candidate.version === 0 ||
      typeof candidate.name !== 'string' ||
      !MIGRATION_NAME_RE.test(candidate.name) ||
      (candidate.checksum !== null && typeof candidate.checksum !== 'string') ||
      (candidate.up !== undefined && typeof candidate.up !== 'string')
    ) {
      throw new RemoteError('INVALID_RESPONSE', 'Migration list entry is malformed')
    }
  }
  return record as MigrationListResponse
}

export async function fetchServerMigrations(
  options: MigrationSyncOptions,
  after: number,
): Promise<MigrationListResponse> {
  const url = `${options.url}/db/${encodeURIComponent(options.databaseId)}/migrations`
  const body: MigrationListRequest = { after }
  return validateMigrationList(await postJson(url, body, options.headers, options.requestTimeoutMs))
}

function historiesDiverge(
  local: readonly { version: number; checksum: string | null }[],
  server: readonly MigrationListEntry[],
  localVersion: number,
  serverVersion: number,
): boolean {
  const serverByVersion = new Map(server.map(entry => [entry.version, entry]))
  for (const row of local) {
    if (row.version > serverVersion) continue
    const counterpart = serverByVersion.get(row.version)
    if (counterpart === undefined) return true
    if (row.checksum !== null && counterpart.checksum !== null && row.checksum !== counterpart.checksum) {
      return true
    }
  }
  const localVersions = new Set(local.map(row => row.version))
  for (const entry of server) {
    if (entry.version <= localVersion && !localVersions.has(entry.version)) return true
  }
  return false
}

function toPendingMigrations(server: readonly MigrationListEntry[], localVersion: number): Migration[] | null {
  const pending: Migration[] = []
  for (const entry of server) {
    if (entry.version <= localVersion) continue
    if (entry.up === undefined || entry.checksum === null) return null
    if (migrationChecksum(entry.up) !== entry.checksum) return null
    pending.push({ version: entry.version, name: entry.name, up: entry.up })
  }
  return pending
}

export async function syncDeviceMigrations(db: Database, options: MigrationSyncOptions): Promise<MigrationSyncResult> {
  const local = await db.appliedMigrations()
  const localVersion = highestMigrationVersion(local)
  const list = await fetchServerMigrations(options, localVersion)

  if (historiesDiverge(local, list.migrations, localVersion, list.serverVersion)) {
    return { status: 'resync-required', schemaVersion: localVersion, serverVersion: list.serverVersion }
  }
  if (localVersion > list.serverVersion) {
    return { status: 'ahead', schemaVersion: localVersion, serverVersion: list.serverVersion }
  }
  if (localVersion === list.serverVersion) {
    return { status: 'in-sync', schemaVersion: localVersion, serverVersion: list.serverVersion }
  }

  const pending = toPendingMigrations(list.migrations, localVersion)
  if (pending === null || pending.length === 0) {
    return { status: 'resync-required', schemaVersion: localVersion, serverVersion: list.serverVersion }
  }

  try {
    await db.migrate(pending)
  } catch (err) {
    if (
      err instanceof MigrationError &&
      (err.code === 'MIGRATION_CHECKSUM_MISMATCH' || err.code === 'MIGRATION_BASELINE_GAP')
    ) {
      return { status: 'resync-required', schemaVersion: localVersion, serverVersion: list.serverVersion }
    }
    throw err
  }

  const schemaVersion = highestMigrationVersion(await db.appliedMigrations())
  return {
    status: schemaVersion === list.serverVersion ? 'migrated' : 'resync-required',
    schemaVersion,
    serverVersion: list.serverVersion,
  }
}
