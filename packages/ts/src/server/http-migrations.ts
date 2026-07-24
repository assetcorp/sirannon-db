import { migrationChecksum } from '../core/migrations/checksum.js'
import type { Sirannon } from '../core/sirannon.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import { parseBody, sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbRouteHandler } from './http-handler.js'
import { schemaVersionValidationError } from './sync-protocol.js'

export interface MigrationListRequest {
  after?: number
}

export interface MigrationListEntry {
  version: number
  name: string
  checksum: string | null
  up?: string
}

export interface MigrationListResponse {
  serverVersion: number
  migrations: MigrationListEntry[]
}

export function handleMigrationList(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<MigrationListRequest>(res, rawBody)
    if (!body) return

    const afterError = schemaVersionValidationError(body.after)
    if (afterError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', afterError.replace('"schemaVersion"', '"after"'))
      return
    }
    const after = body.after ?? 0

    let database: Awaited<ReturnType<Sirannon['resolve']>>
    try {
      database = await sirannon.resolve(dbId)
    } catch (err) {
      sendCaughtError(res, abort, err)
      return
    }
    if (abort.aborted) return
    if (!database) {
      sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${dbId}' not found`)
      return
    }

    try {
      const applied = await database.appliedMigrations()
      const registry = new Map((await sirannon.registryMigrations()).map(migration => [migration.version, migration]))

      const migrations: MigrationListEntry[] = applied.map(row => {
        const entry: MigrationListEntry = { version: row.version, name: row.name, checksum: row.checksum }
        if (row.version > after && row.checksum !== null) {
          const source = registry.get(row.version)
          if (source !== undefined && typeof source.up === 'string' && migrationChecksum(source.up) === row.checksum) {
            entry.up = source.up
          }
        }
        return entry
      })

      if (abort.aborted) return
      const response: MigrationListResponse = { serverVersion: highestMigrationVersion(applied), migrations }
      sendJson(res, response)
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}
