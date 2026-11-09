import { migrationChecksum } from '../core/migrations/checksum.js'
import type { Sirannon } from '../core/sirannon.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import { parseBody, sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbRouteHandler } from './http-handler.js'
import { schemaVersionValidationError } from './sync-protocol.js'

/**
 * The body of `POST /db/{id}/migrations`, which lists the migrations that the database has applied.
 *
 * @public
 */
export interface MigrationListRequest {
  /** The schema version of the device, so that the server sends the statements of every later migration. */
  after?: number
}

/**
 * One migration that the database has applied.
 *
 * @public
 */
export interface MigrationListEntry {
  /** The version of the migration. */
  version: number
  /** The name of the migration. */
  name: string
  /** The checksum of the migration's statements, or null when the database recorded none. */
  checksum: string | null
  /** The statements of a migration later than `after`, which the server sends when it holds them. */
  up?: string
}

/**
 * The migrations that the database has applied.
 *
 * @public
 */
export interface MigrationListResponse {
  /** The schema version of the server's database. */
  serverVersion: number
  /** Every migration that the database has applied, in version order. */
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
