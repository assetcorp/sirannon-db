import type { HttpResponse } from 'uWebSockets.js'
import { createHash } from 'node:crypto'
import { decodeTaggedValues, encodeTaggedValues, encodeWireRowsInPlace } from '../core/cdc/encoding.js'
import { ensureCdcEpoch } from '../core/cdc/epoch.js'
import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import { CHANGES_TABLE } from '../core/internal-tables.js'
import type { Sirannon } from '../core/sirannon.js'
import { canonicaliseForChecksum } from '../core/sync/canonicalise.js'
import { dumpTablePages } from '../core/sync/dump.js'
import { PkResolver } from '../core/sync/pk.js'
import { dumpSchema, tablesInFkOrder } from '../core/sync/schema-dump.js'
import { countTableRows, maxChangeSeq, tableExists } from '../core/system-catalog/index.js'
import type { ResponseAbort } from './http-common.js'
import { parseBody, sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbRouteHandler } from './http-handler.js'
import type { SnapshotManifestResponse, SnapshotPageRequest, SnapshotPageResponse } from './snapshot-protocol.js'
import { SNAPSHOT_DEFAULT_PAGE_ROWS, SNAPSHOT_PAGE_BYTE_CAP, snapshotPageValidationError } from './snapshot-protocol.js'

async function resolveSnapshotDatabase(
  res: HttpResponse,
  abort: ResponseAbort,
  sirannon: Sirannon,
  dbId: string,
): Promise<Database | null> {
  let database: Database | undefined
  try {
    database = await sirannon.resolve(dbId)
  } catch (err) {
    if (abort.aborted) return null
    sendCaughtError(res, abort, err)
    return null
  }
  if (abort.aborted) return null
  if (!database) {
    sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${dbId}' not found`)
    return null
  }
  if (database.path === ':memory:') {
    sendError(res, 400, 'SNAPSHOT_UNSUPPORTED', 'Snapshots require file-based databases')
    return null
  }
  return database
}

async function changeLogStartSeq(conn: SQLiteConnection): Promise<bigint> {
  if (!(await tableExists(conn, CHANGES_TABLE))) return 0n
  return maxChangeSeq(conn)
}

export function handleSnapshotManifest(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, _rawBody, abort) => {
    const database = await resolveSnapshotDatabase(res, abort, sirannon, dbId)
    if (!database) return

    try {
      let epoch = ''
      await database.runCdcMaintenance(async writer => {
        epoch = await ensureCdcEpoch(writer)
      })

      const conn = await sirannon.driver.open(database.path, { walMode: true })
      try {
        const startSeq = await changeLogStartSeq(conn)
        const schema = await dumpSchema(conn)
        const tableNames = await tablesInFkOrder(conn)
        const tables: SnapshotManifestResponse['tables'] = []
        for (const name of tableNames) {
          tables.push({ name, rowCount: await countTableRows(conn, name) })
        }

        if (abort.aborted) return
        const manifest: SnapshotManifestResponse = {
          databaseId: dbId,
          startSeq: startSeq.toString(),
          epoch,
          schema,
          tables,
        }
        sendJson(res, manifest)
      } finally {
        await conn.close()
      }
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}

function trimPageToByteCap(
  rows: Record<string, unknown>[],
  checksum: string,
  isLast: boolean,
): { rows: Record<string, unknown>[]; checksum: string; isLast: boolean } {
  let kept = rows
  let keptChecksum = checksum
  let keptIsLast = isLast
  let canonical = canonicaliseForChecksum(kept)
  while (canonical.length > SNAPSHOT_PAGE_BYTE_CAP && kept.length > 1) {
    kept = kept.slice(0, Math.ceil(kept.length / 2))
    canonical = canonicaliseForChecksum(kept)
    keptChecksum = createHash('sha256').update(canonical).digest('hex')
    keptIsLast = false
  }
  return { rows: kept, checksum: keptChecksum, isLast: keptIsLast }
}

export function handleSnapshotPage(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseBody<SnapshotPageRequest>(res, rawBody)
    if (!body) return

    const validationError = snapshotPageValidationError(body)
    if (validationError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', validationError)
      return
    }

    const database = await resolveSnapshotDatabase(res, abort, sirannon, dbId)
    if (!database) return

    try {
      const conn = await sirannon.driver.open(database.path, { walMode: true })
      try {
        if (!(await tableExists(conn, body.table))) {
          sendError(res, 404, 'TABLE_NOT_FOUND', `Table '${body.table}' not found`)
          return
        }

        const pkResolver = new PkResolver(conn)
        const pkColumns = await pkResolver.forTableOnConnection(conn, body.table)
        let startAfter: unknown[] | undefined
        if (body.afterKey !== undefined) {
          if (body.afterKey.length !== pkColumns.length) {
            sendError(
              res,
              400,
              'INVALID_REQUEST',
              `"afterKey" must carry ${pkColumns.length} value(s) for table '${body.table}'`,
            )
            return
          }
          startAfter = decodeTaggedValues(body.afterKey) as unknown[]
        }

        const limit = body.limit ?? SNAPSHOT_DEFAULT_PAGE_ROWS
        const pages = dumpTablePages(conn, pkResolver, body.table, limit, startAfter)
        const first = await pages.next()
        await pages.return(undefined)

        if (abort.aborted) return
        if (first.done || first.value === undefined) {
          const empty: SnapshotPageResponse = { rows: [], checksum: '', done: true, nextKey: null }
          sendJson(res, empty)
          return
        }

        const page = trimPageToByteCap(first.value.rows, first.value.checksum, first.value.isLast)
        let nextKey: unknown[] | null = null
        if (!page.isLast && page.rows.length > 0) {
          const lastRow = page.rows[page.rows.length - 1]
          nextKey = encodeTaggedValues(pkColumns.map(column => lastRow[column])) as unknown[]
        }

        const response: SnapshotPageResponse = {
          rows: encodeWireRowsInPlace(page.rows),
          checksum: page.checksum,
          done: page.isLast,
          nextKey,
        }
        sendJson(res, response)
      } finally {
        await conn.close()
      }
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}
