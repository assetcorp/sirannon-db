import type { HttpResponse } from 'uWebSockets.js'
import type { BackupCycleStatus } from '../core/backup/cycle-status.js'
import type { Database } from '../core/database.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type {
  BackupChainResponse,
  BackupSafeToDeleteRequest,
  BackupSafeToDeleteResponse,
  BackupVerifyRequest,
} from './backup-protocol.js'
import { restorableFromValidationError, verifyNameValidationError } from './backup-protocol.js'
import type { ResponseAbort } from './http-common.js'
import { parseBody, sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbGetRouteHandler, DbRouteHandler } from './http-handler.js'

/**
 * Parses the JSON body of a request whose fields are all optional, so that a
 * caller with no fields to set can send an empty body.
 *
 * @param res - The response through which the server rejects a malformed body.
 * @param rawBody - The bytes of the request body.
 * @returns The parsed body, an empty object when the request body is empty, or null when the server has already sent an error response.
 */
export function parseOptionalBody<T>(res: HttpResponse, rawBody: Buffer): T | null {
  if (rawBody.length === 0) return {} as T
  return parseBody<T>(res, rawBody)
}

/**
 * Returns the open database that a backup route names.
 *
 * @param res - The response through which the server sends an error.
 * @param abort - Tracks whether the caller has disconnected.
 * @param sirannon - The registry in which the database is open.
 * @param dbId - The identifier in the route.
 * @returns The database, or null when the server has already answered the caller.
 */
export async function resolveBackupDatabase(
  res: HttpResponse,
  abort: ResponseAbort,
  sirannon: Sirannon,
  dbId: string,
): Promise<Database | null> {
  let database: Database | undefined
  try {
    database = await sirannon.resolve(dbId)
  } catch (err) {
    sendCaughtError(res, abort, err)
    return null
  }
  if (abort.aborted) return null
  if (!database) {
    sendError(res, 404, 'DATABASE_NOT_FOUND', `Database '${dbId}' not found`)
    return null
  }
  return database
}

function refusedOnTheFirstTick(turn: Promise<unknown>): Promise<unknown> {
  const settled = turn.then(
    () => undefined,
    (err: unknown) => err ?? new SirannonError('The backup turn failed', 'BACKUP_ERROR'),
  )
  const tick = new Promise<undefined>(resolve => {
    setImmediate(() => resolve(undefined))
  })
  return Promise.race([settled, tick])
}

function readStatus(res: HttpResponse, abort: ResponseAbort, database: Database): BackupCycleStatus | null {
  try {
    return database.backupStatus()
  } catch (err) {
    sendCaughtError(res, abort, err)
    return null
  }
}

/**
 * Handles the route that starts one turn of the checkpoint cycle now.
 *
 * The server waits one event-loop tick, so that it can report a turn that fails
 * at once, then answers with 202, because a full copy of a large database can
 * take longer than the timeout of a proxy between the caller and the server. The
 * caller reads the result of the turn from the status route.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 */
export function handleBackupTrigger(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, _rawBody, abort) => {
    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return
    if (!readStatus(res, abort, database)) return

    const refusal = await refusedOnTheFirstTick(database.captureBackupChanges())
    if (refusal !== undefined) {
      sendCaughtError(res, abort, refusal)
      return
    }
    if (abort.aborted) return
    sendJson(res, { started: true }, '202 Accepted')
  }
}

/**
 * Handles the route that reports the checkpoint cycle's current activity and
 * the results of its recent turns.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 */
export function handleBackupStatus(sirannon: Sirannon): DbGetRouteHandler {
  return async (res, dbId, _ctx, abort) => {
    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return
    const status = readStatus(res, abort, database)
    if (!status || abort.aborted) return
    sendJson(res, status)
  }
}

/**
 * Handles the route that lists the chains in the backup destination.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 */
export function handleBackupChain(sirannon: Sirannon): DbGetRouteHandler {
  return async (res, dbId, _ctx, abort) => {
    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return

    try {
      const chains = await database.backupChain()
      if (abort.aborted) return
      const body: BackupChainResponse = { chains }
      sendJson(res, body)
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}

/**
 * Handles the route that reads one stored backup back from the destination and
 * compares it against the record that Sirannon wrote when it made the backup.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 */
export function handleBackupVerify(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseOptionalBody<BackupVerifyRequest>(res, rawBody)
    if (!body) return

    const nameError = verifyNameValidationError(body.name)
    if (nameError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', nameError)
      return
    }

    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return

    try {
      const result = await database.verifyBackup(body.name as string)
      if (abort.aborted) return
      sendJson(res, result)
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}

/**
 * Handles the route that lists the backups that you no longer need for any restore.
 *
 * Sirannon lists them and deletes nothing, so the caller decides what to
 * delete from its own destination.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 */
export function handleBackupSafeToDelete(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    const body = parseOptionalBody<BackupSafeToDeleteRequest>(res, rawBody)
    if (!body) return

    const restorableFromError = restorableFromValidationError(body.restorableFrom)
    if (restorableFromError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', restorableFromError)
      return
    }

    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return

    try {
      const records = await database.backupPiecesSafeToDelete(
        body.restorableFrom === undefined ? undefined : { restorableFrom: body.restorableFrom as number },
      )
      if (abort.aborted) return
      const answer: BackupSafeToDeleteResponse = { records }
      sendJson(res, answer)
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}
