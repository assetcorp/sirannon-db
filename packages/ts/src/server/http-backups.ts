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
 * Reads the JSON body of a request whose fields are all optional, so that a
 * caller with nothing to say sends nothing.
 *
 * @param res - Response the server refuses a malformed body through.
 * @param rawBody - Bytes the request supplied.
 * @returns The parsed body, an empty one where the request supplied no bytes, or null where the server has already refused it.
 */
export function parseOptionalBody<T>(res: HttpResponse, rawBody: Buffer): T | null {
  if (rawBody.length === 0) return {} as T
  return parseBody<T>(res, rawBody)
}

/**
 * Finds the open database a backup route addresses.
 *
 * @param res - Response the server refuses through.
 * @param abort - Whether the caller has disconnected.
 * @param sirannon - Registry the database is open in.
 * @param dbId - Identifier the route named.
 * @returns The database, or null where the server has already answered the caller.
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

/**
 * Gives a turn one tick of the event loop in which to refuse, and reports what
 * it refused with.
 *
 * A turn continues long past any response, so the route answers without waiting
 * for it. The guards in front of that turn refuse on the first tick, however,
 * and a caller told the turn had started when a closed database or a snapshot
 * load had already refused it would then poll a progress route that reports
 * nothing wrong.
 *
 * @param turn - The turn the route asked for.
 * @returns What the turn refused with, or undefined where it is still under way.
 */
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
 * Serves the route that takes one turn of the checkpoint cycle now.
 *
 * Sirannon accepts the turn and answers the caller straight away, since a full
 * copy of a large database may continue past the deadline any proxy between
 * that caller and the server allows. The progress route is where the caller
 * reads what the turn produced.
 *
 * @param sirannon - Registry the databases the server serves are open in.
 * @returns The handler the server registers.
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
 * Serves the route that reports what the cycle is doing and what its recent
 * turns produced.
 *
 * @param sirannon - Registry the databases the server serves are open in.
 * @returns The handler the server registers.
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
 * Serves the route that lists what the backup destination stores.
 *
 * @param sirannon - Registry the databases the server serves are open in.
 * @returns The handler the server registers.
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
 * Serves the route that reads one stored backup back out of the destination and
 * compares it against the record the backup that wrote it left behind.
 *
 * @param sirannon - Registry the databases the server serves are open in.
 * @returns The handler the server registers.
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
 * Serves the route that answers which backups no restore still needs.
 *
 * Sirannon lists them and deletes nothing, so a caller reads the answer and
 * then does as it likes with its own destination.
 *
 * @param sirannon - Registry the databases the server serves are open in.
 * @returns The handler the server registers.
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
