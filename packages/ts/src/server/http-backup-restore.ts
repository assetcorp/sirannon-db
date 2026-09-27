import { discardStagedChain } from '../core/backup/cycle-capture.js'
import { restoreBackup } from '../core/backup/restore.js'
import type { BackupChainLocation } from '../core/database-backup.js'
import type { Sirannon } from '../core/sirannon.js'
import type { BackupRestoreRequest } from './backup-protocol.js'
import { BACKUP_RESTORE_NOT_ACCEPTED_MESSAGE, restoreRequestValidationError } from './backup-protocol.js'
import type { BackupRestoreRuns } from './backup-restore-runs.js'
import { parseOptionalBody, resolveBackupDatabase } from './http-backups.js'
import { sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbGetRouteHandler, DbRouteHandler } from './http-handler.js'

function restoreAlreadyRunning(databaseId: string): string {
  return `A restore of database '${databaseId}' is already under way, and it keeps that file to itself until it finishes`
}

interface RestoreRequest {
  sirannon: Sirannon
  runs: BackupRestoreRuns
  databaseId: string
  location: BackupChainLocation
  moment: number
  batchSize?: number
}

async function rebuildDatabase(request: RestoreRequest): Promise<void> {
  const { sirannon, runs, databaseId, location } = request
  try {
    const outcome = await sirannon.withDatabaseOffline(databaseId, async destPath => {
      await discardStagedChain(location.stagingDir)
      return restoreBackup({
        destination: location.destination,
        chainName: location.chainName,
        driver: sirannon.driver,
        destPath,
        replaceExisting: true,
        moment: request.moment,
        ...(location.destinationTimeoutMs === undefined ? {} : { destinationTimeoutMs: location.destinationTimeoutMs }),
        ...(request.batchSize === undefined ? {} : { batchSize: request.batchSize }),
        onProgress: progress => runs.progressed(databaseId, progress),
      })
    })
    if (!outcome.ok) {
      runs.failed(databaseId, outcome.failure)
      return
    }
    runs.finished(databaseId, outcome.value, outcome.reopenFailure)
  } catch (err) {
    runs.failed(databaseId, err)
  }
}

/**
 * Handles the route that rebuilds a database at a moment that the caller names.
 *
 * The route refuses every request with `BACKUP_RESTORE_NOT_ACCEPTED` until the
 * operator turns `acceptBackupRestore` on, because a restore replaces a database
 * while the server is serving it.
 *
 * The server starts the restore and answers with 202 at once, because rebuilding
 * a large database can take longer than the timeout of a proxy between the caller
 * and the server. The server takes the database offline while it replaces the
 * file, so the caller reads the outcome from the status route.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @param accepted - Whether the operator opens this route with `acceptBackupRestore`.
 * @param runs - The server's record of each restore.
 * @returns The handler that the server registers.
 */
export function handleBackupRestore(sirannon: Sirannon, accepted: boolean, runs: BackupRestoreRuns): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    if (!accepted) {
      sendError(res, 403, 'BACKUP_RESTORE_NOT_ACCEPTED', BACKUP_RESTORE_NOT_ACCEPTED_MESSAGE)
      return
    }

    const body = parseOptionalBody<BackupRestoreRequest>(res, rawBody)
    if (!body) return

    const validationError = restoreRequestValidationError(body)
    if (validationError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', validationError)
      return
    }

    if (runs.read(dbId).state === 'running') {
      sendError(res, 409, 'BACKUP_RESTORE_IN_PROGRESS', restoreAlreadyRunning(dbId))
      return
    }

    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return

    let location: BackupChainLocation
    try {
      location = database.backupLocation()
    } catch (err) {
      sendCaughtError(res, abort, err)
      return
    }

    const moment = body.moment === undefined ? Date.now() : (body.moment as number)
    if (!runs.claim(dbId, moment)) {
      sendError(res, 409, 'BACKUP_RESTORE_IN_PROGRESS', restoreAlreadyRunning(dbId))
      return
    }

    void rebuildDatabase({
      sirannon,
      runs,
      databaseId: dbId,
      location,
      moment,
      ...(body.batchSize === undefined ? {} : { batchSize: body.batchSize as number }),
    })
    if (abort.aborted) return
    sendJson(res, { started: true }, '202 Accepted')
  }
}

/**
 * Handles the route that reports the state of one database's restore.
 *
 * It reads the server's own record, so that it can answer while the database
 * is closed for the restore.
 *
 * @param runs - The server's record of each restore.
 * @returns The handler that the server registers.
 */
export function handleBackupRestoreStatus(runs: BackupRestoreRuns): DbGetRouteHandler {
  return async (res, dbId, _ctx, abort) => {
    if (abort.aborted) return
    sendJson(res, runs.read(dbId))
  }
}
