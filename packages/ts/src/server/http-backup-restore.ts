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

/**
 * Rebuilds one database from its own backups, at the path it already occupies.
 *
 * The database closes first, since no connection may be open on the file while
 * Sirannon replaces its bytes, and it opens again at the end with the settings
 * it had before.
 *
 * Sirannon discards the cycle's record of where it had reached before the
 * rebuild, and that order is what makes this path safe. A process that died
 * between a finished rebuild and a later cleanup would leave the new file
 * beside a state file naming the old chain, and the next capture would then
 * append a piece cut from the restored timeline onto that chain. No check
 * downstream would catch it, since the piece starts at frame one under a fresh
 * log sequence, which is what an ordinary log restart also produces, and both
 * its digest and its byte count would match. A restore of that chain would then
 * rebuild a database missing the writes the earlier restore had rolled back,
 * and report success.
 *
 * Discarding first means a fresh full copy where the rebuild then fails, since
 * the reopened cycle starts a new chain. It loses no data, because a staged
 * capture stores frames SQLite has already folded into the database file, and
 * that fresh full copy includes them.
 *
 * @param request - The registry, the restore record, and what to rebuild from.
 */
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
 * Serves the route that rebuilds a database from a moment the caller names.
 *
 * The route stays shut unless the operator turns `acceptBackupRestore` on. A
 * restore replaces the database that is serving traffic, and no default
 * configuration should reach a route that does that.
 *
 * Sirannon accepts the restore and answers straight away, since rebuilding a
 * large database may continue past the deadline any proxy between the caller
 * and the server allows. That database answers nothing while Sirannon replaces
 * its file, so the status route is where a caller reads how the restore went.
 *
 * @param sirannon - Registry the databases the server serves are open in.
 * @param accepted - Whether the operator opened this route.
 * @param runs - Where the server records how each restore went.
 * @returns The handler the server registers.
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
 * Serves the route that reports how one database's restore went.
 *
 * It reads the server's own record, which is what lets it answer while that
 * database is closed, and a caller asks at exactly that moment.
 *
 * @param runs - Where the server records how each restore went.
 * @returns The handler the server registers.
 */
export function handleBackupRestoreStatus(runs: BackupRestoreRuns): DbGetRouteHandler {
  return async (res, dbId, _ctx, abort) => {
    if (abort.aborted) return
    sendJson(res, runs.read(dbId))
  }
}
