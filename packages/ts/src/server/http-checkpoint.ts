import type { Sirannon } from '../core/sirannon.js'
import type { CheckpointResponse } from './checkpoint-protocol.js'
import { parseOptionalBody, resolveBackupDatabase } from './http-backups.js'
import { sendCaughtError, sendJson } from './http-common.js'
import type { DbRouteHandler } from './http-handler.js'

/**
 * Handles the route that checkpoints the write-ahead log of one database and reports whether the checkpoint finished.
 *
 * The server answers once the checkpoint ends, because the result is what the caller asks for. On a database opened
 * with backups, the checkpoint is a turn of the backup cycle, which can take as long as a full copy of the database.
 *
 * The server accepts an empty body or a JSON object on this route, and it answers any other body with `400`.
 *
 * @param sirannon - The registry in which the server's databases are open.
 * @returns The handler that the server registers.
 * @internal
 */
export function handleCheckpoint(sirannon: Sirannon): DbRouteHandler {
  return async (res, dbId, rawBody, abort) => {
    if (!parseOptionalBody<Record<string, unknown>>(res, rawBody)) return

    const database = await resolveBackupDatabase(res, abort, sirannon, dbId)
    if (!database) return

    try {
      const result = await database.checkpoint()
      if (abort.aborted) return
      const body: CheckpointResponse = {
        busy: result.busy,
        framesInLog: result.framesInLog,
        framesCheckpointed: result.framesCheckpointed,
        emptied: result.emptied,
      }
      sendJson(res, body)
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}
