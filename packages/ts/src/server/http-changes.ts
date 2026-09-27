import type { HttpResponse } from 'uWebSockets.js'
import type { PushHookContext } from '../core/hooks/types.js'
import type { Sirannon } from '../core/sirannon.js'
import { HLC } from '../core/sync/hlc.js'
import type { ReplicationBatch } from '../core/sync/types.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import type { ServerExecutionTarget, ServerExecutionTargetResolver } from '../core/types.js'
import type { ResponseAbort } from './http-common.js'
import { parseBody, resolveExecutionTarget, sendCaughtError, sendError, sendJson } from './http-common.js'
import type { DbRouteHandler } from './http-handler.js'
import type { ChangesRequest } from './sync-protocol.js'
import {
  decodeSyncBatch,
  schemaVersionGateRefusal,
  schemaVersionValidationError,
  syncBatchValidationError,
  toChangesResponse,
} from './sync-protocol.js'

export const MAX_DEVICE_CLOCK_AHEAD_MS = 300_000

function changeClockRefusal(batch: ReplicationBatch, nowMs: number): { code: string; message: string } | null {
  for (const [index, change] of batch.changes.entries()) {
    let wallMs: number
    try {
      wallMs = HLC.decode(change.hlc).wallMs
    } catch {
      wallMs = Number.NaN
    }
    if (!Number.isFinite(wallMs)) {
      return { code: 'INVALID_REQUEST', message: `changes[${index}].hlc is not a valid timestamp` }
    }
    if (wallMs > nowMs + MAX_DEVICE_CLOCK_AHEAD_MS) {
      return {
        code: 'DEVICE_CLOCK_AHEAD',
        message: `changes[${index}] is stamped more than ${MAX_DEVICE_CLOCK_AHEAD_MS} ms ahead of the server clock; correct the device clock`,
      }
    }
  }
  return null
}

async function refuseUngrantedPush(
  sirannon: Sirannon,
  databaseId: string,
  batch: ReplicationBatch,
  identity: unknown,
): Promise<void> {
  if (!sirannon.hookRegistry.has('beforePush')) return
  const tables = new Set(batch.changes.map(change => change.table))
  for (const table of tables) {
    const ctx: PushHookContext = { databaseId, table, deviceId: batch.sourceNodeId, identity }
    await sirannon.hookRegistry.invoke('beforePush', ctx)
  }
}

async function refuseSchemaMismatch(
  res: HttpResponse,
  abort: ResponseAbort,
  target: ServerExecutionTarget,
  deviceVersion: number,
): Promise<boolean> {
  const appliedMigrations = target.appliedMigrations
  if (typeof appliedMigrations !== 'function') return false
  const serverVersion = highestMigrationVersion(await appliedMigrations.call(target))
  const refusal = schemaVersionGateRefusal(deviceVersion, serverVersion)
  if (refusal === null) return false
  if (!abort.aborted) {
    sendError(res, 409, refusal.code, refusal.message, { serverVersion })
  }
  return true
}

export function handleChanges(sirannon: Sirannon, resolveTarget?: ServerExecutionTargetResolver): DbRouteHandler {
  return async (res, dbId, rawBody, abort, identity) => {
    const body = parseBody<ChangesRequest>(res, rawBody)
    if (!body) return

    const batchError = syncBatchValidationError(body.batch)
    if (batchError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', batchError)
      return
    }

    const schemaVersionError = schemaVersionValidationError(body.schemaVersion)
    if (schemaVersionError !== null) {
      sendError(res, 400, 'INVALID_REQUEST', schemaVersionError)
      return
    }

    const target = await resolveExecutionTarget(res, abort, sirannon, dbId, resolveTarget)
    if (!target) return

    try {
      const batch = decodeSyncBatch(body.batch)
      await refuseUngrantedPush(sirannon, dbId, batch, identity)

      const clockRefusal = changeClockRefusal(batch, Date.now())
      if (clockRefusal !== null) {
        sendError(res, 400, clockRefusal.code, clockRefusal.message)
        return
      }

      if (await refuseSchemaMismatch(res, abort, target, body.schemaVersion ?? 0)) return

      const applyChanges = target.applyChanges
      if (typeof applyChanges !== 'function') {
        sendError(
          res,
          501,
          'SYNC_UNSUPPORTED',
          'The execution target for this database does not support applying sync changes',
        )
        return
      }

      const result = await applyChanges.call(target, batch)
      if (abort.aborted) return
      sendJson(res, toChangesResponse(result))
    } catch (err) {
      sendCaughtError(res, abort, err)
    }
  }
}
