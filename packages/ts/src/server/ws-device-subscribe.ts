import { needsResync } from '../core/cdc/primed-subscription.js'
import { filteredChange } from '../core/cdc/subscription.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSConnection } from './ws-connection.js'
import { DeviceFramePacker } from './ws-device-frames.js'
import { DeviceChangeStream } from './ws-device-stream.js'
import type { ConnectionState } from './ws-handler.js'
import type { WSSubscribeDeps } from './ws-subscribe.js'

export interface DeviceSubscribeRequest {
  id: string
  tables: readonly string[]
  filter: Record<string, unknown> | undefined
  sinceSeq: bigint | undefined
  clientEpoch: string | undefined
  deviceId: string
  stagedStream: boolean
}

export async function subscribeDevice(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  request: DeviceSubscribeRequest,
): Promise<void> {
  const { id, tables, filter, sinceSeq, clientEpoch, deviceId, stagedStream } = request

  let stream: DeviceChangeStream
  let boundary: bigint
  let resync: boolean
  let epoch: string
  try {
    const ctx = await deps.cdc.ensure(state.databaseId, state.database)
    for (const table of tables) {
      await ctx.tracker.watch(ctx.cdcConn, table)
    }
    await state.database.ensureChangeStamping()
    epoch = ctx.epoch
    boundary = ctx.tracker.cursor

    if (sinceSeq === undefined) {
      resync = false
    } else {
      const minSeq = await ctx.tracker.getMinSeq(ctx.cdcConn)
      const foreignEpoch = clientEpoch !== undefined && clientEpoch !== ctx.epoch
      resync = foreignEpoch || needsResync(sinceSeq, minSeq, boundary)
    }

    const suppress = (event: ChangeEvent): boolean => event.origin === deviceId || event.origin === undefined
    const resumeFrom = resync || sinceSeq === undefined ? boundary : sinceSeq

    stream = new DeviceChangeStream(
      {
        deviceId,
        maxUnacknowledgedChanges: deps.maxUnacknowledgedChanges,
        pacing: stagedStream ? 'perEvent' : 'perTransaction',
        packer: stagedStream ? new DeviceFramePacker(id, data => deps.sendText(conn, data)) : null,
        sendEvent: event => deps.sendChange(conn, id, event),
        socketBuffered: () => conn.bufferedAmount(),
        socketCongested: () => conn.bufferedAmount() >= deps.socketResumeBytes,
        flushSocket: () => conn.flush(),
        onOverload: () => deps.handleOverload(conn),
        onFault: () => deps.closeFaulted(conn),
        readLog: (afterSeq, upToSeq, limit) =>
          ctx.tracker.readSinceTables(ctx.cdcConn, tables, afterSeq, upToSeq, limit),
        logCursor: () => ctx.tracker.cursor,
        logCursorAtTxBoundary: () => ctx.tracker.pollEndedAtTxBoundary,
        transform: event => {
          if (suppress(event)) return null
          return filter === undefined ? event : filteredChange(event, filter)
        },
      },
      resumeFrom,
      resumeFrom < boundary ? 'catchup' : 'live',
    )

    const subscriptions = tables.map(table =>
      ctx.manager.subscribe(table, filter, event => {
        if (suppress(event)) return
        stream.receiveLive(event)
      }),
    )
    const removeBatchEnd = ctx.manager.addBatchEndListener(atTxBoundary => stream.onBatchEnd(atTxBoundary))

    state.subscriptions.set(id, {
      unsubscribe: () => {
        removeBatchEnd()
        stream.stop()
        state.deviceStreams.delete(id)
        for (const subscription of subscriptions) {
          subscription.unsubscribe()
        }
      },
    })
    state.deviceStreams.set(id, stream)
  } catch (err) {
    deps.cdc.maybeCleanup(state.databaseId)
    deps.sendSirannonError(conn, id, err)
    return
  }

  deps.sendSubscribed(conn, id, boundary.toString(), epoch, resync, deps.maxUnacknowledgedChanges)
  stream.start()
}
