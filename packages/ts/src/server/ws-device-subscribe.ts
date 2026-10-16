import { needsResync } from '../core/cdc/primed-subscription.js'
import { filteredChange } from '../core/cdc/subscription.js'
import { selectMaxAssignedChangeSeq } from '../core/system-catalog/index.js'
import type { ChangeEvent, Subscription } from '../core/types.js'
import { DEVICE_CURSOR_DROPPED_REASON, deviceCursorExists, WS_CLOSE_DEVICE_CURSOR_DROPPED } from './device-cursors.js'
import type { WSConnection } from './ws-connection.js'
import { DeviceFramePacker, deviceFrameTargetBytes } from './ws-device-frames.js'
import { DeviceChangeStream } from './ws-device-stream.js'
import type { ConnectionState } from './ws-handler.js'
import { releaseUnattached, type WSSubscribeDeps } from './ws-subscribe.js'

class DroppedDeviceCursor extends Error {
  constructor() {
    super(DEVICE_CURSOR_DROPPED_REASON)
    this.name = 'DroppedDeviceCursor'
  }
}

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
  let subscription: Subscription | null = null
  try {
    const ctx = await deps.cdc.ensure(state.databaseId, state.database)
    for (const table of tables) {
      await state.database.watch(table)
    }
    const holdsCursor = await deviceCursorExists(ctx.cdcConn, deviceId)
    epoch = ctx.epoch
    const logEnd =
      sinceSeq === undefined ? 0n : await selectMaxAssignedChangeSeq(ctx.cdcConn, ctx.tracker.changeLogTable)
    boundary = ctx.tracker.cursor

    if (sinceSeq === undefined) {
      resync = false
    } else {
      const minSeq = await ctx.tracker.getMinSeq(ctx.cdcConn)
      const foreignEpoch = clientEpoch !== undefined && clientEpoch !== ctx.epoch
      resync = foreignEpoch || sinceSeq > logEnd || needsResync(sinceSeq, minSeq, boundary)
    }

    const suppress = (event: ChangeEvent): boolean => event.origin === deviceId || event.origin === undefined
    const resumeFrom = resync || sinceSeq === undefined ? boundary : sinceSeq

    stream = new DeviceChangeStream(
      {
        deviceId,
        holdsCursor,
        maxUnacknowledgedChanges: deps.maxUnacknowledgedChanges,
        pacing: stagedStream ? 'perEvent' : 'perTransaction',
        packer: stagedStream
          ? new DeviceFramePacker(id, data => deps.sendText(conn, data), deviceFrameTargetBytes(deps.socketResumeBytes))
          : null,
        sendEvent: event => deps.sendChange(conn, id, event),
        socketBuffered: () => conn.bufferedAmount(),
        socketCongested: () => conn.bufferedAmount() >= deps.socketResumeBytes,
        flushSocket: () => conn.flush(),
        onOverload: () => deps.handleOverload(conn),
        onFault: err => {
          if (err instanceof DroppedDeviceCursor) {
            conn.close(WS_CLOSE_DEVICE_CURSOR_DROPPED, err.message)
            return
          }
          deps.closeFaulted(conn)
        },
        readLog: async (afterSeq, upToSeq, limit) => {
          const events = await ctx.tracker.readSinceTables(ctx.cdcConn, tables, afterSeq, upToSeq, limit)
          if (stream.holdsCursor && !(await deviceCursorExists(ctx.cdcConn, deviceId))) {
            throw new DroppedDeviceCursor()
          }
          return events
        },
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

    subscription = {
      unsubscribe: () => {
        removeBatchEnd()
        stream.stop()
        state.deviceStreams.delete(id)
        for (const tableSubscription of subscriptions) {
          tableSubscription.unsubscribe()
        }
      },
    }
    const attachment = deps.attachSubscription(conn, id, subscription)
    if (attachment !== 'attached') {
      releaseUnattached(deps, conn, state, id, subscription, attachment)
      return
    }
    state.deviceStreams.set(id, stream)
  } catch (err) {
    if (subscription) deps.detachSubscription(conn, id, subscription)
    deps.cdc.maybeCleanup(state.databaseId)
    deps.sendSirannonError(conn, id, err)
    return
  }

  deps.sendSubscribed(conn, id, boundary.toString(), epoch, resync, deps.maxUnacknowledgedChanges)
  stream.start()
}
