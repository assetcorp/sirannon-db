import type { ChangeEvent } from '../core/types.js'
import type { CDCContext } from './ws-cdc.js'
import { needsResync, PrimedSubscription } from './ws-cdc-resume.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
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
}

interface DeviceSubscription {
  ctx: CDCContext
  stream: DeviceChangeStream
  primed: PrimedSubscription | null
  boundary: bigint
  resync: boolean
}

export async function subscribeDevice(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  request: DeviceSubscribeRequest,
): Promise<void> {
  let active: DeviceSubscription
  try {
    active = await openDeviceSubscription(deps, conn, state, request)
  } catch (err) {
    deps.cdc.maybeCleanup(state.databaseId)
    deps.sendSirannonError(conn, request.id, err)
    return
  }

  const { ctx, stream, primed, boundary, resync } = active
  deps.sendSubscribed(conn, request.id, boundary.toString(), ctx.epoch, resync)

  if (primed === null) return

  if (!resync) {
    try {
      await primed.replayTables(ctx.tracker, ctx.cdcConn, request.tables, request.filter, boundary)
    } catch {
      deps.sendSubscribed(conn, request.id, boundary.toString(), ctx.epoch, true)
    }
  }
  primed.goLive()
  stream.endPriming()
}

async function openDeviceSubscription(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  request: DeviceSubscribeRequest,
): Promise<DeviceSubscription> {
  const { id, tables, filter, sinceSeq, clientEpoch, deviceId } = request

  const ctx = await deps.cdc.ensure(state.databaseId, state.database)
  for (const table of tables) {
    await ctx.tracker.watch(ctx.cdcConn, table)
  }

  const stream = new DeviceChangeStream({
    deviceId,
    maxUnacknowledgedChanges: deps.maxUnacknowledgedChanges,
    send: event => deps.sendChange(conn, id, event),
    onOverload: () => deps.handleOverload(conn),
  })

  const deliver = (event: ChangeEvent): WSSendOutcome => {
    if (event.origin === deviceId) return 'sent'
    stream.receive(event)
    return stream.stopped ? 'dropped' : 'sent'
  }

  const boundary = ctx.tracker.cursor
  const primed =
    sinceSeq === undefined ? null : new PrimedSubscription(sinceSeq, deliver, () => deps.handleOverload(conn))
  if (primed !== null) {
    stream.beginPriming()
  }
  const onEvent = primed === null ? deliver : (event: ChangeEvent) => primed.onLiveEvent(event)

  const subscriptions = tables.map(table => ctx.manager.subscribe(table, filter, onEvent))
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

  if (sinceSeq === undefined) {
    return { ctx, stream, primed, boundary, resync: false }
  }

  const minSeq = await ctx.tracker.getMinSeq(ctx.cdcConn)
  const foreignEpoch = clientEpoch !== undefined && clientEpoch !== ctx.epoch
  return { ctx, stream, primed, boundary, resync: foreignEpoch || needsResync(sinceSeq, minSeq, boundary) }
}
