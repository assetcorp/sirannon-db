import { highestMigrationVersion } from '../core/system-catalog/index.js'
import type { ChangeEvent } from '../core/types.js'
import type { AckResponse } from './protocol.js'
import { decodeBoundParams } from './protocol.js'
import { isValidDeviceId, isValidSchemaVersion, schemaVersionGateRefusal } from './sync-protocol.js'
import type { CdcContextRegistry } from './ws-cdc.js'
import { needsResync, PrimedSubscription } from './ws-cdc-resume.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
import type { ConnectionState } from './ws-handler.js'

export interface WSSubscribeDeps {
  cdc: CdcContextRegistry
  sendSubscribed(conn: WSConnection, id: string, seq: string, epoch: string, resync: boolean): void
  sendResult(conn: WSConnection, id: string, data: AckResponse): void
  sendError(conn: WSConnection, id: string, code: string, message: string): void
  sendSirannonError(conn: WSConnection, id: string, err: unknown): void
  sendChange(conn: WSConnection, subscriptionId: string, event: ChangeEvent): WSSendOutcome
  handleOverload(conn: WSConnection): void
}

export async function handleSubscribeMessage(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  if (typeof msg.table !== 'string') {
    deps.sendError(conn, id, 'INVALID_MESSAGE', 'Subscribe message requires a "table" string field')
    return
  }

  if (state.subscriptions.has(id)) {
    deps.sendError(conn, id, 'DUPLICATE_SUBSCRIPTION', `Subscription '${id}' already exists on this connection`)
    return
  }

  if (state.database.readOnly) {
    deps.sendError(conn, id, 'READ_ONLY', 'Subscriptions are not available on read-only databases')
    return
  }

  if (state.database.path === ':memory:') {
    deps.sendError(conn, id, 'CDC_UNSUPPORTED', 'CDC subscriptions require file-based databases')
    return
  }

  if (
    msg.filter !== undefined &&
    msg.filter !== null &&
    (typeof msg.filter !== 'object' || Array.isArray(msg.filter))
  ) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"filter" must be a plain object')
    return
  }

  const decodedFilter = decodeBoundParams(msg.filter, 'filter')
  if (!decodedFilter.ok) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', decodedFilter.message)
    return
  }
  const filter = decodedFilter.value as Record<string, unknown> | undefined

  let sinceSeq: bigint | undefined
  if (msg.sinceSeq !== undefined) {
    if (typeof msg.sinceSeq !== 'string' || !/^\d+$/.test(msg.sinceSeq)) {
      deps.sendError(conn, id, 'INVALID_MESSAGE', '"sinceSeq" must be a non-negative integer string')
      return
    }
    sinceSeq = BigInt(msg.sinceSeq)
  }

  let clientEpoch: string | undefined
  if (msg.epoch !== undefined) {
    if (typeof msg.epoch !== 'string') {
      deps.sendError(conn, id, 'INVALID_MESSAGE', '"epoch" must be a string')
      return
    }
    clientEpoch = msg.epoch
  }

  let deviceId: string | undefined
  if (msg.deviceId !== undefined) {
    if (!isValidDeviceId(msg.deviceId)) {
      deps.sendError(conn, id, 'INVALID_MESSAGE', '"deviceId" must be a 32-hex device id')
      return
    }
    deviceId = msg.deviceId
  }

  if (deviceId !== undefined) {
    if (msg.schemaVersion !== undefined && !isValidSchemaVersion(msg.schemaVersion)) {
      deps.sendError(conn, id, 'INVALID_MESSAGE', '"schemaVersion" must be a non-negative integer')
      return
    }
    let refusal: ReturnType<typeof schemaVersionGateRefusal>
    try {
      const serverVersion = highestMigrationVersion(await state.database.appliedMigrations())
      refusal = schemaVersionGateRefusal(msg.schemaVersion ?? 0, serverVersion)
    } catch (err) {
      deps.sendSirannonError(conn, id, err)
      return
    }
    if (refusal !== null) {
      deps.sendError(conn, id, refusal.code, refusal.message)
      return
    }
  }

  if (sinceSeq === undefined) {
    await subscribeLive(deps, conn, state, id, msg.table, filter, deviceId)
    return
  }

  await subscribeResuming(deps, conn, state, id, msg.table, filter, sinceSeq, clientEpoch, deviceId)
}

function withEchoSuppression(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  id: string,
  deviceId: string | undefined,
): (event: ChangeEvent) => WSSendOutcome {
  return event => {
    if (deviceId !== undefined && event.origin === deviceId) return 'sent'
    return deps.sendChange(conn, id, event)
  }
}

async function subscribeLive(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  id: string,
  table: string,
  filter: Record<string, unknown> | undefined,
  deviceId: string | undefined,
): Promise<void> {
  try {
    const ctx = await deps.cdc.ensure(state.databaseId, state.database)
    await ctx.tracker.watch(ctx.cdcConn, table)

    const deliver = withEchoSuppression(deps, conn, id, deviceId)
    const boundary = ctx.tracker.cursor
    const sub = ctx.manager.subscribe(table, filter, (event: ChangeEvent) => {
      deliver(event)
    })

    state.subscriptions.set(id, sub)
    deps.sendSubscribed(conn, id, boundary.toString(), ctx.epoch, false)
  } catch (err) {
    deps.cdc.maybeCleanup(state.databaseId)
    deps.sendSirannonError(conn, id, err)
  }
}

async function subscribeResuming(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  id: string,
  table: string,
  filter: Record<string, unknown> | undefined,
  sinceSeq: bigint,
  clientEpoch: string | undefined,
  deviceId: string | undefined,
): Promise<void> {
  let ctx: Awaited<ReturnType<CdcContextRegistry['ensure']>>
  let primed: PrimedSubscription
  let boundary: bigint
  let resync: boolean
  try {
    ctx = await deps.cdc.ensure(state.databaseId, state.database)
    await ctx.tracker.watch(ctx.cdcConn, table)

    const deliver = withEchoSuppression(deps, conn, id, deviceId)
    boundary = ctx.tracker.cursor
    primed = new PrimedSubscription(
      sinceSeq,
      event => deliver(event),
      () => deps.handleOverload(conn),
    )
    const sub = ctx.manager.subscribe(table, filter, event => primed.onLiveEvent(event))
    state.subscriptions.set(id, sub)

    const minSeq = await ctx.tracker.getMinSeq(ctx.cdcConn)
    const foreignEpoch = clientEpoch !== undefined && clientEpoch !== ctx.epoch
    resync = foreignEpoch || needsResync(sinceSeq, minSeq, boundary)
    deps.sendSubscribed(conn, id, boundary.toString(), ctx.epoch, resync)
  } catch (err) {
    deps.cdc.maybeCleanup(state.databaseId)
    deps.sendSirannonError(conn, id, err)
    return
  }

  if (!resync) {
    try {
      await primed.replay(ctx.tracker, ctx.cdcConn, table, filter, boundary)
    } catch {
      deps.sendSubscribed(conn, id, boundary.toString(), ctx.epoch, true)
    }
  }
  primed.goLive()
}
