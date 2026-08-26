import { TransactionGrouper } from '../core/cdc/transaction-grouper.js'
import type { SubscribeHookContext } from '../core/hooks/types.js'
import { highestMigrationVersion } from '../core/system-catalog/index.js'
import type { ChangeEvent, Subscription } from '../core/types.js'
import type { AckResponse } from './protocol.js'
import { decodeBoundParams } from './protocol.js'
import { isValidDeviceId, isValidSchemaVersion, schemaVersionGateRefusal } from './sync-protocol.js'
import type { CdcContextRegistry } from './ws-cdc.js'
import { needsResync, PrimedSubscription } from './ws-cdc-resume.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
import { subscribeDevice } from './ws-device-subscribe.js'
import type { ConnectionState } from './ws-handler.js'

const MAX_SUBSCRIBED_TABLES = 500

export type SubscriptionAttachment = 'attached' | 'duplicate' | 'disconnected'

export interface WSSubscribeDeps {
  cdc: CdcContextRegistry
  maxUnacknowledgedChanges: number
  socketResumeBytes: number
  hasSubscribeHook(): boolean
  beforeSubscribe(ctx: SubscribeHookContext): Promise<void>
  attachSubscription(conn: WSConnection, id: string, subscription: Subscription): SubscriptionAttachment
  detachSubscription(conn: WSConnection, id: string, subscription: Subscription): void
  sendSubscribed(
    conn: WSConnection,
    id: string,
    seq: string,
    epoch: string,
    resync: boolean,
    maxUnacknowledgedChanges?: number,
  ): void
  sendResult(conn: WSConnection, id: string, data: AckResponse): void
  sendError(conn: WSConnection, id: string, code: string, message: string): void
  sendSirannonError(conn: WSConnection, id: string, err: unknown): void
  sendChange(conn: WSConnection, subscriptionId: string, event: ChangeEvent): WSSendOutcome
  sendText(conn: WSConnection, data: string): WSSendOutcome
  closeFaulted(conn: WSConnection): void
  handleOverload(conn: WSConnection): void
}

export async function handleSubscribeMessage(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  msg: Record<string, unknown>,
  id: string,
): Promise<void> {
  const tables = readTableSet(msg)
  if (typeof tables === 'string') {
    deps.sendError(conn, id, 'INVALID_MESSAGE', tables)
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

  if (msg.stagedStream !== undefined && typeof msg.stagedStream !== 'boolean') {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"stagedStream" must be a boolean')
    return
  }

  let schemaVersion = 0
  if (deviceId !== undefined && msg.schemaVersion !== undefined) {
    if (!isValidSchemaVersion(msg.schemaVersion)) {
      deps.sendError(conn, id, 'INVALID_MESSAGE', '"schemaVersion" must be a non-negative integer')
      return
    }
    schemaVersion = msg.schemaVersion
  }

  if (deps.hasSubscribeHook()) {
    for (const table of tables) {
      try {
        await deps.beforeSubscribe({ databaseId: state.databaseId, table, filter, identity: state.identity })
      } catch (err) {
        deps.sendSirannonError(conn, id, err)
        return
      }
    }
  }

  if (deviceId !== undefined) {
    let refusal: ReturnType<typeof schemaVersionGateRefusal>
    try {
      const serverVersion = highestMigrationVersion(await state.database.appliedMigrations())
      refusal = schemaVersionGateRefusal(schemaVersion, serverVersion)
    } catch (err) {
      deps.sendSirannonError(conn, id, err)
      return
    }
    if (refusal !== null) {
      deps.sendError(conn, id, refusal.code, refusal.message)
      return
    }
  }

  if (deviceId !== undefined) {
    await subscribeDevice(deps, conn, state, {
      id,
      tables,
      filter,
      sinceSeq,
      clientEpoch,
      deviceId,
      stagedStream: msg.stagedStream === true,
    })
    return
  }

  if (msg.stagedStream !== undefined) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"stagedStream" requires a "deviceId"')
    return
  }

  if (tables.length > 1) {
    deps.sendError(conn, id, 'INVALID_MESSAGE', '"tables" requires a "deviceId"')
    return
  }

  const table = tables[0]
  if (sinceSeq === undefined) {
    await subscribeLive(deps, conn, state, id, table, filter)
    return
  }

  await subscribeResuming(deps, conn, state, id, table, filter, sinceSeq, clientEpoch)
}

export function releaseUnattached(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  id: string,
  subscription: Subscription,
  attachment: SubscriptionAttachment,
): void {
  subscription.unsubscribe()
  deps.cdc.maybeCleanup(state.databaseId)
  if (attachment === 'duplicate') {
    deps.sendError(conn, id, 'DUPLICATE_SUBSCRIPTION', `Subscription '${id}' already exists on this connection`)
  }
}

function readTableSet(msg: Record<string, unknown>): string[] | string {
  if (msg.tables !== undefined) {
    if (!Array.isArray(msg.tables) || msg.tables.length === 0) {
      return '"tables" must be a non-empty array of table names'
    }
    if (msg.tables.length > MAX_SUBSCRIBED_TABLES) {
      return `"tables" must hold at most ${MAX_SUBSCRIBED_TABLES} table names`
    }
    for (const table of msg.tables) {
      if (typeof table !== 'string' || table.length === 0) {
        return '"tables" must hold non-empty table names'
      }
    }
    return msg.tables as string[]
  }

  if (typeof msg.table !== 'string') {
    return 'Subscribe message requires a "table" string field'
  }
  return [msg.table]
}

async function subscribeLive(
  deps: WSSubscribeDeps,
  conn: WSConnection,
  state: ConnectionState,
  id: string,
  table: string,
  filter: Record<string, unknown> | undefined,
): Promise<void> {
  let subscription: Subscription | null = null
  try {
    const ctx = await deps.cdc.ensure(state.databaseId, state.database)
    await ctx.tracker.watch(ctx.cdcConn, table)
    await state.database.ensureChangeStamping()

    const grouper = new TransactionGrouper(event => deps.sendChange(conn, id, event) !== 'dropped')
    const boundary = ctx.tracker.cursor
    const sub = ctx.manager.subscribe(table, filter, (event: ChangeEvent) => {
      grouper.receive(event)
    })
    const removeBatchEnd = ctx.manager.addBatchEndListener(atTxBoundary => {
      grouper.flush(atTxBoundary)
    })

    subscription = {
      unsubscribe: () => {
        removeBatchEnd()
        sub.unsubscribe()
      },
    }
    const attachment = deps.attachSubscription(conn, id, subscription)
    if (attachment !== 'attached') {
      releaseUnattached(deps, conn, state, id, subscription, attachment)
      return
    }
    deps.sendSubscribed(conn, id, boundary.toString(), ctx.epoch, false)
  } catch (err) {
    if (subscription) deps.detachSubscription(conn, id, subscription)
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
): Promise<void> {
  let ctx: Awaited<ReturnType<CdcContextRegistry['ensure']>>
  let primed: PrimedSubscription
  let boundary: bigint
  let resync: boolean
  let goLive: () => void
  let subscription: Subscription | null = null
  try {
    ctx = await deps.cdc.ensure(state.databaseId, state.database)
    await ctx.tracker.watch(ctx.cdcConn, table)
    await state.database.ensureChangeStamping()

    const grouper = new TransactionGrouper(event => deps.sendChange(conn, id, event) !== 'dropped')
    const deliver = (event: ChangeEvent): WSSendOutcome => (grouper.receive(event) ? 'sent' : 'dropped')
    boundary = ctx.tracker.cursor
    const boundaryEndsTransaction = ctx.tracker.pollEndedAtTxBoundary
    primed = new PrimedSubscription(sinceSeq, deliver, () => deps.handleOverload(conn))
    const sub = ctx.manager.subscribe(table, filter, event => primed.onLiveEvent(event))

    let removeBatchEnd = (): void => {}
    let cancelled = false
    goLive = () => {
      if (cancelled) return
      grouper.flush(boundaryEndsTransaction)
      primed.goLive()
      removeBatchEnd = ctx.manager.addBatchEndListener(atTxBoundary => {
        grouper.flush(atTxBoundary)
      })
    }
    subscription = {
      unsubscribe: () => {
        cancelled = true
        removeBatchEnd()
        sub.unsubscribe()
      },
    }
    const attachment = deps.attachSubscription(conn, id, subscription)
    if (attachment !== 'attached') {
      releaseUnattached(deps, conn, state, id, subscription, attachment)
      return
    }

    const minSeq = await ctx.tracker.getMinSeq(ctx.cdcConn)
    const foreignEpoch = clientEpoch !== undefined && clientEpoch !== ctx.epoch
    resync = foreignEpoch || needsResync(sinceSeq, minSeq, boundary)
    deps.sendSubscribed(conn, id, boundary.toString(), ctx.epoch, resync)
  } catch (err) {
    if (subscription) deps.detachSubscription(conn, id, subscription)
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
  goLive()
}
