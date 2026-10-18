import { invokeCallerCallback, reportCallerFailure } from '../../core/caller-callbacks.js'
import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { ChangeEvent } from '../../core/types.js'
import type {
  WSChangeMessage,
  WSChangesMessage,
  WSClientMessage,
  WSSubscribedMessage,
  WSWireChangeEvent,
} from '../../server/protocol.js'
import { RemoteError } from '../types.js'

export interface ActiveSubscription {
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
  onError: ((error: Error) => void) | undefined
  onReset: (() => void) | undefined
  onSubscribed:
    | ((info: {
        seq: bigint | undefined
        epoch: string | undefined
        resync: boolean
        maxUnacknowledgedChanges: number | undefined
      }) => void)
    | undefined
  deviceId: string | undefined
  tables: readonly string[] | undefined
  schemaVersion: number | undefined
  lastSeq: bigint | undefined
  resumeSeq: (() => bigint | undefined) | undefined
  epoch: string | undefined
  stagedStream: boolean | undefined
  established: boolean
}

const UNREACHABLE_SERVER_CODES: ReadonlySet<string> = new Set(['CONNECTION_ERROR', 'TIMEOUT'])

/**
 * Marks every subscription as not yet restored on the server, which the transport does when its connection drops, so
 * that {@link restoreSubscriptions} subscribes each one again on the next connection.
 *
 * @param subscriptions - The subscriptions that the transport holds, keyed by subscription identifier.
 * @internal
 */
export function markSubscriptionsDisconnected(subscriptions: Map<string, ActiveSubscription>): void {
  for (const sub of subscriptions.values()) {
    sub.established = false
  }
}

/**
 * Subscribes again, one at a time, to every subscription that the new connection has not restored, resuming each one
 * from the last sequence that it delivered.
 *
 * A subscription whose restore the server refuses leaves the map, and its `onError` handler receives the refusal. A
 * `DUPLICATE_SUBSCRIPTION` answer means that the connection already holds the subscription, so the subscription
 * stays. A restore that fails with `CONNECTION_ERROR` or `TIMEOUT` keeps the subscription for the next connection.
 *
 * @param subscriptions - The subscriptions that the transport holds, keyed by subscription identifier.
 * @param request - Sends one subscribe message and resolves once the server confirms it.
 * @param isClosed - Returns whether the transport has closed, which stops the restore.
 * @returns The last `CONNECTION_ERROR` or `TIMEOUT` error, which the transport throws so that it tries the restore
 * again, or `null` when every restore reached the server.
 * @internal
 */
export async function restoreSubscriptions(
  subscriptions: Map<string, ActiveSubscription>,
  request: (message: WSClientMessage) => Promise<void>,
  isClosed: () => boolean,
): Promise<RemoteError | null> {
  let unreachable: RemoteError | null = null

  for (const [id, sub] of [...subscriptions.entries()]) {
    if (isClosed()) break
    if (sub.established || subscriptions.get(id) !== sub) continue
    try {
      await request(buildResubscribeMessage(id, sub))
    } catch (err) {
      if (err instanceof RemoteError && UNREACHABLE_SERVER_CODES.has(err.code)) {
        unreachable = err
        continue
      }
      if (err instanceof RemoteError && err.code === 'DUPLICATE_SUBSCRIPTION') {
        sub.established = true
        continue
      }
      if (subscriptions.get(id) !== sub) continue
      subscriptions.delete(id)
      reportCallerFailure(sub.onError, err)
    }
  }

  return unreachable
}

export function applySubscribedMessage(sub: ActiveSubscription, msg: WSSubscribedMessage): void {
  sub.established = true
  if (msg.epoch !== undefined) {
    sub.epoch = msg.epoch
  }
  let baseline: bigint | undefined
  if (msg.seq !== undefined) {
    try {
      baseline = BigInt(msg.seq)
    } catch {
      baseline = undefined
    }
  }
  if (msg.resync) {
    sub.lastSeq = baseline
    invokeCallerCallback(() => sub.onReset?.(), sub.onError)
  } else if (sub.lastSeq === undefined && baseline !== undefined) {
    sub.lastSeq = baseline
  }
  invokeCallerCallback(
    () =>
      sub.onSubscribed?.({
        seq: baseline,
        epoch: sub.epoch,
        resync: msg.resync === true,
        maxUnacknowledgedChanges: msg.maxUnacknowledgedChanges,
      }),
    sub.onError,
  )
}

export function deliverChangeMessage(sub: ActiveSubscription, msg: WSChangeMessage): void {
  deliverWireEvent(sub, msg.event)
}

export function deliverChangesMessage(sub: ActiveSubscription, msg: WSChangesMessage): void {
  if (!Array.isArray(msg.events)) return
  for (const event of msg.events) {
    deliverWireEvent(sub, event)
  }
}

function deliverWireEvent(sub: ActiveSubscription, wire: WSWireChangeEvent): void {
  let event: ChangeEvent
  try {
    event = {
      type: wire.type,
      table: wire.table,
      row: decodeTaggedValues(wire.row) as Record<string, unknown>,
      oldRow: wire.oldRow === undefined ? undefined : (decodeTaggedValues(wire.oldRow) as Record<string, unknown>),
      seq: BigInt(wire.seq),
      timestamp: wire.timestamp,
      ...(wire.hlc !== undefined ? { hlc: wire.hlc } : {}),
      ...(wire.origin !== undefined ? { origin: wire.origin } : {}),
      ...(wire.rowId !== undefined ? { rowId: wire.rowId } : {}),
      ...(wire.txId !== undefined ? { txId: wire.txId } : {}),
      ...(wire.txEnd === true ? { txEnd: true } : {}),
    }
  } catch (err) {
    reportCallerFailure(sub.onError, err)
    return
  }
  if (sub.lastSeq === undefined || event.seq > sub.lastSeq) {
    sub.lastSeq = event.seq
  }
  invokeCallerCallback(() => sub.callback(event), sub.onError)
}

export function buildResubscribeMessage(id: string, sub: ActiveSubscription): WSClientMessage {
  const resumeFrom = sub.resumeSeq === undefined ? sub.lastSeq : sub.resumeSeq()
  return {
    type: 'subscribe',
    id,
    table: sub.table,
    ...(sub.tables !== undefined ? { tables: [...sub.tables] } : {}),
    ...(sub.filter ? { filter: encodeTaggedValues(sub.filter) as Record<string, unknown> } : {}),
    ...(resumeFrom !== undefined ? { sinceSeq: resumeFrom.toString() } : {}),
    ...(sub.epoch !== undefined ? { epoch: sub.epoch } : {}),
    ...(sub.deviceId !== undefined ? { deviceId: sub.deviceId } : {}),
    ...(sub.schemaVersion !== undefined ? { schemaVersion: sub.schemaVersion } : {}),
    ...(sub.stagedStream === true ? { stagedStream: true } : {}),
  }
}
