import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { ChangeEvent } from '../../core/types.js'
import type {
  WSChangeMessage,
  WSChangesMessage,
  WSClientMessage,
  WSSubscribedMessage,
  WSWireChangeEvent,
} from '../../server/protocol.js'

export interface ActiveSubscription {
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
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
}

export function applySubscribedMessage(sub: ActiveSubscription, msg: WSSubscribedMessage): void {
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
    try {
      sub.onReset?.()
    } catch {}
  } else if (sub.lastSeq === undefined && baseline !== undefined) {
    sub.lastSeq = baseline
  }
  try {
    sub.onSubscribed?.({
      seq: baseline,
      epoch: sub.epoch,
      resync: msg.resync === true,
      maxUnacknowledgedChanges: msg.maxUnacknowledgedChanges,
    })
  } catch {}
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
  try {
    const event: ChangeEvent = {
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
    if (sub.lastSeq === undefined || event.seq > sub.lastSeq) {
      sub.lastSeq = event.seq
    }
    sub.callback(event)
  } catch {}
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
