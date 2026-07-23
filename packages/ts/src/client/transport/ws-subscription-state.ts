import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { ChangeEvent } from '../../core/types.js'
import type { WSChangeMessage, WSClientMessage, WSSubscribedMessage } from '../../server/protocol.js'

export interface ActiveSubscription {
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
  onReset: (() => void) | undefined
  onSubscribed: ((info: { seq: bigint | undefined; epoch: string | undefined; resync: boolean }) => void) | undefined
  deviceId: string | undefined
  lastSeq: bigint | undefined
  epoch: string | undefined
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
    } catch {
      /* a failing reset handler must not disrupt message processing */
    }
  } else if (sub.lastSeq === undefined && baseline !== undefined) {
    sub.lastSeq = baseline
  }
  try {
    sub.onSubscribed?.({ seq: baseline, epoch: sub.epoch, resync: msg.resync === true })
  } catch {}
}

export function deliverChangeMessage(sub: ActiveSubscription, msg: WSChangeMessage): void {
  try {
    const event: ChangeEvent = {
      type: msg.event.type,
      table: msg.event.table,
      row: decodeTaggedValues(msg.event.row) as Record<string, unknown>,
      oldRow:
        msg.event.oldRow === undefined ? undefined : (decodeTaggedValues(msg.event.oldRow) as Record<string, unknown>),
      seq: BigInt(msg.event.seq),
      timestamp: msg.event.timestamp,
      ...(msg.event.hlc !== undefined ? { hlc: msg.event.hlc } : {}),
      ...(msg.event.origin !== undefined ? { origin: msg.event.origin } : {}),
    }
    if (sub.lastSeq === undefined || event.seq > sub.lastSeq) {
      sub.lastSeq = event.seq
    }
    sub.callback(event)
  } catch {
    /* malformed data and subscriber callback errors must not disrupt the processing loop */
  }
}

export function buildResubscribeMessage(id: string, sub: ActiveSubscription): WSClientMessage {
  return {
    type: 'subscribe',
    id,
    table: sub.table,
    ...(sub.filter ? { filter: encodeTaggedValues(sub.filter) as Record<string, unknown> } : {}),
    ...(sub.lastSeq !== undefined ? { sinceSeq: sub.lastSeq.toString() } : {}),
    ...(sub.epoch !== undefined ? { epoch: sub.epoch } : {}),
    ...(sub.deviceId !== undefined ? { deviceId: sub.deviceId } : {}),
  }
}
