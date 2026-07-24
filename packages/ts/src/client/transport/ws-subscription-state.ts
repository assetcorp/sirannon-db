import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { ChangeEvent } from '../../core/types.js'
import type { WSChangeMessage, WSClientMessage, WSSubscribedMessage } from '../../server/protocol.js'

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
    sub.onSubscribed?.({
      seq: baseline,
      epoch: sub.epoch,
      resync: msg.resync === true,
      maxUnacknowledgedChanges: msg.maxUnacknowledgedChanges,
    })
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
      ...(msg.event.rowId !== undefined ? { rowId: msg.event.rowId } : {}),
      ...(msg.event.txId !== undefined ? { txId: msg.event.txId } : {}),
      ...(msg.event.txEnd === true ? { txEnd: true } : {}),
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
  }
}
