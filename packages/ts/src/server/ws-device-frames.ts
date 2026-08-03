import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'
import type { WSWireChangeEvent } from './ws-protocol.js'

/**
 * Target size of one packed `changes` frame. A soft limit in the manner of
 * MySQL's row-event grouping: events are packed until the next one would
 * cross it, and a single event larger than the whole target is sent alone
 * rather than split. 64 KiB keeps a frame at 1/16 of the 1 MiB
 * `maxBodyBytes` frame ceiling and 1/256 of the 16 MiB send-backpressure
 * allowance, while a full 1,000-change delivery window of ordinary rows
 * still spans several frames instead of collapsing into one burst.
 */
export const DEVICE_FRAME_TARGET_BYTES = 65_536

export function wireChangeEvent(event: ChangeEvent): WSWireChangeEvent {
  return {
    type: event.type,
    table: event.table,
    row: encodeTaggedValues(event.row) as Record<string, unknown>,
    oldRow: event.oldRow === undefined ? undefined : (encodeTaggedValues(event.oldRow) as Record<string, unknown>),
    seq: event.seq.toString(),
    timestamp: event.timestamp,
    ...(event.hlc !== undefined ? { hlc: event.hlc } : {}),
    ...(event.origin !== undefined ? { origin: event.origin } : {}),
    ...(event.rowId !== undefined ? { rowId: event.rowId } : {}),
    ...(event.txId !== undefined ? { txId: event.txId } : {}),
    ...(event.txEnd === true ? { txEnd: true } : {}),
  }
}

/** The outcome of offering an event to the packer: a send outcome when a frame went out, or `queued`. */
export type FrameAppendOutcome = WSSendOutcome | 'queued'

/**
 * Packs encoded change events into `changes` frames bounded by a byte
 * target. Each event is serialised once; the frame is assembled from the
 * serialised pieces so that the byte accounting is exact.
 */
export class DeviceFramePacker {
  private parts: string[] = []
  private bytes = 0

  constructor(
    private readonly subscriptionId: string,
    private readonly sendText: (data: string) => WSSendOutcome,
    private readonly targetBytes: number = DEVICE_FRAME_TARGET_BYTES,
  ) {}

  append(event: ChangeEvent): FrameAppendOutcome {
    let encoded: string
    try {
      encoded = JSON.stringify(wireChangeEvent(event))
    } catch {
      return 'dropped'
    }

    let outcome: FrameAppendOutcome = 'queued'
    if (this.parts.length > 0 && this.bytes + encoded.length > this.targetBytes) {
      outcome = this.flush()
      if (outcome === 'dropped') return outcome
    }

    this.parts.push(encoded)
    this.bytes += encoded.length

    if (this.bytes >= this.targetBytes) {
      outcome = worstOutcome(outcome, this.flush())
    }
    return outcome
  }

  flush(): FrameAppendOutcome {
    if (this.parts.length === 0) return 'queued'
    const frame = `{"type":"changes","id":${JSON.stringify(this.subscriptionId)},"events":[${this.parts.join(',')}]}`
    this.parts = []
    this.bytes = 0
    return this.sendText(frame)
  }

  clear(): void {
    this.parts = []
    this.bytes = 0
  }

  get pendingEvents(): number {
    return this.parts.length
  }
}

const OUTCOME_SEVERITY: Record<FrameAppendOutcome, number> = {
  queued: 0,
  sent: 1,
  buffered: 2,
  dropped: 3,
}

function worstOutcome(a: FrameAppendOutcome, b: FrameAppendOutcome): FrameAppendOutcome {
  return OUTCOME_SEVERITY[a] >= OUTCOME_SEVERITY[b] ? a : b
}
