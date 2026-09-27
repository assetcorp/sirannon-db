import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'
import type { WSWireChangeEvent } from './ws-server-messages.js'

/**
 * The target size of one packed `changes` frame. The packer adds events to a
 * frame until the next one would take it past this size, and it sends an event
 * larger than the whole target in a frame of its own. At 64 KiB, a frame is
 * 1/16 of the default 1 MiB `maxBodyBytes` and 1/256 of the default 16 MiB
 * backpressure limit, while a full delivery window of 1,000 ordinary changes
 * still spans several frames.
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

/** The outcome of adding an event to the packer, which is the send outcome when the packer sends a frame, or `queued` when it keeps the event for a later frame. */
export type FrameAppendOutcome = WSSendOutcome | 'queued'

/**
 * Packs change events into `changes` frames up to a size target. The packer
 * serialises each event once, builds the frame from those strings, and
 * measures the frame by string length.
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
