import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'
import type { WSWireChangeEvent } from './ws-server-messages.js'

/**
 * The target size of one packed `changes` frame. The packer adds events to a
 * frame until the next one would take it past this size, and it sends an event
 * larger than the whole target in a separate frame. At 64 KiB, a frame is
 * 1/16 of the default 1 MiB `maxBodyBytes` and 1/256 of the default 16 MiB
 * backpressure limit, while a full delivery window of 1,000 ordinary changes
 * still spans several frames.
 */
export const DEVICE_FRAME_TARGET_BYTES = 65_536

const FRAMES_PER_RESUME_THRESHOLD = 2

/**
 * Returns the frame size target for a connection, which is {@link DEVICE_FRAME_TARGET_BYTES} or half of the buffered
 * byte count below which a paused stream resumes, whichever is smaller.
 *
 * A stream sends a frame only while its socket buffers less than that resume count, which is half of
 * `maxWebSocketBackpressureBytes`. A frame of at most a quarter of the bound therefore leaves the buffer under the
 * bound, so that the stream never closes its connection with close code 4290.
 *
 * @param socketResumeBytes - The buffered byte count below which a paused stream resumes sending.
 * @returns The size in bytes at which the packer sends a frame.
 * @internal
 */
export function deviceFrameTargetBytes(socketResumeBytes: number): number {
  return Math.max(1, Math.min(DEVICE_FRAME_TARGET_BYTES, Math.floor(socketResumeBytes / FRAMES_PER_RESUME_THRESHOLD)))
}

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

/**
 * One change event in its wire form, as JSON text, with that text's length in UTF-8 bytes.
 *
 * @internal
 */
export interface EncodedChangeEvent {
  json: string
  bytes: number
}

const encodedChangeEvents = new WeakMap<ChangeEvent, EncodedChangeEvent>()

/**
 * Returns the wire JSON of a change event, which the server computes once per event object and shares with every
 * subscription that receives that object, so that fanning one change out to many devices encodes it once.
 *
 * @param event - The event, which no caller may change once it has been encoded.
 * @returns The JSON text and its length in UTF-8 bytes.
 * @throws The error from `JSON.stringify` when the event holds a value that cannot be serialised.
 * @internal
 */
export function encodedChangeEvent(event: ChangeEvent): EncodedChangeEvent {
  let encoded = encodedChangeEvents.get(event)
  if (encoded === undefined) {
    const json = JSON.stringify(wireChangeEvent(event))
    encoded = { json, bytes: Buffer.byteLength(json) }
    encodedChangeEvents.set(event, encoded)
  }
  return encoded
}

/** The outcome of adding an event to the packer, which is the send outcome when the packer sends a frame, or `queued` when it keeps the event for a later frame. */
export type FrameAppendOutcome = WSSendOutcome | 'queued'

/**
 * Packs change events into `changes` frames up to a size target. The packer
 * takes each event's JSON from {@link encodedChangeEvent}, builds the frame from
 * those strings, and measures the frame in UTF-8 bytes, which is what the socket buffers.
 */
export class DeviceFramePacker {
  private parts: string[] = []
  private bytes = 0
  private readonly frameStart: string

  constructor(
    subscriptionId: string,
    private readonly sendText: (data: string) => WSSendOutcome,
    private readonly targetBytes: number = DEVICE_FRAME_TARGET_BYTES,
  ) {
    this.frameStart = `{"type":"changes","id":${JSON.stringify(subscriptionId)},"events":[`
  }

  append(event: ChangeEvent): FrameAppendOutcome {
    let encoded: EncodedChangeEvent
    try {
      encoded = encodedChangeEvent(event)
    } catch {
      return 'dropped'
    }

    const encodedBytes = encoded.bytes
    let outcome: FrameAppendOutcome = 'queued'
    if (this.parts.length > 0 && this.bytes + encodedBytes > this.targetBytes) {
      outcome = this.flush()
      if (outcome === 'dropped') return outcome
    }

    this.parts.push(encoded.json)
    this.bytes += encodedBytes

    if (this.bytes >= this.targetBytes) {
      outcome = worstOutcome(outcome, this.flush())
    }
    return outcome
  }

  flush(): FrameAppendOutcome {
    if (this.parts.length === 0) return 'queued'
    const frame = `${this.frameStart}${this.parts.join(',')}]}`
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
