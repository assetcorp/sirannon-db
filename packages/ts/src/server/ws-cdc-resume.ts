import type { ChangeTracker } from '../core/cdc/change-tracker.js'
import { encodeTaggedValues } from '../core/cdc/encoding.js'
import { filteredChange } from '../core/cdc/subscription.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'

const REPLAY_BATCH_SIZE = 1000
const DEFAULT_PRIME_BUFFER_LIMIT = 10_000
const DEFAULT_PRIME_BUFFER_BYTES = 16 * 1_048_576

function approxEventBytes(event: ChangeEvent): number {
  let bytes = 0
  if (event.row) bytes += JSON.stringify(encodeTaggedValues(event.row)).length
  if (event.oldRow) bytes += JSON.stringify(encodeTaggedValues(event.oldRow)).length
  return bytes
}

export function needsResync(sinceSeq: bigint, minSeq: bigint | null, boundary: bigint): boolean {
  if (sinceSeq >= boundary) return false
  if (minSeq === null) return true
  return minSeq > sinceSeq + 1n
}

export class PrimedSubscription {
  private mode: 'priming' | 'live' | 'stopped' = 'priming'
  private buffer: ChangeEvent[] = []
  private bufferBytes = 0
  private readonly bufferLimit: number
  private readonly bufferByteLimit: number

  constructor(
    private readonly sinceSeq: bigint,
    private readonly deliver: (event: ChangeEvent) => WSSendOutcome,
    private readonly onOverload: () => void,
    bufferLimit: number = DEFAULT_PRIME_BUFFER_LIMIT,
    bufferByteLimit: number = DEFAULT_PRIME_BUFFER_BYTES,
  ) {
    this.bufferLimit = bufferLimit
    this.bufferByteLimit = bufferByteLimit
  }

  onLiveEvent(event: ChangeEvent): void {
    if (this.mode === 'stopped') return
    if (this.mode === 'live') {
      this.emit(event)
      return
    }
    const nextBytes = this.bufferBytes + approxEventBytes(event)
    if (this.buffer.length >= this.bufferLimit || nextBytes > this.bufferByteLimit) {
      this.stop()
      this.onOverload()
      return
    }
    this.buffer.push(event)
    this.bufferBytes = nextBytes
  }

  replay(
    tracker: ChangeTracker,
    conn: SQLiteConnection,
    table: string,
    filter: Record<string, unknown> | undefined,
    boundary: bigint,
  ): Promise<void> {
    return this.replayTables(tracker, conn, [table], filter, boundary)
  }

  async replayTables(
    tracker: ChangeTracker,
    conn: SQLiteConnection,
    tables: readonly string[],
    filter: Record<string, unknown> | undefined,
    boundary: bigint,
  ): Promise<void> {
    let cursor = this.sinceSeq
    while (this.mode === 'priming' && cursor < boundary) {
      const events = await tracker.readSinceTables(conn, tables, cursor, boundary, REPLAY_BATCH_SIZE)
      if (events.length === 0) break

      for (const event of events) {
        const delivered = filter === undefined ? event : filteredChange(event, filter)
        if (delivered === null) continue
        if (!this.emit(delivered)) return
      }

      const lastEvent = events[events.length - 1]
      if (lastEvent === undefined) break
      cursor = lastEvent.seq
      if (events.length < REPLAY_BATCH_SIZE) break
    }
  }

  goLive(): void {
    if (this.mode !== 'priming') return
    const pending = this.buffer
    this.buffer = []
    for (const event of pending) {
      if (!this.emit(event)) return
    }
    if (this.mode === 'priming') {
      this.mode = 'live'
    }
  }

  private emit(event: ChangeEvent): boolean {
    if (this.mode === 'stopped') return false
    if (event.seq <= this.sinceSeq) return true
    const outcome = this.deliver(event)
    if (outcome === 'dropped') {
      this.stop()
      return false
    }
    return true
  }

  private stop(): void {
    this.mode = 'stopped'
    this.buffer = []
    this.bufferBytes = 0
  }
}
