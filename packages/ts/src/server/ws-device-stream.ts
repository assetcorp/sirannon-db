import { TransactionGrouper } from '../core/cdc/transaction-grouper.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'
import type { DeviceFramePacker, FrameAppendOutcome } from './ws-device-frames.js'

export const DEFAULT_MAX_UNACKNOWLEDGED_CHANGES = 1_000

const MAX_CATCHUP_READ_BATCH = 1_000
const MIN_CATCHUP_READ_BATCH = 32

/**
 * How the delivery window paces the stream. `perTransaction` is the
 * contract for a device that applies a transaction from memory: the window
 * is checked only before a new transaction starts so that a transaction larger
 * than the window is still delivered whole. `perEvent` is the contract for
 * a device that stages to disk and acknowledges staged changes: the window
 * may pause the stream anywhere, because acknowledgements keep arriving
 * mid-transaction.
 */
export type DeviceStreamPacing = 'perTransaction' | 'perEvent'

export interface DeviceStreamDeps {
  deviceId: string
  maxUnacknowledgedChanges: number
  pacing: DeviceStreamPacing
  packer: DeviceFramePacker | null
  sendEvent(event: ChangeEvent): WSSendOutcome
  socketBuffered(): number
  socketCongested(): boolean
  flushSocket(): void
  onOverload(): void
  onFault(err: unknown): void
  readLog(afterSeq: bigint, upToSeq: bigint, limit: number): Promise<ChangeEvent[]>
  logCursor(): bigint
  logCursorAtTxBoundary(): boolean
  transform(event: ChangeEvent): ChangeEvent | null
}

/**
 * Streams the change log to one device subscription with bounded memory.
 *
 * In `live` mode events flow straight from the poller through a one-event
 * lookahead that resolves the `txEnd` flag, so at most one event is ever
 * held back. Whenever the stream cannot send, because the delivery window is
 * full or the socket reports backpressure, it switches to `catchup` mode:
 * nothing is buffered, the position of the last queued event is remembered,
 * and the gap is re-read from the change log once an acknowledgement or a
 * socket drain reopens the way. The change log is the buffer; retention
 * already keeps every row a live device has not acknowledged.
 */
export class DeviceChangeStream {
  private mode: 'live' | 'catchup'
  private grouper: TransactionGrouper | null = null
  private catchupFrom: bigint
  private highestQueuedSeq: bigint
  private processedSeq: bigint
  private heldSeq: bigint | null = null
  private ackedSeq: bigint
  private lastQueuedTxId: string | undefined
  private midTransaction = false
  private draining = false
  private wakeRequested = false
  private socketWait = false
  private halted = false
  private lastBuffered = 0
  private catchupBatch = MAX_CATCHUP_READ_BATCH

  constructor(
    private readonly deps: DeviceStreamDeps,
    baselineSeq: bigint,
    startMode: 'live' | 'catchup',
  ) {
    this.mode = startMode
    this.catchupFrom = baselineSeq
    this.highestQueuedSeq = baselineSeq
    this.processedSeq = baselineSeq
    this.ackedSeq = baselineSeq
  }

  get deviceId(): string {
    return this.deps.deviceId
  }

  get stopped(): boolean {
    return this.halted
  }

  get catchingUp(): boolean {
    return this.mode === 'catchup'
  }

  start(): void {
    if (this.mode === 'catchup') {
      this.requestDrain()
    }
  }

  receiveLive(event: ChangeEvent): void {
    if (this.halted || this.mode !== 'live') return
    this.ensureGrouper().receive(event)
  }

  onBatchEnd(atTxBoundary: boolean): void {
    if (this.halted) return
    this.nudgeStalledSocket()
    if (this.socketWait && !this.deps.socketCongested()) {
      this.onSocketDrain()
    }
    if (this.mode !== 'live') return
    if (this.grouper !== null && !this.grouper.flush(atTxBoundary)) return
    this.settleFrame(this.deps.packer?.flush() ?? 'queued')
  }

  onAck(seq: bigint): void {
    if (this.halted) return
    if (seq > this.ackedSeq) {
      this.ackedSeq = seq
    }
    if (this.mode === 'catchup') {
      this.requestDrain()
    }
  }

  onSocketDrain(): void {
    if (this.halted) return
    this.socketWait = false
    if (this.mode === 'catchup') {
      this.requestDrain()
    }
  }

  stop(): void {
    this.halted = true
    this.grouper = null
    this.deps.packer?.clear()
  }

  /**
   * Moves a socket that stopped writing on its own. uWebSockets holds the
   * remainder of a partial write until the next write on that socket and
   * reports no drain in the meantime, so a stream that has queued everything
   * it has would leave the last changes sitting there. A small buffered count
   * that has not moved since the previous poll is that state, and a control
   * frame carries the remainder out. A socket holding enough to count as
   * congested is draining under its own flow control and takes nothing extra,
   * because another frame there would cross the backpressure limit.
   */
  private nudgeStalledSocket(): void {
    const buffered = this.deps.socketBuffered()
    if (buffered > 0 && buffered === this.lastBuffered && !this.deps.socketCongested()) {
      this.deps.flushSocket()
    }
    this.lastBuffered = buffered
  }

  private ensureGrouper(): TransactionGrouper {
    this.grouper ??= new TransactionGrouper(event => this.deliver(event))
    return this.grouper
  }

  private windowClosed(): boolean {
    return this.highestQueuedSeq - this.ackedSeq > BigInt(this.deps.maxUnacknowledgedChanges)
  }

  private deliver(event: ChangeEvent): boolean {
    if (this.halted) return false

    const startsTransaction = event.txId === undefined || event.txId !== this.lastQueuedTxId
    if (this.windowClosed() && (this.deps.pacing === 'perEvent' || startsTransaction)) {
      this.enterCatchup()
      return false
    }

    const outcome: FrameAppendOutcome =
      this.deps.packer === null ? this.deps.sendEvent(event) : this.deps.packer.append(event)
    if (outcome === 'dropped') {
      this.stop()
      this.deps.onOverload()
      return false
    }

    this.lastQueuedTxId = event.txId
    if (event.seq > this.highestQueuedSeq) {
      this.highestQueuedSeq = event.seq
    }
    if (event.seq > this.processedSeq) {
      this.processedSeq = event.seq
    }
    this.midTransaction = event.txEnd !== true && event.txId !== undefined

    if (outcome === 'buffered') {
      this.parkOnSocket()
      this.enterCatchup()
      return false
    }
    return true
  }

  /**
   * Waits for the socket only while it holds enough to be worth waiting for.
   * uWebSockets reports `buffered` for a send it could not finish in one go,
   * including one that queued a few hundred bytes of a frame it otherwise
   * sent, and it notifies a drain only when the socket becomes writable
   * again. A stream that parked on the outcome alone would wait on a small
   * remainder that the next send would have flushed, and the wake-up would
   * never come.
   */
  private parkOnSocket(): void {
    this.socketWait = this.deps.socketCongested()
  }

  /**
   * Abandons the in-flight stream state and falls back to the change log.
   * The grouper's held event was never queued and its seq is above both
   * watermarks, so dropping it loses nothing. The catch-up read resumes
   * after the last position fully settled: `highestQueuedSeq` is the last
   * event that reached the socket, and `processedSeq` may run further ahead
   * of it over a span whose every event was suppressed, so a long run of
   * the device's own echoes is not re-read on every pause.
   */
  private enterCatchup(): void {
    const flushed = this.deps.packer?.flush() ?? 'queued'
    if (flushed === 'dropped') {
      this.stop()
      this.deps.onOverload()
      return
    }
    if (flushed === 'buffered') {
      this.parkOnSocket()
    }
    this.mode = 'catchup'
    this.grouper = null
    this.heldSeq = null
    this.catchupFrom = this.processedSeq > this.highestQueuedSeq ? this.processedSeq : this.highestQueuedSeq
    if (!this.socketWait) {
      this.requestDrain()
    }
  }

  /**
   * Starts the catch-up read, or records that one is owed when a read is
   * already running. A read that hands back the stream mid-pass, because the
   * socket reported backpressure it no longer holds, would otherwise leave
   * the stream in catch-up with nothing scheduled to resume it.
   */
  private requestDrain(): void {
    if (this.draining) {
      this.wakeRequested = true
      return
    }
    void this.drain()
  }

  /**
   * A closed window stops the catch-up read only where the window may pace
   * the stream: anywhere under `perEvent` pacing, and at a transaction
   * boundary under `perTransaction` pacing. Mid-transaction the read keeps
   * going, because a `perTransaction` device acknowledges only applied
   * whole transactions, and holding the rest of an open transaction back
   * would wait for an acknowledgement that can never arrive.
   */
  private readGateClosed(): boolean {
    if (!this.windowClosed()) return false
    return this.deps.pacing === 'perEvent' || !this.midTransaction
  }

  /**
   * Sizes the next catch-up read from what the socket accepted this time.
   * A read that ends at a full socket discards everything past the event
   * that stopped it, so a device on a congested link would otherwise decode
   * the same rows on every pause. Doubling the accepted count keeps a
   * caught-up device reading whole batches while a paced one reads close to
   * what it can send.
   */
  private resizeCatchupBatch(accepted: number): void {
    const doubled = accepted * 2
    if (doubled < MIN_CATCHUP_READ_BATCH) {
      this.catchupBatch = MIN_CATCHUP_READ_BATCH
      return
    }
    this.catchupBatch = doubled > MAX_CATCHUP_READ_BATCH ? MAX_CATCHUP_READ_BATCH : doubled
  }

  private async drain(): Promise<void> {
    if (this.draining || this.halted) return
    this.draining = true
    try {
      while (!this.halted && this.mode === 'catchup') {
        this.wakeRequested = false
        if (this.socketWait || this.readGateClosed()) return

        const upTo = this.deps.logCursor()
        if (this.catchupFrom >= upTo) {
          this.goLive()
          return
        }

        let events: ChangeEvent[]
        try {
          events = await this.deps.readLog(this.catchupFrom, upTo, this.catchupBatch)
        } catch (err) {
          this.halted = true
          this.deps.onFault(err)
          return
        }
        if (this.halted || this.mode !== 'catchup') return

        if (events.length === 0) {
          this.catchupFrom = upTo
          this.goLive()
          return
        }

        const grouper = this.ensureGrouper()
        let offered = 0
        let stopped = false
        for (const event of events) {
          this.catchupFrom = event.seq
          offered += 1
          const delivered = this.deps.transform(event)
          if (delivered === null) {
            if (this.heldSeq === null && event.seq > this.processedSeq) {
              this.processedSeq = event.seq
            }
            continue
          }
          if (!grouper.receive(delivered)) {
            this.resizeCatchupBatch(offered)
            stopped = true
            break
          }
          const settledBefore = event.seq - 1n
          if (settledBefore > this.processedSeq) {
            this.processedSeq = settledBefore
          }
          this.heldSeq = delivered.txId === undefined ? null : event.seq
        }
        if (!stopped) {
          this.resizeCatchupBatch(this.catchupBatch)
        }
      }
    } finally {
      this.draining = false
      if (this.wakeRequested) {
        this.wakeRequested = false
        void this.drain()
      }
    }
  }

  /**
   * Rejoins the live feed. Runs synchronously right after a log read so that no
   * poller tick can dispatch between the caught-up check and the mode flip.
   * The grouper survives the transition: the event it holds is released by
   * the boundary flush when the poller stopped at a transaction boundary,
   * exactly as a resuming ordinary subscription does.
   */
  private goLive(): void {
    this.mode = 'live'
    const grouper = this.ensureGrouper()
    const atBoundary = this.deps.logCursorAtTxBoundary()
    if (!grouper.flush(atBoundary)) return
    if (atBoundary) {
      this.heldSeq = null
    }
    this.settleFrame(this.deps.packer?.flush() ?? 'queued')
  }

  private settleFrame(outcome: FrameAppendOutcome): void {
    if (this.halted) return
    if (outcome === 'dropped') {
      this.stop()
      this.deps.onOverload()
      return
    }
    if (outcome === 'buffered') {
      this.parkOnSocket()
      if (this.mode === 'live') {
        this.enterCatchup()
      }
    }
  }
}
