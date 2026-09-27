import { TransactionGrouper } from '../core/cdc/transaction-grouper.js'
import type { ChangeEvent } from '../core/types.js'
import type { WSSendOutcome } from './ws-connection.js'
import type { DeviceFramePacker, FrameAppendOutcome } from './ws-device-frames.js'

export const DEFAULT_MAX_UNACKNOWLEDGED_CHANGES = 1_000

const MAX_CATCHUP_READ_BATCH = 1_000
const MIN_CATCHUP_READ_BATCH = 32

/**
 * How the delivery window paces the stream. Under `perTransaction`, which suits
 * a device that applies each transaction from memory, the stream checks the
 * window only before a new transaction starts, so the device receives a
 * transaction larger than the window whole. Under `perEvent`, which suits a
 * device that stages changes to disk and acknowledges them as it stages them,
 * the stream can pause at any event, because the device keeps sending
 * acknowledgements in the middle of a transaction.
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
 * In `live` mode, the poller passes each event through a one-event lookahead
 * that sets the `txEnd` flag, so the stream holds back at most one event. When
 * the stream cannot send, because the delivery window is full or uWebSockets
 * reports backpressure on the socket, it switches to `catchup` mode. In that mode it buffers
 * no events and records the position where it stopped, then reads the missing
 * range from the change log once the device acknowledges more changes or the
 * socket drains. The stream needs no buffer of its own, because the server
 * keeps every change that a live device has not acknowledged in the change log.
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
   * Sends a ping on a socket whose buffered bytes have stopped flushing.
   * uWebSockets keeps the rest of a partial write queued until the next write
   * on that socket and fires no drain event in the meantime, so the last
   * changes of a stream with nothing more to send could stay in the buffer.
   * When the buffered count is above zero, unchanged since the previous poll
   * batch, and below the congestion threshold, this method sends a ping so that
   * uWebSockets flushes the rest. It skips a congested socket, because that
   * socket drains under its own flow control and one more frame could push it
   * past the backpressure limit.
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
   * Makes the stream wait for a drain event only while the socket is congested.
   * uWebSockets reports `buffered` for any send that it cannot finish at once,
   * including one that leaves only a few hundred bytes of a frame queued, and
   * it fires a drain event only when the socket becomes writable again. If the
   * stream waited on every `buffered` outcome, it could wait on a small
   * remainder for a drain event that never fires.
   */
  private parkOnSocket(): void {
    this.socketWait = this.deps.socketCongested()
  }

  /**
   * Discards the in-flight stream state and switches to reading the change log.
   * The event that the grouper holds back is still unqueued, and its seq is
   * above both watermarks, so discarding it loses no change. The catch-up read
   * starts after the later of two positions. `highestQueuedSeq` is the last
   * event that the stream queued on the socket, and `processedSeq` can be further
   * ahead when the transform suppresses every event after that one, so the
   * stream skips a long sequence of the device's own echoes on every pause.
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
   * Starts the catch-up read, or sets a flag for another read when one is
   * already in progress. Without the flag, a read that stops partway because
   * of backpressure that clears before the read ends could leave the stream in
   * catch-up mode with no read scheduled to resume it.
   */
  private requestDrain(): void {
    if (this.draining) {
      this.wakeRequested = true
      return
    }
    void this.drain()
  }

  /**
   * Returns true when the catch-up read must pause for a full delivery window,
   * which happens at any event under `perEvent` pacing and only at a
   * transaction boundary under `perTransaction` pacing. A `perTransaction`
   * device acknowledges only whole transactions that it has applied, so if
   * the stream held back the rest of an open transaction, it would wait for
   * an acknowledgement that the device never sends.
   */
  private readGateClosed(): boolean {
    if (!this.windowClosed()) return false
    return this.deps.pacing === 'perEvent' || !this.midTransaction
  }

  /**
   * Sets the size of the next catch-up read to twice the number of events that
   * the stream offered from the last read, between 32 and 1,000. When a read stops
   * at a full socket, the stream discards every event after the one where it
   * stopped, so with a larger fixed batch the server would decode the same rows again on
   * every pause for a device on a congested link. Doubling the count gives a
   * caught-up device full batches and a paced device batches close to the
   * number of events that its socket accepts.
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
   * Switches the stream back to the live feed. This method is synchronous and
   * follows a log read directly, so that no poller tick can dispatch events
   * between the caught-up check and the switch to `live` mode. The stream keeps its grouper across the switch,
   * and when the poller's last batch ends at a transaction boundary, the
   * boundary flush releases the event that the grouper holds, as it does for
   * an ordinary subscription that resumes.
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
