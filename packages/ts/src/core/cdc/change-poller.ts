import type { DatabaseChangeSignal } from './change-signal.js'

/**
 * The settings of a {@link ChangePoller}.
 *
 * @internal
 */
export interface ChangePollerOptions {
  /** The signal that tells the poller when the database may have changed. */
  signal: DatabaseChangeSignal
  /**
   * The shortest time between the starts of two reads, in milliseconds, which is also the longest that a committed
   * change waits before the poller reads it, and the read interval while the signal misses writes from other
   * processes.
   */
  intervalMs: number
  /**
   * Reads the change log once and delivers what it read.
   *
   * @returns `true` when the read found changes, so that the poller reads again after the interval in case more
   * arrived or the read stopped at its batch limit.
   */
  poll: () => Promise<boolean>
  /**
   * Receives a failed read and the number of reads in a row that have failed.
   *
   * @returns `true` to keep polling, or `false` to stop the poller.
   */
  onFailure: (error: unknown, consecutiveFailures: number) => boolean
}

/**
 * Reads a database's change log when its {@link DatabaseChangeSignal} fires, at most once per interval, so that a
 * committed change reaches subscribers within one interval while a database that nobody writes to costs no reads.
 *
 * A signal records that the database may have changed and schedules a read, which runs later on a timer, so a write
 * never waits for a read. While the signal cannot see writes from other processes, the poller reads once per
 * interval, as a timer-driven poller does.
 *
 * @internal
 */
export class ChangePoller {
  private stopListening: (() => void) | null = null
  private timer: ReturnType<typeof setTimeout> | null = null
  private timerDueAt = 0
  private reading: Promise<void> | null = null
  private changedSinceRead = false
  private lastReadStartedAt = Number.NEGATIVE_INFINITY
  private consecutiveFailures = 0

  constructor(private readonly options: ChangePollerOptions) {}

  /** Returns `true` between {@link ChangePoller.start} and {@link ChangePoller.stop}. */
  get running(): boolean {
    return this.stopListening !== null
  }

  /** Starts listening to the signal and schedules a first read at once. */
  start(): void {
    if (this.stopListening !== null) return
    this.consecutiveFailures = 0
    this.stopListening = this.options.signal.listen(this.wake)
    this.wake()
  }

  /** Stops listening and cancels the next read. A read under way finishes, and {@link ChangePoller.settled} waits for it. */
  stop(): void {
    const stop = this.stopListening
    this.stopListening = null
    stop?.()
    this.clearTimer()
  }

  /** Resolves once the read under way, if any, has finished. */
  async settled(): Promise<void> {
    await this.reading
  }

  /** Records that the database may have changed and schedules a read within one interval. */
  readonly wake = (): void => {
    if (this.stopListening === null) return
    this.changedSinceRead = true
    if (this.reading !== null) return
    this.arm(this.delayUntilNextRead())
  }

  private delayUntilNextRead(): number {
    const due = this.lastReadStartedAt + this.options.intervalMs - now()
    return due > 0 ? due : 0
  }

  private arm(delayMs: number): void {
    const dueAt = now() + delayMs
    if (this.timer !== null) {
      if (this.timerDueAt <= dueAt) return
      clearTimeout(this.timer)
    }
    this.timerDueAt = dueAt
    const timer = setTimeout(this.fire, delayMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
    this.timer = timer
  }

  private clearTimer(): void {
    if (this.timer === null) return
    clearTimeout(this.timer)
    this.timer = null
  }

  private readonly fire = (): void => {
    this.timer = null
    if (this.stopListening === null || this.reading !== null) return
    this.reading = this.options.signal.detached(() =>
      this.read().finally(() => {
        this.reading = null
        this.scheduleAfterRead()
      }),
    )
  }

  private async read(): Promise<void> {
    this.changedSinceRead = false
    this.lastReadStartedAt = now()
    try {
      if (await this.options.poll()) this.changedSinceRead = true
      this.consecutiveFailures = 0
    } catch (err) {
      this.consecutiveFailures++
      if (!this.options.onFailure(err, this.consecutiveFailures)) this.stop()
      else this.changedSinceRead = true
    }
  }

  private scheduleAfterRead(): void {
    if (this.stopListening === null) return
    if (this.changedSinceRead) {
      this.arm(this.delayUntilNextRead())
      return
    }
    if (!this.options.signal.detectsEveryWrite) this.arm(this.options.intervalMs)
  }
}

function now(): number {
  return performance.now()
}
