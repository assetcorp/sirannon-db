const MAX_TIMER_DELAY_MS = 2_147_483_647

/**
 * Calls a function once a pool has gone a set time without a read, so that the pool can close the readers that
 * nobody reads through, and reports whether the pool has also gone that time without a write, so that it can close
 * its writer.
 *
 * Recording a read or a write costs one clock read and one comparison, because the clock keeps one timer and arms it
 * again only when it fires early.
 *
 * @internal
 */
export class ConnectionIdleClock {
  private lastReadAt = performance.now()
  private lastReadOrWriteAt = this.lastReadAt
  private timer: ReturnType<typeof setTimeout> | null = null
  private stopped = false

  /**
   * @param timeoutMs - How long the pool has to go without a read, or `0` to never call `onReadIdle`.
   * @param onReadIdle - Called once the pool has gone `timeoutMs` without a read, and must return at once.
   */
  constructor(
    private readonly timeoutMs: number,
    private readonly onReadIdle: () => void,
  ) {}

  /** Returns `true` once the pool has gone the whole timeout without a read and without a write. */
  get readsAndWritesExpired(): boolean {
    return this.timeoutMs > 0 && this.remainingMsSince(this.lastReadOrWriteAt) <= 0
  }

  /** Records a read now, which also counts as a use of the writer, and makes sure that the clock checks again. */
  recordRead(): void {
    const now = performance.now()
    this.lastReadAt = now
    this.lastReadOrWriteAt = now
    this.arm(this.timeoutMs)
  }

  /** Records a write now, which leaves the readers' idle time running, and makes sure that the clock checks again. */
  recordWrite(): void {
    this.lastReadOrWriteAt = performance.now()
    this.arm(this.timeoutMs)
  }

  /** Makes sure that the clock checks again once the timeout passes, for an idle close that could not run yet. */
  checkAgainLater(): void {
    this.arm(this.timeoutMs)
  }

  /** Makes sure that the clock checks again once the pool has gone the timeout without a read and without a write. */
  checkOnceReadsAndWritesExpire(): void {
    this.arm(Math.max(this.remainingMsSince(this.lastReadOrWriteAt), 0))
  }

  /** Cancels the next check for good. */
  stop(): void {
    this.stopped = true
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
  }

  private arm(delayMs: number): void {
    if (this.timeoutMs === 0 || this.timer !== null || this.stopped) return
    const timer = setTimeout(this.fire, Math.min(delayMs, MAX_TIMER_DELAY_MS)) as ReturnType<typeof setTimeout> & {
      unref?: () => void
    }
    timer.unref?.()
    this.timer = timer
  }

  private remainingMsSince(at: number): number {
    return this.timeoutMs - (performance.now() - at)
  }

  private readonly fire = (): void => {
    this.timer = null
    const remainingMs = this.remainingMsSince(this.lastReadAt)
    if (remainingMs > 0) {
      this.arm(remainingMs)
      return
    }
    this.onReadIdle()
  }
}
