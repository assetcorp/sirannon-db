const MAX_TIMER_DELAY_MS = 2_147_483_647

/**
 * Calls a function once a pool has gone a set time without a read or a write, so that the pool can close the
 * connections that nobody uses.
 *
 * Recording a use costs one clock read and one comparison, because the clock keeps one timer and arms it again only
 * when it fires early.
 *
 * @internal
 */
export class ConnectionIdleClock {
  private lastUsedAt = performance.now()
  private timer: ReturnType<typeof setTimeout> | null = null
  private stopped = false

  /**
   * @param timeoutMs - How long the pool has to go without a use, or `0` to never call `onIdle`.
   * @param onIdle - Called once the pool has gone `timeoutMs` without a use, and must return at once.
   */
  constructor(
    private readonly timeoutMs: number,
    private readonly onIdle: () => void,
  ) {}

  /** Returns how long the pool has gone without a use, in milliseconds. */
  get idleForMs(): number {
    return performance.now() - this.lastUsedAt
  }

  /** Returns `true` once the pool has gone the whole timeout without a use. */
  get expired(): boolean {
    return this.timeoutMs > 0 && this.idleForMs >= this.timeoutMs
  }

  /** Records a use now, and makes sure that the clock checks again once the timeout passes. */
  used(): void {
    this.lastUsedAt = performance.now()
    this.arm(this.timeoutMs)
  }

  /** Makes sure that the clock checks again once the timeout passes, for an idle close that could not run yet. */
  checkAgainLater(): void {
    this.arm(this.timeoutMs)
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

  private readonly fire = (): void => {
    this.timer = null
    const remainingMs = this.timeoutMs - this.idleForMs
    if (remainingMs > 0) {
      this.arm(remainingMs)
      return
    }
    this.onIdle()
  }
}
