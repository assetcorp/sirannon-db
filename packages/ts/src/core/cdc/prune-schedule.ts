const POLLS_PER_PRUNE = 100
const MAX_TIMER_DELAY_MS = 2_147_483_647

/**
 * Decides when a change feed prunes the change log, so that a database that nobody writes to prunes only when a
 * change or a device cursor passes its retention window.
 *
 * A prune runs at most once per 100 poll intervals, which is the cadence of a timer-driven poller that prunes every
 * 100th read. The schedule asks for a prune within that cadence after a read that found changes, after a prune
 * boundary moves, after a device acknowledges, when a feed starts, and on every read while the poller reads on a
 * fixed interval. Each prune reports when the oldest remaining change or cursor passes its window, and the schedule
 * sets one timer for that moment.
 *
 * @internal
 */
export class PruneSchedule {
  private dueAt: number | null = null
  private lastPrunedAt = performance.now()
  private timer: ReturnType<typeof setTimeout> | null = null
  private readonly cadenceMs: number

  /**
   * @param intervalMs - The poll interval of the feed.
   * @param wake - Wakes the feed's poller, which prunes on its next read once a prune is due.
   */
  constructor(
    intervalMs: number,
    private readonly wake: () => void,
  ) {
    this.cadenceMs = POLLS_PER_PRUNE * Math.max(1, intervalMs)
  }

  /** Asks for a prune within one cadence of the last prune. */
  requestSoon(): void {
    this.request(this.lastPrunedAt + this.cadenceMs)
  }

  /**
   * Returns `true` when a prune is due, and records the prune as started.
   *
   * @returns Whether the caller prunes now.
   */
  takeDue(): boolean {
    const at = performance.now()
    if (this.dueAt === null || at < this.dueAt) return false
    this.lastPrunedAt = at
    this.dueAt = null
    this.clearTimer()
    return true
  }

  /**
   * Records what a prune reported, and sets the timer for the next prune.
   *
   * @param nextAtEpochMs - The time in milliseconds since the Unix epoch at which the next prune can remove more, or
   * `null` when nothing will pass its window without another write.
   */
  pruned(nextAtEpochMs: number | null): void {
    if (nextAtEpochMs === null) return
    this.request(performance.now() + Math.max(0, nextAtEpochMs - Date.now()))
  }

  /** Arms the timer again for a prune that is due, after {@link PruneSchedule.pause}. */
  resume(): void {
    if (this.dueAt !== null) this.arm()
  }

  /** Cancels the timer, and keeps the due prune for {@link PruneSchedule.resume}. */
  pause(): void {
    this.clearTimer()
  }

  private request(at: number): void {
    if (this.dueAt !== null && this.dueAt <= at) return
    this.dueAt = at
    this.arm()
  }

  private arm(): void {
    this.clearTimer()
    const dueAt = this.dueAt
    if (dueAt === null) return
    const delay = Math.min(MAX_TIMER_DELAY_MS, Math.max(0, dueAt - performance.now()))
    const timer = setTimeout(this.fire, delay) as ReturnType<typeof setTimeout> & { unref?: () => void }
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
    const dueAt = this.dueAt
    if (dueAt === null) return
    if (performance.now() < dueAt) {
      this.arm()
      return
    }
    this.wake()
  }
}
