/**
 * The timer that starts each turn of one backup cycle.
 *
 * The timer unreferences its handle where the runtime supports that, so that a
 * process can exit while the timer is still set.
 *
 * @internal
 */
export class BackupCycleTimer {
  private handle: ReturnType<typeof setInterval> | null = null

  /**
   * Starts the timer. At an interval of zero or less, the timer stays off and
   * the cycle takes a turn only when a caller requests one.
   *
   * @param intervalMs - The number of milliseconds between one turn and the next.
   * @param tick - Called on every interval.
   */
  arm(intervalMs: number, tick: () => void): void {
    this.disarm()
    if (intervalMs <= 0) return
    this.handle = setInterval(tick, intervalMs)
    this.handle.unref?.()
  }

  /** Stops the timer. */
  disarm(): void {
    if (!this.handle) return
    clearInterval(this.handle)
    this.handle = null
  }
}

/**
 * Runs one turn at a time, in the order that callers request them.
 *
 * In a turn, Sirannon acquires the writer, reads the log, and then checkpoints
 * it. If two turns overlapped, one of them could checkpoint frames that the
 * other had yet to read, so every entry point into a cycle goes through this
 * queue.
 *
 * @internal
 */
export class SerialTurns {
  private inFlight: Promise<unknown> = Promise.resolve()

  /**
   * Starts an operation once every turn requested before it settles.
   *
   * @param op - The turn to take.
   * @returns The result of that turn.
   */
  run<T>(op: () => Promise<T>): Promise<T> {
    const turn = this.inFlight.then(op, op)
    this.inFlight = turn.then(
      () => {},
      () => {},
    )
    return turn
  }
}
