/**
 * The interval on which one backup cycle takes its turns.
 *
 * The timer unreferences its handle where the runtime allows that, so a
 * repeating cycle never keeps a process alive on its own.
 *
 * @internal
 */
export class BackupCycleTimer {
  private handle: ReturnType<typeof setInterval> | null = null

  /**
   * Starts repeating. At an interval of zero or less the cycle takes a turn
   * only when somebody asks it to.
   *
   * @param intervalMs - Milliseconds between one turn and the next.
   * @param tick - Called on every interval.
   */
  arm(intervalMs: number, tick: () => void): void {
    this.disarm()
    if (intervalMs <= 0) return
    this.handle = setInterval(tick, intervalMs)
    this.handle.unref?.()
  }

  /** Stops repeating. */
  disarm(): void {
    if (!this.handle) return
    clearInterval(this.handle)
    this.handle = null
  }
}

/**
 * Takes one turn at a time, in the order the callers asked for them.
 *
 * A turn acquires the writer, reads the log, and then checkpoints it. Two turns
 * overlapping would let one of them checkpoint frames the other had yet to
 * read, so every entry point into a cycle passes through here.
 *
 * @internal
 */
export class SerialTurns {
  private inFlight: Promise<unknown> = Promise.resolve()

  /**
   * Starts an operation once every turn asked for before it has settled.
   *
   * @param op - The turn to take.
   * @returns Whatever that turn produced.
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
