/**
 * The socket calls that a {@link DeviceSocketWatch} makes.
 *
 * @internal
 */
export interface WatchedDeviceSocket {
  /** Returns the bytes that the socket holds and has not sent yet. */
  buffered(): number
  /** Returns `true` while the socket holds at least the byte count below which a paused stream resumes. */
  congested(): boolean
  /** Sends a ping so that uWebSockets writes the rest of a partial write. */
  flush(): void
}

/**
 * Checks one device stream's socket on a timer, only while the socket holds unsent bytes or the stream waits for the
 * socket, so that a stream on an idle socket runs no timer.
 *
 * uWebSockets keeps the rest of a partial write queued until the next write on that socket and fires no drain event
 * in the meantime, so the last changes of a stream with nothing more to send could stay in the buffer. On each check,
 * when the buffered count is above zero, unchanged since the previous check, and below the congestion threshold, the
 * watch sends a ping so that uWebSockets writes the rest. It skips a congested socket, because that socket drains
 * under its flow control and one more frame could push it past the backpressure limit. Each check also calls
 * `onCheck`, which resumes a stream whose socket has drained without a drain event.
 *
 * @internal
 */
export class DeviceSocketWatch {
  private timer: ReturnType<typeof setTimeout> | null = null
  private lastBuffered = 0
  private stopped = false

  /**
   * @param socket - The socket calls.
   * @param intervalMs - The time between two checks.
   * @param onCheck - Called on each check, which resumes the stream when it can send again, and returns `true`
   * while the stream still waits for the socket.
   */
  constructor(
    private readonly socket: WatchedDeviceSocket,
    private readonly intervalMs: number,
    private readonly onCheck: () => boolean,
  ) {}

  /** Schedules a check, unless one is already scheduled. */
  arm(): void {
    if (this.stopped || this.timer !== null) return
    const timer = setTimeout(this.check, this.intervalMs) as ReturnType<typeof setTimeout> & { unref?: () => void }
    timer.unref?.()
    this.timer = timer
  }

  /** Cancels the next check for good. */
  stop(): void {
    this.stopped = true
    if (this.timer !== null) clearTimeout(this.timer)
    this.timer = null
  }

  private readonly check = (): void => {
    this.timer = null
    if (this.stopped) return
    const buffered = this.socket.buffered()
    if (buffered > 0 && buffered === this.lastBuffered && !this.socket.congested()) {
      this.socket.flush()
    }
    this.lastBuffered = buffered
    const waiting = this.onCheck()
    if (buffered > 0 || waiting) this.arm()
  }
}
