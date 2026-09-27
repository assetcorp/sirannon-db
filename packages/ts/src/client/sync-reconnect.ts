import { unrefTimer } from './http-json.js'
import type { NetworkSignal } from './network-signal.js'

/**
 * Returns the wait before retry number `attempt`: the base doubled once per earlier attempt and capped at `maxMs`,
 * with the lower half of that value fixed and the upper half random, so that devices recovering from one outage spread
 * their retries and no wait exceeds the cap.
 *
 * @param baseMs - The wait before the first retry.
 * @param attempt - The number of retries made so far.
 * @param maxMs - The longest wait.
 * @param random - A source of numbers in `[0, 1)`.
 * @returns The wait in milliseconds.
 *
 * @internal
 */
export function jitteredBackoff(baseMs: number, attempt: number, maxMs: number, random = Math.random): number {
  const ceiling = Math.min(baseMs * 2 ** attempt, maxMs)
  return Math.round(ceiling / 2 + random() * (ceiling / 2))
}

/**
 * The waits that a {@link PullReconnector} applies, and the network signal that it reads.
 *
 * @internal
 */
export interface PullReconnectConfig {
  baseDelayMs: number
  maxDelayMs: number
  network: NetworkSignal
}

/**
 * The functions that a {@link PullReconnector} calls to reopen the pull connection and to report the network's return.
 *
 * @internal
 */
export interface PullReconnectHooks {
  reopen(): void
  onOnline(): void
}

/**
 * Reopens the device's pull connection after a failure, waiting longer after each consecutive failure, holding every
 * retry while the device reports no network, and retrying at once when the network returns.
 *
 * @internal
 */
export class PullReconnector {
  private timer: ReturnType<typeof setTimeout> | null = null
  private heldForNetwork = false
  private failures = 0
  private stopWatching: (() => void) | null = null

  constructor(
    private readonly config: PullReconnectConfig,
    private readonly hooks: PullReconnectHooks,
  ) {}

  /** Begins listening for the network's return. */
  start(): void {
    if (this.stopWatching !== null) return
    this.stopWatching = this.config.network.watchOnline(() => this.handleOnline())
  }

  /** Clears any pending retry and stops listening for the network. */
  stop(): void {
    this.clearTimer()
    this.heldForNetwork = false
    this.stopWatching?.()
    this.stopWatching = null
  }

  /** Records that the pull connection works, so that the next failure waits the shortest time again. */
  reset(): void {
    this.failures = 0
  }

  /** Schedules one reopen, unless one is already pending or held for the network. */
  schedule(): void {
    if (this.timer !== null || this.heldForNetwork) return
    const delay = jitteredBackoff(this.config.baseDelayMs, this.failures, this.config.maxDelayMs)
    this.failures += 1
    this.timer = setTimeout(() => {
      this.timer = null
      if (this.config.network.reportsOffline()) {
        this.heldForNetwork = true
        return
      }
      this.hooks.reopen()
    }, delay)
    unrefTimer(this.timer)
  }

  private handleOnline(): void {
    this.hooks.onOnline()
    if (this.timer === null && !this.heldForNetwork) return
    this.clearTimer()
    this.heldForNetwork = false
    this.failures = 0
    this.hooks.reopen()
  }

  private clearTimer(): void {
    if (this.timer === null) return
    clearTimeout(this.timer)
    this.timer = null
  }
}
