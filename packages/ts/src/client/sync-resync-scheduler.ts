import { invokeCallerCallback } from '../core/caller-callbacks.js'
import type { DeviceSyncPort } from '../core/database-sync.js'
import { unrefTimer } from './http-json.js'
import type { SnapshotOutcome } from './sync-controller-types.js'

export interface ResyncSchedulerConfig {
  autoResync?: boolean
  retryDelayMs: number
  maxRetryDelayMs: number
  onResyncRequired?: () => void
  onSnapshotComplete?: (outcome: SnapshotOutcome) => void
}

export interface ResyncSchedulerHooks {
  isRunning(): boolean
  isSnapshotting(): boolean
  port(): DeviceSyncPort | null
  download(): Promise<void>
  recordError(err: unknown): void
}

/**
 * Owns when a device replaces its database from a server snapshot and what it
 * tells the application about it.
 *
 * A failed load leaves the local database refusing reads and writes, so the two
 * callbacks bracket the window in which the application cannot use it:
 * `onResyncRequired` opens it and `onSnapshotComplete` closes it only once a
 * load has succeeded. A failure reports whether another try is already
 * scheduled, because an application the scheduler has given up on has to
 * request the copy itself.
 */
export class ResyncScheduler {
  required = false
  private timer: ReturnType<typeof setTimeout> | null = null
  private consecutiveFailures = 0

  constructor(
    private readonly config: ResyncSchedulerConfig,
    private readonly hooks: ResyncSchedulerHooks,
  ) {}

  get retryScheduled(): boolean {
    return this.timer !== null
  }

  markRequired(): void {
    this.required = true
    void this.hooks
      .port()
      ?.setResyncRequired(true)
      .catch(err => this.hooks.recordError(err))
    invokeCallerCallback(() => this.config.onResyncRequired?.())
  }

  schedule(): void {
    if (this.config.autoResync === false) return
    if (this.timer !== null || this.hooks.isSnapshotting()) return
    const delay =
      this.consecutiveFailures === 0
        ? 0
        : Math.min(this.config.retryDelayMs * 2 ** (this.consecutiveFailures - 1), this.config.maxRetryDelayMs)
    this.timer = setTimeout(() => {
      this.timer = null
      void this.attempt()
    }, delay)
    unrefTimer(this.timer)
  }

  cancel(): void {
    if (this.timer === null) return
    clearTimeout(this.timer)
    this.timer = null
  }

  recordSuccess(): void {
    this.required = false
    this.consecutiveFailures = 0
  }

  recordFailure(): void {
    this.required = true
    this.consecutiveFailures += 1
  }

  complete(outcome: SnapshotOutcome): void {
    invokeCallerCallback(() => this.config.onSnapshotComplete?.(outcome))
  }

  private async attempt(): Promise<void> {
    if (!this.hooks.isRunning() || !this.required) return
    try {
      await this.hooks.download()
    } catch {
      this.schedule()
    }
  }
}
