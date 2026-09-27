import { invokeCallerCallback } from '../core/caller-callbacks.js'
import { unrefTimer } from './http-json.js'
import type { SyncStatus } from './sync-controller-types.js'

const MIN_OUTBOX_COUNT_INTERVAL_MS = 100

/**
 * Calls `onStatusChange` for a sync controller.
 *
 * The controller keeps every status field except the pending push count in
 * memory, so the notifier captures each transition when it happens and keeps
 * every one, however short-lived. It calls the listener with the captured
 * statuses on a microtask, in the order that they occur, outside the
 * controller's own call stack.
 *
 * The pending push count is the one field that needs an outbox read through the
 * write gate, so the notifier refreshes it in the background at most once every
 * `MIN_OUTBOX_COUNT_INTERVAL_MS` and captures a further status when the count
 * changes. When the controller has no listener, the notifier reads nothing and
 * starts no timer.
 */
export class SyncStatusNotifier {
  private readonly queued: SyncStatus[] = []
  private flushing = false
  private counting = false
  private countTimer: ReturnType<typeof setTimeout> | null = null
  private lastCountAt = 0

  constructor(
    private readonly listener: ((status: SyncStatus) => void) | undefined,
    private readonly capture: () => SyncStatus,
    private readonly refreshOutboxCount: () => Promise<boolean>,
  ) {}

  /**
   * Captures the controller's status now and passes it to the listener on a microtask.
   */
  notify(): void {
    if (this.listener === undefined) return
    this.queued.push(this.capture())
    this.scheduleFlush()
    this.scheduleCount()
  }

  private scheduleFlush(): void {
    if (this.flushing) return
    this.flushing = true
    queueMicrotask(() => {
      this.flushing = false
      this.flush()
    })
  }

  private flush(): void {
    const listener = this.listener
    if (listener === undefined) return
    while (this.queued.length > 0) {
      const status = this.queued.shift()
      if (status === undefined) return
      invokeCallerCallback(() => listener(status))
    }
  }

  private scheduleCount(): void {
    if (this.counting || this.countTimer !== null) return
    const waitMs = Math.max(0, this.lastCountAt + MIN_OUTBOX_COUNT_INTERVAL_MS - Date.now())
    this.countTimer = setTimeout(() => {
      this.countTimer = null
      void this.runCount()
    }, waitMs)
    unrefTimer(this.countTimer)
  }

  private async runCount(): Promise<void> {
    this.counting = true
    let changed = false
    try {
      changed = await this.refreshOutboxCount()
    } catch {
      changed = false
    } finally {
      this.lastCountAt = Date.now()
      this.counting = false
    }
    if (changed) {
      this.queued.push(this.capture())
      this.scheduleFlush()
    }
  }
}
