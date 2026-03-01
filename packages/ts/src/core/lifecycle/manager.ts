import type { Database } from '../database.js'
import { MaxDatabasesError, SirannonError } from '../errors.js'
import type { DatabaseOptions, LifecycleConfig } from '../types.js'

/**
 * Callbacks the LifecycleManager uses to interact with the database registry
 * (Sirannon). Injected at construction to avoid circular dependencies.
 */
export interface LifecycleCallbacks {
  open: (id: string, path: string, options?: DatabaseOptions) => Database
  close: (id: string) => void
  count: () => number
  has: (id: string) => boolean
}

/**
 * Manages automatic database lifecycle: on-demand opening via a resolver,
 * idle-timeout based closing, and LRU eviction when the maximum number of
 * open databases is reached.
 */
export class LifecycleManager {
  private readonly config: LifecycleConfig
  private readonly callbacks: LifecycleCallbacks
  private readonly lastAccess = new Map<string, number>()
  private idleTimer: ReturnType<typeof setInterval> | null = null
  private _disposed = false

  constructor(config: LifecycleConfig, callbacks: LifecycleCallbacks) {
    this.config = config
    this.callbacks = callbacks

    const timeout = config.idleTimeout
    if (timeout && timeout > 0) {
      // Check at half the timeout interval, clamped between 100ms and 60s.
      const interval = Math.min(Math.max(Math.floor(timeout / 2), 100), 60_000)
      this.idleTimer = setInterval(() => this.checkIdle(), interval)
      // Unref so the timer does not keep the Node.js process alive.
      if (typeof this.idleTimer === 'object' && 'unref' in this.idleTimer) {
        this.idleTimer.unref()
      }
    }
  }

  /**
   * Attempt to auto-open a database by ID using the configured resolver.
   * Returns the opened Database, or `undefined` when no resolver is
   * configured or the resolver does not recognise the ID.
   *
   * Throws {@link MaxDatabasesError} when the registry is at capacity and
   * eviction cannot free a slot.
   */
  resolve(id: string): Database | undefined {
    this.ensureNotDisposed()

    const resolver = this.config.autoOpen?.resolver
    if (!resolver) return undefined

    const resolved = resolver(id)
    if (!resolved) return undefined

    const maxOpen = this.config.maxOpen
    if (maxOpen && maxOpen > 0 && this.callbacks.count() >= maxOpen) {
      this.evict()
      if (this.callbacks.count() >= maxOpen) {
        throw new MaxDatabasesError(maxOpen)
      }
    }

    const db = this.callbacks.open(id, resolved.path, resolved.options)
    this.markActive(id)
    return db
  }

  /** Record an access for the given database ID. */
  markActive(id: string): void {
    if (!this._disposed) {
      this.lastAccess.set(id, Date.now())
    }
  }

  /**
   * Close every database whose last access was longer ago than the
   * configured idle timeout. Also cleans up tracking entries for
   * databases that were closed externally.
   */
  checkIdle(): void {
    const timeout = this.config.idleTimeout
    if (!timeout || timeout <= 0) return

    const now = Date.now()
    const toRemove: string[] = []
    const toClose: string[] = []

    for (const [id, lastTime] of this.lastAccess) {
      if (!this.callbacks.has(id)) {
        // Database was closed externally; clean up stale entry.
        toRemove.push(id)
        continue
      }
      if (now - lastTime >= timeout) {
        toClose.push(id)
      }
    }

    for (const id of toClose) {
      try {
        this.callbacks.close(id)
      } catch {
        // Idle cleanup errors are non-fatal.
      }
      toRemove.push(id)
    }

    for (const id of toRemove) {
      this.lastAccess.delete(id)
    }
  }

  /**
   * Close the least-recently-used tracked database. Called internally
   * by {@link resolve} when `maxOpen` capacity is reached.
   */
  evict(): void {
    let oldestId: string | null = null
    let oldestTime = Infinity
    const stale: string[] = []

    for (const [id, time] of this.lastAccess) {
      if (!this.callbacks.has(id)) {
        stale.push(id)
        continue
      }
      if (time < oldestTime) {
        oldestTime = time
        oldestId = id
      }
    }

    for (const id of stale) {
      this.lastAccess.delete(id)
    }

    if (oldestId) {
      try {
        this.callbacks.close(oldestId)
      } catch {
        // Eviction errors are non-fatal.
      }
      this.lastAccess.delete(oldestId)
    }
  }

  /** Remove a database from idle tracking (e.g. after an explicit close). */
  untrack(id: string): void {
    this.lastAccess.delete(id)
  }

  /** Whether this manager has been disposed. */
  get disposed(): boolean {
    return this._disposed
  }

  /** The number of databases currently tracked for idle management. */
  get trackedCount(): number {
    return this.lastAccess.size
  }

  /** Shut down the manager: stop the idle timer and clear all state. */
  dispose(): void {
    if (this._disposed) return
    this._disposed = true

    if (this.idleTimer !== null) {
      clearInterval(this.idleTimer)
      this.idleTimer = null
    }
    this.lastAccess.clear()
  }

  private ensureNotDisposed(): void {
    if (this._disposed) {
      throw new SirannonError('LifecycleManager has been disposed', 'LIFECYCLE_DISPOSED')
    }
  }
}
