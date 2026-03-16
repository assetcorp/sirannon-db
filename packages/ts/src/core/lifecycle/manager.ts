import type { Database } from '../database.js'
import { MaxDatabasesError, SirannonError } from '../errors.js'
import type { DatabaseOptions, LifecycleConfig } from '../types.js'

export interface LifecycleCallbacks {
  open: (id: string, path: string, options?: DatabaseOptions) => Promise<Database>
  close: (id: string) => Promise<void>
  count: () => number
  has: (id: string) => boolean
}

export class LifecycleManager {
  private readonly config: LifecycleConfig
  private readonly callbacks: LifecycleCallbacks
  private readonly lastAccess = new Map<string, number>()
  private idleTimer: ReturnType<typeof setInterval> | null = null
  private _disposed = false
  #idleCheckPromise: Promise<void> | null = null

  constructor(config: LifecycleConfig, callbacks: LifecycleCallbacks) {
    this.config = config
    this.callbacks = callbacks

    const timeout = config.idleTimeout
    if (timeout && timeout > 0) {
      const interval = Math.min(Math.max(Math.floor(timeout / 2), 100), 60_000)
      this.idleTimer = setInterval(async () => {
        await this.#runIdleCheck()
      }, interval)
      if (typeof this.idleTimer === 'object' && 'unref' in this.idleTimer) {
        this.idleTimer.unref()
      }
    }
  }

  async resolve(id: string): Promise<Database | undefined> {
    this.ensureNotDisposed()

    const resolver = this.config.autoOpen?.resolver
    if (!resolver) return undefined

    const resolved = resolver(id)
    if (!resolved) return undefined

    const maxOpen = this.config.maxOpen
    if (maxOpen && maxOpen > 0 && this.callbacks.count() >= maxOpen) {
      await this.evict()
      if (this.callbacks.count() >= maxOpen) {
        throw new MaxDatabasesError(maxOpen)
      }
    }

    const db = await this.callbacks.open(id, resolved.path, resolved.options)
    this.markActive(id)
    return db
  }

  markActive(id: string): void {
    if (!this._disposed) {
      this.lastAccess.set(id, Date.now())
    }
  }

  async checkIdle(): Promise<void> {
    const timeout = this.config.idleTimeout
    if (!timeout || timeout <= 0) return

    const now = Date.now()
    const toRemove: string[] = []
    const toClose: string[] = []

    for (const [id, lastTime] of this.lastAccess) {
      if (!this.callbacks.has(id)) {
        toRemove.push(id)
        continue
      }
      if (now - lastTime >= timeout) {
        toClose.push(id)
      }
    }

    for (const id of toClose) {
      try {
        await this.callbacks.close(id)
      } catch {
        // Idle cleanup errors are non-fatal.
      }
      toRemove.push(id)
    }

    for (const id of toRemove) {
      this.lastAccess.delete(id)
    }
  }

  async evict(): Promise<void> {
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
        await this.callbacks.close(oldestId)
      } catch {
        // Eviction errors are non-fatal.
      }
      this.lastAccess.delete(oldestId)
    }
  }

  untrack(id: string): void {
    this.lastAccess.delete(id)
  }

  get disposed(): boolean {
    return this._disposed
  }

  get trackedCount(): number {
    return this.lastAccess.size
  }

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

  async #runIdleCheck(): Promise<void> {
    if (this.#idleCheckPromise) {
      await this.#idleCheckPromise
      return
    }

    this.#idleCheckPromise = this.checkIdle()

    try {
      await this.#idleCheckPromise
    } catch {
    } finally {
      this.#idleCheckPromise = null
    }
  }
}
