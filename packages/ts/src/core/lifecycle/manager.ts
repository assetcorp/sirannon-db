import type { Database } from '../database.js'
import { MaxDatabasesError, SirannonError } from '../errors.js'
import type { DatabaseOptions, LifecycleConfig } from '../types.js'

/**
 * The registry supplies these functions so that the lifecycle manager can open, close, count, and look up its databases.
 *
 * @internal
 */
export interface LifecycleCallbacks {
  open: (id: string, path: string, options?: DatabaseOptions) => Promise<Database>
  close: (id: string) => Promise<void>
  count: () => number
  has: (id: string) => boolean
}

function settledWithoutValue(): void {}

/**
 * Opens a database through the `autoOpen` resolver when the registry asks for it, and closes each database that stays idle for `idleTimeout` or longer.
 *
 * When one more open database would exceed `maxOpen`, the manager first closes the least recently used database.
 * Under a `maxOpen` limit, the manager admits one automatic open at a time and
 * counts every open that is still under way against the limit until that open
 * settles. When requests for several new tenants arrive at once, each one
 * therefore waits for its turn, so the number of open databases stays within
 * the limit.
 *
 * @internal
 */
export class LifecycleManager {
  private readonly config: LifecycleConfig
  private readonly callbacks: LifecycleCallbacks
  private readonly lastAccess = new Map<string, number>()
  private idleTimer: ReturnType<typeof setInterval> | null = null
  private _disposed = false
  #idleCheckPromise: Promise<void> | null = null
  #placeClaims: Promise<unknown> = Promise.resolve()
  readonly #opensUnderWay = new Set<Promise<void>>()

  constructor(config: LifecycleConfig, callbacks: LifecycleCallbacks) {
    this.config = config
    this.callbacks = callbacks

    const timeout = config.idleTimeout
    if (timeout && timeout > 0) {
      const interval = Math.min(Math.max(Math.floor(timeout / 2), 100), 60_000)
      const timer = setInterval(async () => {
        await this.#runIdleCheck()
      }, interval) as ReturnType<typeof setInterval> & { unref?: () => void }
      timer.unref?.()
      this.idleTimer = timer
    }
  }

  async resolve(id: string): Promise<Database | undefined> {
    this.ensureNotDisposed()

    const resolver = this.config.autoOpen?.resolver
    if (!resolver) return undefined

    const resolved = resolver(id)
    if (!resolved) return undefined

    const open = () => this.callbacks.open(id, resolved.path, resolved.options)
    const maxOpen = this.config.maxOpen
    const { opened } = maxOpen && maxOpen > 0 ? await this.#claimPlace(maxOpen, open) : { opened: open() }

    const db = await opened
    this.markActive(id)
    return db
  }

  markActive(id: string): void {
    if (!this._disposed) {
      this.lastAccess.delete(id)
      this.lastAccess.set(id, performance.now())
    }
  }

  async checkIdle(): Promise<void> {
    const timeout = this.config.idleTimeout
    if (!timeout || timeout <= 0) return

    const now = performance.now()
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
      } catch {}
      toRemove.push(id)
    }

    for (const id of toRemove) {
      this.lastAccess.delete(id)
    }
  }

  async evict(): Promise<void> {
    await this.#closeLeastRecentlyUsed()
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

  #claimPlace(maxOpen: number, open: () => Promise<Database>): Promise<{ opened: Promise<Database> }> {
    const claim = this.#placeClaims.then(() => this.#makeRoomThenOpen(maxOpen, open))
    this.#placeClaims = claim.catch(settledWithoutValue)
    return claim
  }

  async #makeRoomThenOpen(maxOpen: number, open: () => Promise<Database>): Promise<{ opened: Promise<Database> }> {
    while (this.callbacks.count() >= maxOpen) {
      if (await this.#closeLeastRecentlyUsed()) continue
      if (this.#opensUnderWay.size === 0) throw new MaxDatabasesError(maxOpen)
      await Promise.race(this.#opensUnderWay)
    }

    const opened = open()
    const underWay: Promise<void> = opened.then(
      () => {
        this.#opensUnderWay.delete(underWay)
      },
      () => {
        this.#opensUnderWay.delete(underWay)
      },
    )
    this.#opensUnderWay.add(underWay)
    return { opened }
  }

  async #closeLeastRecentlyUsed(): Promise<boolean> {
    for (const id of [...this.lastAccess.keys()]) {
      this.lastAccess.delete(id)
      if (!this.callbacks.has(id)) continue
      try {
        await this.callbacks.close(id)
      } catch {}
      return true
    }
    return false
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
