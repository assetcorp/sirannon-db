import { ChangeTracker } from '../core/cdc/change-tracker.js'
import { ensureCdcEpoch } from '../core/cdc/epoch.js'
import { SubscriptionManager } from '../core/cdc/subscription.js'
import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'

const DEFAULT_POLL_INTERVAL_MS = 50
const CLEANUP_INTERVAL_TICKS = 100

export interface CDCContext {
  cdcConn: SQLiteConnection
  tracker: ChangeTracker
  manager: SubscriptionManager
  stopPolling: () => void
  epoch: string
}

/**
 * Per-database CDC polling contexts shared by every WebSocket subscription on
 * the same database. Each context owns a dedicated read connection so CDC
 * polling never contends with the single writer, and the context is torn
 * down once its last subscriber leaves.
 */
export class CdcContextRegistry {
  private readonly sirannon: Sirannon
  private readonly retentionMs: number | undefined
  private readonly contexts = new Map<string, CDCContext>()
  private readonly pending = new Map<string, Promise<CDCContext>>()
  private closed = false

  constructor(sirannon: Sirannon, retentionMs?: number) {
    this.sirannon = sirannon
    this.retentionMs = retentionMs
  }

  async ensure(databaseId: string, database: Database): Promise<CDCContext> {
    const existing = this.contexts.get(databaseId)
    if (existing) return existing

    const pending = this.pending.get(databaseId)
    if (pending) return pending

    const promise = this.createContext(database)
    this.pending.set(databaseId, promise)
    try {
      const ctx = await promise
      if (this.closed) {
        ctx.stopPolling()
        await ctx.cdcConn.close().catch(() => {})
        throw new SirannonError('WebSocket handler is shut down', 'HANDLER_CLOSED')
      }
      this.contexts.set(databaseId, ctx)
      return ctx
    } finally {
      this.pending.delete(databaseId)
    }
  }

  get(databaseId: string): CDCContext | undefined {
    return this.contexts.get(databaseId)
  }

  maybeCleanup(databaseId: string): void {
    const ctx = this.contexts.get(databaseId)
    if (!ctx || ctx.manager.size > 0) return

    ctx.stopPolling()
    ctx.cdcConn.close().catch(() => {
      /* best effort */
    })
    this.contexts.delete(databaseId)
  }

  async closeAll(): Promise<void> {
    if (this.closed) return
    this.closed = true

    for (const ctx of this.contexts.values()) {
      ctx.stopPolling()
      try {
        await ctx.cdcConn.close()
      } catch {
        /* best effort */
      }
    }
    this.contexts.clear()
  }

  private async createContext(database: Database): Promise<CDCContext> {
    const cdcConn = await this.sirannon.driver.open(database.path, { walMode: true })

    const tracker = new ChangeTracker(this.retentionMs === undefined ? undefined : { retention: this.retentionMs })
    const manager = new SubscriptionManager()
    let epoch = ''
    try {
      await tracker.advanceToLatest(cdcConn)
      await database.runCdcMaintenance(async writer => {
        epoch = await ensureCdcEpoch(writer)
      })
    } catch (err) {
      await cdcConn.close().catch(() => {})
      throw err
    }
    if (epoch === '') {
      await cdcConn.close().catch(() => {})
      throw new SirannonError(`Database '${database.id}' is closed`, 'DATABASE_CLOSED')
    }

    let polling = false
    let consecutiveErrors = 0
    let tickCount = 0
    const MAX_CONSECUTIVE_ERRORS = 10

    const stopPolling = () => {
      clearInterval(interval)
    }

    const tick = async () => {
      if (manager.size === 0) return
      if (polling) return
      polling = true
      try {
        const events = await tracker.poll(cdcConn)
        if (events.length > 0) {
          manager.dispatch(events)
        }
        consecutiveErrors = 0

        tickCount++
        if (tickCount >= CLEANUP_INTERVAL_TICKS) {
          tickCount = 0
          await database.runCdcMaintenance(writer => tracker.cleanup(writer)).catch(() => {})
        }
      } catch {
        consecutiveErrors++
        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          stopPolling()
        }
      } finally {
        polling = false
      }
    }
    const interval = setInterval(tick, DEFAULT_POLL_INTERVAL_MS) as ReturnType<typeof setInterval> & {
      unref?: () => void
    }
    interval.unref?.()

    return { cdcConn, tracker, manager, stopPolling, epoch }
  }
}
