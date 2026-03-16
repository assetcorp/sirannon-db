import type { SQLiteConnection } from '../driver/types.js'
import type { ChangeEvent, Subscription, SubscriptionBuilder } from '../types.js'
import type { ChangeTracker } from './change-tracker.js'

interface InternalSubscription {
  id: number
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
}

export class SubscriptionManager {
  private nextId = 1
  private readonly subscriptions = new Map<number, InternalSubscription>()
  private readonly byTable = new Map<string, Set<number>>()

  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
  ): Subscription {
    const id = this.nextId++
    this.subscriptions.set(id, { id, table, filter, callback })

    let tableSet = this.byTable.get(table)
    if (!tableSet) {
      tableSet = new Set()
      this.byTable.set(table, tableSet)
    }
    tableSet.add(id)

    return {
      unsubscribe: () => {
        this.subscriptions.delete(id)
        const set = this.byTable.get(table)
        if (set) {
          set.delete(id)
          if (set.size === 0) {
            this.byTable.delete(table)
          }
        }
      },
    }
  }

  dispatch(events: ChangeEvent[]): void {
    for (const event of events) {
      const ids = this.byTable.get(event.table)
      if (!ids) continue

      for (const id of ids) {
        const sub = this.subscriptions.get(id)
        if (!sub) continue
        if (sub.filter && !matchesFilter(event, sub.filter)) {
          continue
        }
        try {
          sub.callback(event)
        } catch {
          /* subscriber errors must not break other subscribers */
        }
      }
    }
  }

  get size(): number {
    return this.subscriptions.size
  }

  subscriberCount(table: string): number {
    return this.byTable.get(table)?.size ?? 0
  }
}

export class SubscriptionBuilderImpl implements SubscriptionBuilder {
  private conditions: Record<string, unknown> | undefined

  constructor(
    private readonly table: string,
    private readonly manager: SubscriptionManager,
  ) {}

  filter(conditions: Record<string, unknown>): SubscriptionBuilder {
    this.conditions = { ...this.conditions, ...conditions }
    return this
  }

  subscribe(callback: (event: ChangeEvent) => void): Subscription {
    return this.manager.subscribe(this.table, this.conditions, callback)
  }
}

export function startPolling(
  conn: SQLiteConnection,
  tracker: ChangeTracker,
  manager: SubscriptionManager,
  intervalMs: number,
  onError?: (err: Error) => void,
): () => void {
  let consecutiveErrors = 0
  let tickCount = 0
  let polling = false
  const MAX_CONSECUTIVE_ERRORS = 10
  const CLEANUP_INTERVAL_TICKS = 100

  const tick = async () => {
    if (manager.size === 0) return
    if (polling) return
    polling = true

    try {
      const events = await tracker.poll(conn)
      if (events.length > 0) {
        manager.dispatch(events)
      }
      consecutiveErrors = 0

      tickCount++
      if (tickCount >= CLEANUP_INTERVAL_TICKS) {
        tickCount = 0
        await tracker.cleanup(conn)
      }
    } catch (err) {
      consecutiveErrors++
      if (onError) {
        onError(err instanceof Error ? err : new Error(String(err)))
      }
      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        stop()
      }
    } finally {
      polling = false
    }
  }

  const interval = setInterval(tick, intervalMs)
  if (typeof interval === 'object' && 'unref' in interval) {
    interval.unref()
  }

  const stop = () => {
    clearInterval(interval)
  }

  return stop
}

function matchesFilter(event: ChangeEvent, filter: Record<string, unknown>): boolean {
  const target = event.type === 'delete' ? (event.oldRow ?? {}) : event.row
  for (const [key, value] of Object.entries(filter)) {
    if ((target as Record<string, unknown>)[key] !== value) {
      return false
    }
  }
  return true
}
