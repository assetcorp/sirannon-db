import { invokeCallerCallback, reportCallerFailure, toError } from '../caller-callbacks.js'
import type { SQLiteConnection } from '../driver/types.js'
import type { ChangeEvent, Subscription, SubscriptionBuilder, SubscriptionOptions } from '../types.js'
import type { ChangeTracker } from './change-tracker.js'

interface InternalSubscription {
  id: number
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
  onError: ((error: Error) => void) | undefined
}

export class SubscriptionManager {
  private nextId = 1
  private readonly subscriptions = new Map<number, InternalSubscription>()
  private readonly byTable = new Map<string, Set<number>>()
  private readonly batchEndListeners = new Set<(atTxBoundary: boolean) => void>()

  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscriptionOptions,
  ): Subscription {
    const id = this.nextId++
    this.subscriptions.set(id, { id, table, filter, callback, onError: options?.onError })

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
        const delivered = sub.filter === undefined ? event : filteredChange(event, sub.filter)
        if (delivered === null) continue
        invokeCallerCallback(() => sub.callback(delivered), sub.onError)
      }
    }
  }

  reportError(error: Error): void {
    for (const sub of this.subscriptions.values()) {
      reportCallerFailure(sub.onError, error)
    }
  }

  addBatchEndListener(listener: (atTxBoundary: boolean) => void): () => void {
    this.batchEndListeners.add(listener)
    return () => {
      this.batchEndListeners.delete(listener)
    }
  }

  endBatch(atTxBoundary: boolean): void {
    for (const listener of this.batchEndListeners) {
      invokeCallerCallback(() => listener(atTxBoundary))
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

  subscribe<T = Record<string, unknown>>(
    callback: (event: ChangeEvent<T>) => void,
    options?: SubscriptionOptions,
  ): Subscription {
    return this.manager.subscribe(this.table, this.conditions, callback as (event: ChangeEvent) => void, options)
  }
}

export function startPolling(
  conn: SQLiteConnection,
  tracker: ChangeTracker,
  manager: SubscriptionManager,
  intervalMs: number,
  onError?: (err: Error) => void,
  runExclusive?: <T>(operation: () => Promise<T>) => Promise<T>,
): () => void {
  let consecutiveErrors = 0
  let tickCount = 0
  let polling = false
  const MAX_CONSECUTIVE_ERRORS = 10
  const CLEANUP_INTERVAL_TICKS = 100

  const exclusive = runExclusive ?? (<T>(operation: () => Promise<T>) => operation())

  const tick = async () => {
    if (manager.size === 0) return
    if (polling) return
    polling = true

    try {
      const events = await exclusive(() => tracker.poll(conn))
      if (events.length > 0) {
        manager.dispatch(events)
      }
      manager.endBatch(tracker.pollEndedAtTxBoundary)
      consecutiveErrors = 0

      tickCount++
      if (tickCount >= CLEANUP_INTERVAL_TICKS) {
        tickCount = 0
        await exclusive(() => tracker.cleanup(conn))
      }
    } catch (err) {
      consecutiveErrors++
      const failure = toError(err)
      if (onError) {
        onError(failure)
      }
      if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
        stop()
        manager.reportError(failure)
      }
    } finally {
      polling = false
    }
  }

  const interval = setInterval(tick, intervalMs) as ReturnType<typeof setInterval> & { unref?: () => void }
  interval.unref?.()

  const stop = () => {
    clearInterval(interval)
  }

  return stop
}

export function filteredChange(event: ChangeEvent, filter: Record<string, unknown>): ChangeEvent | null {
  const matchedBefore = event.type !== 'insert' && event.oldRow !== undefined && rowMatchesFilter(event.oldRow, filter)
  const matchedAfter = event.type !== 'delete' && rowMatchesFilter(event.row, filter)

  if (!matchedBefore && !matchedAfter) return null
  if (matchedBefore && matchedAfter) return event

  if (matchedAfter) {
    if (event.type === 'insert') return event
    const entering: ChangeEvent = { ...event, type: 'insert' }
    entering.oldRow = undefined
    return entering
  }

  if (event.type === 'delete') return event
  return { ...event, type: 'delete', row: {} }
}

function rowMatchesFilter(row: Record<string, unknown>, filter: Record<string, unknown>): boolean {
  for (const [key, value] of Object.entries(filter)) {
    if (!filterValueMatches(row[key], value)) {
      return false
    }
  }
  return true
}

function filterValueMatches(rowValue: unknown, filterValue: unknown): boolean {
  if (rowValue === filterValue) return true
  if (typeof rowValue === 'bigint' && typeof filterValue === 'number') {
    return Number.isInteger(filterValue) && BigInt(filterValue) === rowValue
  }
  if (typeof rowValue === 'number' && typeof filterValue === 'bigint') {
    return Number.isInteger(rowValue) && BigInt(rowValue) === filterValue
  }
  if (rowValue instanceof Uint8Array && filterValue instanceof Uint8Array) {
    if (rowValue.byteLength !== filterValue.byteLength) return false
    for (let i = 0; i < rowValue.byteLength; i++) {
      if (rowValue[i] !== filterValue[i]) return false
    }
    return true
  }
  return false
}
