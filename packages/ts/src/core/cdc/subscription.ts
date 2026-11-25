import { invokeCallerCallback, reportCallerFailure } from '../caller-callbacks.js'
import type { ChangeEvent, Subscription, SubscriptionBuilder, SubscriptionOptions } from '../types.js'

interface InternalSubscription {
  id: number
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
  onError: ((error: Error) => void) | undefined
}

export type ChangeDispatchObserver = (event: ChangeEvent, subscriberCount: number) => void

const NO_SUBSCRIBERS: ReadonlySet<number> = new Set()

export class SubscriptionManager {
  constructor(private readonly onDispatched?: ChangeDispatchObserver) {}

  private nextId = 1
  private readonly subscriptions = new Map<number, InternalSubscription>()
  private readonly byTable = new Map<string, Set<number>>()
  private readonly foldedNames = new Map<string, string>()
  private readonly batchEndListeners = new Set<(atTxBoundary: boolean) => void>()
  private readonly activityListeners = new Set<(active: boolean) => void>()

  /**
   * Registers a function that the manager calls with `true` when it gains its first subscription, and with `false`
   * when it loses its last one, so that a change feed reads the log only while somebody listens.
   *
   * @param listener - The function to call.
   * @returns A function that removes the registration.
   */
  onActivityChange(listener: (active: boolean) => void): () => void {
    this.activityListeners.add(listener)
    return () => {
      this.activityListeners.delete(listener)
    }
  }

  /** The identifier that the next subscription receives, which {@link SubscriptionManager.dispatch} compares against. */
  get nextSubscriptionId(): number {
    return this.nextId
  }

  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscriptionOptions,
  ): Subscription {
    const id = this.nextId++
    this.subscriptions.set(id, { id, table, filter, callback, onError: options?.onError })

    const key = table.toLowerCase()
    let tableSet = this.byTable.get(key)
    if (!tableSet) {
      tableSet = new Set()
      this.byTable.set(key, tableSet)
    }
    tableSet.add(id)
    if (this.subscriptions.size === 1) this.notifyActivity(true)

    return {
      unsubscribe: () => {
        if (!this.subscriptions.delete(id)) return
        const set = this.byTable.get(key)
        if (set) {
          set.delete(id)
          if (set.size === 0) {
            this.byTable.delete(key)
          }
        }
        if (this.subscriptions.size === 0) this.notifyActivity(false)
      },
    }
  }

  /**
   * Delivers each event to every matching subscription.
   *
   * @param events - The events, in log order.
   * @param subscribedBefore - The value of {@link SubscriptionManager.nextSubscriptionId} when the batch that holds
   * these events was read, so that a subscription made while a feed delivers the batch in slices skips the rest of
   * it, because that subscription starts from the tracker's cursor, which already covers the whole batch.
   */
  dispatch(events: ChangeEvent[], subscribedBefore?: number): void {
    for (const event of events) {
      const ids = this.byTable.get(this.foldLoggedName(event.table)) ?? NO_SUBSCRIBERS
      const firstIdAfterThisEvent =
        subscribedBefore === undefined ? this.nextId : Math.min(subscribedBefore, this.nextId)
      let deliveredTo = 0
      for (const id of ids) {
        if (id >= firstIdAfterThisEvent) continue
        const sub = this.subscriptions.get(id)
        if (!sub) continue
        const delivered = sub.filter === undefined ? event : filteredChange(event, sub.filter)
        if (delivered === null) continue
        deliveredTo++
        invokeCallerCallback(() => sub.callback(delivered), sub.onError)
      }
      this.onDispatched?.(event, deliveredTo)
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
    return this.byTable.get(table.toLowerCase())?.size ?? 0
  }

  private notifyActivity(active: boolean): void {
    for (const listener of [...this.activityListeners]) {
      invokeCallerCallback(() => listener(active))
    }
  }

  private foldLoggedName(table: string): string {
    let folded = this.foldedNames.get(table)
    if (folded === undefined) {
      folded = table.toLowerCase()
      this.foldedNames.set(table, folded)
    }
    return folded
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

const enteringVariants = new WeakMap<ChangeEvent, ChangeEvent>()
const leavingVariants = new WeakMap<ChangeEvent, ChangeEvent>()

/**
 * Returns the event as a subscription with a filter sees it: the event itself when the row matches the filter before
 * and after the change, an insert when an update moves the row into the filter, a delete when an update moves it out,
 * and `null` when the row matches on neither side.
 *
 * Every subscription receives the same insert or delete object for one event, so that a server encodes it for the
 * wire once however many subscriptions receive it, and no subscription may change it.
 *
 * @param event - The event from the change log.
 * @param filter - The column values that the subscription matches.
 * @returns The event that the subscription receives, or `null`.
 * @internal
 */
export function filteredChange(event: ChangeEvent, filter: Record<string, unknown>): ChangeEvent | null {
  const matchedBefore = event.type !== 'insert' && event.oldRow !== undefined && rowMatchesFilter(event.oldRow, filter)
  const matchedAfter = event.type !== 'delete' && rowMatchesFilter(event.row, filter)

  if (!matchedBefore && !matchedAfter) return null
  if (matchedBefore && matchedAfter) return event

  if (matchedAfter) {
    if (event.type === 'insert') return event
    let entering = enteringVariants.get(event)
    if (entering === undefined) {
      entering = { ...event, type: 'insert' }
      entering.oldRow = undefined
      enteringVariants.set(event, entering)
    }
    return entering
  }

  if (event.type === 'delete') return event
  let leaving = leavingVariants.get(event)
  if (leaving === undefined) {
    leaving = { ...event, type: 'delete', row: {} }
    leavingVariants.set(event, leaving)
  }
  return leaving
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
