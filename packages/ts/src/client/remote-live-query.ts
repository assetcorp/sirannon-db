import { invokeCallerCallback } from '../core/caller-callbacks.js'
import type { LiveQuery, LiveQueryState, LiveUpdate, ResultOp } from '../core/live/types.js'
import type { LiveHandlers, RemoteSubscription } from './types.js'
import { RemoteError } from './types.js'

/**
 * A live query running against a remote server, which keeps its rows current as the tables behind it change.
 *
 * @public
 */
export class RemoteLiveQuery<T> implements LiveQuery<T> {
  private state: LiveQueryState<T> = { status: 'pending' }
  private rows: T[] = []
  private readonly listeners = new Set<(update: LiveUpdate<T>) => void>()
  private subscription: RemoteSubscription | null = null
  private closed = false

  private onError: ((error: Error) => void) | undefined

  /** @internal */
  static async open<T>(
    subscribe: (handlers: LiveHandlers) => Promise<RemoteSubscription>,
    onError?: (error: Error) => void,
  ): Promise<RemoteLiveQuery<T>> {
    const query = new RemoteLiveQuery<T>()
    query.onError = onError
    try {
      query.subscription = await subscribe(query.handlers())
    } catch (err) {
      query.closed = true
      throw err
    }

    if (query.state.status === 'error') {
      const failure = query.state.error
      await query.close()
      throw failure
    }
    return query
  }

  /**
   * Returns the rows the query holds right now.
   */
  getState(): LiveQueryState<T> {
    return this.state
  }

  /**
   * Calls back on each update and returns a function that stops the listener.
   *
   * @param listener - Receives each update.
   * @returns A function that removes the listener.
   */
  subscribe(listener: (update: LiveUpdate<T>) => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  /**
   * Ends the query and releases its subscription.
   */
  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.listeners.clear()
    this.subscription?.unsubscribe()
    this.subscription = null
  }

  private handlers(): LiveHandlers {
    return {
      onRows: rows => this.replace(rows as T[]),
      onOps: ops => this.apply(ops as ResultOp<T>[]),
      onRevalidating: () => this.markRevalidating(),
      onError: error => this.fail(error),
    }
  }

  private replace(rows: T[]): void {
    if (this.closed) return
    this.rows = rows
    this.publish({ status: 'ready', rows: this.rows, revalidating: false }, { kind: 'rows' })
  }

  private apply(ops: ResultOp<T>[]): void {
    if (this.closed) return
    if (this.state.status !== 'ready') {
      this.fail(new RemoteError('INVALID_RESPONSE', 'A live query received row operations before its first result'))
      return
    }

    const next = this.rows.slice()
    for (const op of ops) {
      const highest = op.op === 'insert' ? next.length : next.length - 1
      if (!Number.isInteger(op.index) || op.index < 0 || op.index > highest) {
        this.fail(new RemoteError('INVALID_RESPONSE', 'A live query received an operation outside its rows'))
        return
      }
      if (op.op === 'insert') next.splice(op.index, 0, op.row)
      else if (op.op === 'update') next[op.index] = op.row
      else next.splice(op.index, 1)
    }

    this.rows = next
    this.publish({ status: 'ready', rows: this.rows, revalidating: false }, { kind: 'ops', ops })
  }

  private markRevalidating(): void {
    if (this.closed || this.state.status !== 'ready' || this.state.revalidating) return
    this.publish({ status: 'ready', rows: this.rows, revalidating: true }, { kind: 'revalidating' })
  }

  private fail(error: Error): void {
    if (this.closed) return
    this.publish({ status: 'error', error }, { kind: 'error' })
  }

  private publish(next: LiveQueryState<T>, update: LiveUpdate<T>): void {
    this.state = next
    for (const listener of [...this.listeners]) {
      invokeCallerCallback(() => listener(update), this.onError)
    }
  }
}
