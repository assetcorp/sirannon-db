/** Where a live query stands: waiting for its first rows, holding rows, or failed.
 * @public
 */
export type LiveQueryState<T> =
  | { status: 'pending' }
  | { status: 'ready'; rows: readonly T[]; revalidating: boolean }
  | { status: 'error'; error: Error }

/** One change to a live query's result set, as a position and the row at it.
 * @public
 */
export type ResultOp<T> =
  | { op: 'insert'; index: number; row: T }
  | { op: 'update'; index: number; row: T }
  | { op: 'delete'; index: number }

/** What a live query tells its listeners: the rows replaced, edited in place, being re-read, or failed.
 * @public
 */
export type LiveUpdate<T> =
  | { kind: 'rows' }
  | { kind: 'ops'; ops: readonly ResultOp<T>[] }
  | { kind: 'revalidating' }
  | { kind: 'error' }

/** A registered read that keeps its rows current as the underlying tables change.
 * @public
 */
export interface LiveQuery<T = Record<string, unknown>> {
  /** Returns the rows the query holds right now. */
  getState(): LiveQueryState<T>
  /** Calls back on each update and returns a function that stops the listener. */
  subscribe(listener: (update: LiveUpdate<T>) => void): () => void
  /** Ends the query and releases its subscription. */
  close(): Promise<void>
}

/** Settings for one live query.
 * @public
 */
export interface LiveQueryOptions {
  /** Milliseconds of random delay before a re-read, which spreads the load of many queries reacting at once. */
  rereadJitterMs?: number
  /** Changes in one transaction above which the query re-reads instead of applying them one by one. */
  maxTransactionChanges?: number
}
