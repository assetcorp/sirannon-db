/** Where a live query stands: waiting for its first rows, holding rows, or failed.
 * @public
 */
export type LiveQueryState<T> =
  | {
      /** Where the query stands: `'pending'` while its first read is in flight, `'ready'` once it holds rows, `'error'` after a failure. */
      status: 'pending'
    }
  | {
      /** Names this state as one holding rows. */
      status: 'ready'
      /** The rows the query holds, in the order the read returned them. */
      rows: readonly T[]
      /** True while the query re-reads, and the rows above stay readable throughout. */
      revalidating: boolean
    }
  | {
      /** Names this state as one holding the failure the query met. */
      status: 'error'
      /** What the read or the subscription behind it threw. */
      error: Error
    }

/** One change to a live query's result set, as a position and the row at it.
 * @public
 */
export type ResultOp<T> =
  | {
      /** Which edit this is: `'insert'` for a row added, `'update'` for a row changed in place, `'delete'` for a row removed. */
      op: 'insert'
      /** Position in the result set the edit applies to, counted from zero. */
      index: number
      /** The row that now sits at that position. */
      row: T
    }
  | {
      /** Names this edit as a row changed in place. */
      op: 'update'
      /** Position of the row that changed. */
      index: number
      /** The row as it now reads. */
      row: T
    }
  | {
      /** Names this edit as a row removed. */
      op: 'delete'
      /** Position the removed row held. */
      index: number
    }

/** What a live query tells its listeners: the rows replaced, edited in place, being re-read, or failed.
 * @public
 */
export type LiveUpdate<T> =
  | {
      /** Which update this is: `'rows'` for a replaced result set, `'ops'` for edits to apply, `'revalidating'` while the query re-reads, `'error'` after a failure. */
      kind: 'rows'
    }
  | {
      /** Names this update as one carrying edits. */
      kind: 'ops'
      /** The edits that move the result set to its new state, in the order to apply them. */
      ops: readonly ResultOp<T>[]
    }
  | {
      /** Names this update as notice that the query has started re-reading. */
      kind: 'revalidating'
    }
  | {
      /** Names this update as notice that the query failed. */
      kind: 'error'
    }

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
  /**
   * Receives the failure of any listener this query calls. The query never waits for what a
   * listener returns, so a throw and a rejection both arrive here, and every other listener
   * still receives the update. Sirannon drops whatever this reporter itself throws.
   */
  onError?: (error: Error) => void
}
