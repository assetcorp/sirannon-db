/** Describes a live query's state, which is pending until the first read returns, ready with the current rows, or an error after a failure.
 * @public
 */
export type LiveQueryState<T> =
  | {
      /** `'pending'` while the first read is in flight, `'ready'` once the rows are available, or `'error'` after a failure. */
      status: 'pending'
    }
  | {
      /** Marks the state that has rows. */
      status: 'ready'
      /** The current rows of the query, in its result order. */
      rows: readonly T[]
      /** `true` while Sirannon re-reads the query, in which case `rows` is the previous result until the re-read finishes. */
      revalidating: boolean
    }
  | {
      /** Marks the state after a failure. */
      status: 'error'
      /** The error that the read or its change subscription threw. */
      error: Error
    }

/** Describes one edit to a live query's result set, as a position and the row at that position.
 * @public
 */
export type ResultOp<T> =
  | {
      /** `'insert'` for an added row, `'update'` for a row that changed in place, or `'delete'` for a removed row. */
      op: 'insert'
      /** The zero-based position of the inserted row in the result set. */
      index: number
      /** The row at that position after the edit. */
      row: T
    }
  | {
      /** Marks an edit to a row that changed in place. */
      op: 'update'
      /** The zero-based position of the row that changed. */
      index: number
      /** The row after the change. */
      row: T
    }
  | {
      /** Marks the removal of a row. */
      op: 'delete'
      /** The zero-based position of the removed row before the edit. */
      index: number
    }

/** Describes an update that a live query sends to its listeners, for a replaced result set, edits to apply, a re-read in progress, or a failure.
 * @public
 */
export type LiveUpdate<T> =
  | {
      /** `'rows'` for a replaced result set, `'ops'` for edits to apply, `'revalidating'` while Sirannon re-reads the query, or `'error'` after a failure. */
      kind: 'rows'
    }
  | {
      /** Marks an update with edits to apply. */
      kind: 'ops'
      /** The edits, in the order in which to apply them to reach the new result set. */
      ops: readonly ResultOp<T>[]
    }
  | {
      /** Marks the start of a re-read. */
      kind: 'revalidating'
    }
  | {
      /** Marks a failure, whose error {@link LiveQuery.getState} returns. */
      kind: 'error'
    }

/** A registered read whose rows Sirannon updates as the underlying tables change.
 * @public
 */
export interface LiveQuery<T = Record<string, unknown>> {
  /** Returns the query's current state, with the rows when the state is ready. */
  getState(): LiveQueryState<T>
  /** Registers a listener for each update, and returns a function that removes it. */
  subscribe(listener: (update: LiveUpdate<T>) => void): () => void
  /** Closes the query and releases its change subscription. */
  close(): Promise<void>
}

/** Settings for one live query.
 * @public
 */
export interface LiveQueryOptions {
  /** The upper bound in milliseconds of a random delay before each re-read, so that many queries affected by one change re-read at different moments. */
  rereadJitterMs?: number
  /** The largest number of changes in one transaction that Sirannon applies to the result one by one. For a larger transaction, Sirannon re-reads the whole query. */
  maxTransactionChanges?: number
  /**
   * Receives the error from any listener of this query that throws or returns a rejected promise.
   * Sirannon calls each listener without awaiting it, so the other listeners still receive the
   * update. Sirannon discards any error that `onError` itself throws.
   */
  onError?: (error: Error) => void
}
