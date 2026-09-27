/** The values to bind to a statement, as an object for named parameters or an array for positional ones.
 * @public
 */
export type Params = Record<string, unknown> | unknown[]

/** Which nodes must acknowledge a write before the call returns.
 * @public
 */
export type WriteConcernLevel = 'local' | 'majority' | 'all'

/** Which nodes must acknowledge a write, and how long the caller waits for their acknowledgements.
 * @public
 */
export interface WriteConcern {
  /** Which nodes must acknowledge the write before the call returns. */
  level: WriteConcernLevel
  /** The number of milliseconds to wait for those acknowledgements before the call fails. */
  timeoutMs?: number
}

/** How current the data must be before a node serves a read.
 * @public
 */
export type ReadConcernLevel = 'local' | 'majority' | 'linearizable'

/** How current the data must be before a node serves a read.
 * @public
 */
export interface ReadConcern {
  /** The level that the node must confirm before it responds. */
  level: ReadConcernLevel
}

/** Per-statement settings that you pass with the SQL and its parameters.
 * @public
 */
export interface QueryOptions {
  /** The acknowledgements that a write waits for; in coordinator mode, the replication engine applies 'majority' when you omit it. */
  writeConcern?: WriteConcern
  /** How current the data must be for a read; in coordinator mode, the replication engine enforces it and applies 'majority' when you omit it, while in static mode the engine ignores the setting. */
  readConcern?: ReadConcern
}

/** The result of a write statement such as INSERT, UPDATE, or DELETE.
 * @public
 */
export interface ExecuteResult {
  /** The number of rows that the statement inserted, updated, or deleted. */
  changes: number
  /** The row id that SQLite assigned to the last inserted row. */
  lastInsertRowId: number | bigint
}

/** The kind of change that a change event records.
 * @public
 */
export type ChangeOperation = 'insert' | 'update' | 'delete'

/** The event that Sirannon delivers when a row in a watched table changes.
 * @public
 */
export interface ChangeEvent<T = Record<string, unknown>> {
  /** Whether the change inserted, updated, or deleted the row. */
  type: ChangeOperation
  /** The table that contains the row. */
  table: string
  /** The row after the change; for a delete, this is an empty object and {@link ChangeEvent.oldRow} contains the previous row. */
  row: T
  /** The row before an update or a delete. */
  oldRow?: T
  /** The position of this change in the database's change log, which a subscriber resumes from. */
  seq: bigint
  /** The time when Sirannon recorded the change, in milliseconds since the Unix epoch. */
  timestamp: number
  /** The hybrid logical clock stamp that the writing node gave this change. */
  hlc?: string
  /** The identifier of the node that wrote the change. */
  origin?: string
  /** The primary key of the changed row, encoded as a string. */
  rowId?: string
  /** The identifier of the transaction that made this change. */
  txId?: string
  /** True on the last change of a transaction, so that a consumer can apply the whole transaction at once. */
  txEnd?: boolean
}

/** Builds a change subscription with an optional filter.
 * @public
 */
export interface SubscriptionBuilder {
  /**
   * Narrows the subscription to rows whose columns equal the given values.
   *
   * Sirannon delivers an update that moves a row into the matching set as an
   * insert with no `oldRow`, and an update that moves a row out of the set as a
   * delete with the old row and an empty `row`. It delivers an update that keeps
   * the row in the set unchanged, and no event for an update whose row is
   * outside the set both before and after. A synthesised event looks the same as
   * a real insert or delete, so read `type` as the row entering or leaving the
   * filter.
   */
  filter(conditions: Record<string, unknown>): SubscriptionBuilder
  /**
   * Starts the subscription and calls the callback for each matching change.
   *
   * Sirannon never awaits the promise that your callback returns, so two calls to
   * an asynchronous callback can overlap; chain the work onto one promise when
   * each change has to finish before the next one starts. Sirannon passes a throw
   * or a rejection from the callback to `options.onError`, and still delivers the
   * change to every other subscriber on this table.
   *
   * @typeParam T - The shape of the table's rows, which types `row` and `oldRow`.
   * @param callback - The function that Sirannon calls with each change that this subscription matches.
   * @param options - The subscription options, including `onError`, which Sirannon calls with a failure of the callback or of the change-log poll.
   * @returns A handle whose `unsubscribe` ends the subscription.
   */
  subscribe<T = Record<string, unknown>>(
    callback: (event: ChangeEvent<T>) => void,
    options?: SubscriptionOptions,
  ): Subscription
}

/** The failure reporter for a change subscription.
 * @public
 */
export interface SubscriptionOptions {
  /**
   * Sirannon calls this with the failure of a change callback, and with the
   * failure that stops the change-log poll after ten consecutive errors.
   * Sirannon discards anything that this reporter throws.
   */
  onError?: (error: Error) => void
}

/** The handle of an active subscription.
 * @public
 */
export interface Subscription {
  /** Ends the subscription, after which Sirannon delivers no further events to the callback. */
  unsubscribe(): void
}
