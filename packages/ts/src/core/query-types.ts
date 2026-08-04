/** Query parameter types: named (object) or positional (array).
 * @public
 */
export type Params = Record<string, unknown> | unknown[]

/** How many nodes must acknowledge a write before it returns.
 * @public
 */
export type WriteConcernLevel = 'local' | 'majority' | 'all'

/** How many nodes must acknowledge a write, and how long the caller waits for them.
 * @public
 */
export interface WriteConcern {
  /** Number of acknowledgements the write waits for. */
  level: WriteConcernLevel
  /** Milliseconds to wait for those acknowledgements before the write fails. */
  timeoutMs?: number
}

/** How current a read has to be before the node will serve it.
 * @public
 */
export type ReadConcernLevel = 'local' | 'majority' | 'linearizable'

/** How current a read has to be before the node will serve it.
 * @public
 */
export interface ReadConcern {
  /** Currency the node must prove before it answers. */
  level: ReadConcernLevel
}

/** Per-statement settings you pass alongside the SQL and its parameters.
 * @public
 */
export interface QueryOptions {
  /** Acknowledgements a write waits for. Coordinator mode applies 'majority' when you omit it. */
  writeConcern?: WriteConcern
  /** Currency a read requires. Coordinator mode enforces it and static mode ignores it. */
  readConcern?: ReadConcern
}

/** Result returned by mutation statements (INSERT, UPDATE, DELETE).
 * @public
 */
export interface ExecuteResult {
  /** Number of rows the statement inserted, updated, or deleted. */
  changes: number
  /** Row id SQLite assigned to the last inserted row. */
  lastInsertRowId: number | bigint
}

/** CDC operation type.
 * @public
 */
export type ChangeOperation = 'insert' | 'update' | 'delete'

/** Event emitted when a watched table row changes.
 * @public
 */
export interface ChangeEvent<T = Record<string, unknown>> {
  /** Whether the row was inserted, updated, or deleted. */
  type: ChangeOperation
  /** Table the row belongs to. */
  table: string
  /** The row as it stands after the change. A delete carries the row as it was. */
  row: T
  /** The row as it stood before an update or a delete. */
  oldRow?: T
  /** Position of this change in the database's change log. Subscribers resume from it. */
  seq: bigint
  /** Milliseconds since the Unix epoch, taken when the change was recorded. */
  timestamp: number
  /** Hybrid logical clock stamp the writing node gave this change. */
  hlc?: string
  /** Identifier of the node that authored the change. */
  origin?: string
  /** Primary key of the changed row, encoded as a string. */
  rowId?: string
  /** Identifier of the transaction that produced this change. */
  txId?: string
  /** Set on the last change of a transaction, so a consumer applies the whole transaction at once. */
  txEnd?: boolean
}

/** Builder for creating CDC subscriptions with optional filters.
 * @public
 */
export interface SubscriptionBuilder {
  /**
   * Narrows the subscription to rows whose columns equal the given values.
   *
   * The filter reports membership of the matching set, so an update that moves a row
   * into the set arrives as an insert carrying no `oldRow`, and one that moves a row
   * out arrives as a delete carrying the old row and an empty `row`. An update that
   * leaves the row in the set arrives unchanged, and one that never touches the set
   * is not delivered. A synthesised event is indistinguishable from a real insert or
   * delete, so read `type` as the row's arrival or departure from the filter.
   */
  filter(conditions: Record<string, unknown>): SubscriptionBuilder
  /** Starts the subscription and calls back on each change. */
  subscribe(callback: (event: ChangeEvent) => void): Subscription
}

/** Handle for an active subscription.
 * @public
 */
export interface Subscription {
  /** Ends the subscription, so the callback receives no further events. */
  unsubscribe(): void
}
