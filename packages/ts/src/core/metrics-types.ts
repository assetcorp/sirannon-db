import type { ChangeOperation } from './query-types.js'

/** Metrics emitted after a query completes.
 * @public
 */
export interface QueryMetrics {
  /** Identifier of the database the statement ran against. */
  databaseId: string
  /** The statement that ran. */
  sql: string
  /** How long the statement took, in milliseconds. */
  durationMs: number
  /** Number of rows a read returned. */
  rowsReturned?: number
  /** Number of rows a write changed. */
  changes?: number
  /** Set when the statement threw. */
  error?: boolean
}

/** Metrics emitted when a connection opens or closes.
 * @public
 */
export interface ConnectionMetrics {
  /** Identifier of the database whose connection opened or closed. */
  databaseId: string
  /** File path of the SQLite database. */
  path: string
  /** Number of read connections the pool holds. */
  readerCount: number
  /** Whether the connection opened or closed. */
  event: 'open' | 'close'
}

/** Metrics emitted when a CDC event is dispatched.
 * @public
 */
export interface CDCMetrics {
  /** Identifier of the database the change came from. */
  databaseId: string
  /** Table the changed row belongs to. */
  table: string
  /** Whether the row was inserted, updated, or deleted. */
  operation: ChangeOperation
  /** Number of subscribers the event reached. */
  subscriberCount: number
}

/** Callbacks for metrics collection.
 * @public
 */
export interface MetricsConfig {
  /** Called once each statement finishes, whether it succeeded or threw. */
  onQueryComplete?: (metrics: QueryMetrics) => void
  /** Called when a database connection opens. */
  onConnectionOpen?: (metrics: ConnectionMetrics) => void
  /** Called when a database connection closes. */
  onConnectionClose?: (metrics: ConnectionMetrics) => void
  /** Called each time a change event reaches its subscribers. */
  onCDCEvent?: (metrics: CDCMetrics) => void
}
