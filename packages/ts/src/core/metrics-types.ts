import type { ChangeOperation } from './query-types.js'

/** The metrics that Sirannon reports after each statement finishes.
 * @public
 */
export interface QueryMetrics {
  /** The identifier of the database that the statement executed against. */
  databaseId: string
  /** The statement that executed. */
  sql: string
  /** The statement's duration, in milliseconds. */
  durationMs: number
  /** The number of rows that a read returns, and absent for a write or a failed statement. */
  rowsReturned?: number
  /** The number of rows that a write changes, and absent for a read or a failed statement. */
  changes?: number
  /** True when the statement threw. */
  error?: boolean
}

/** The metrics that Sirannon reports when the registry opens or closes a database.
 * @public
 */
export interface ConnectionMetrics {
  /** The identifier of the database that the registry opened or closed. */
  databaseId: string
  /** The file path of the SQLite database. */
  path: string
  /** The number of read connections in the database's pool, which is 0 on close. */
  readerCount: number
  /** Whether the registry opened or closed the database. */
  event: 'open' | 'close'
}

/** The metrics for one change event and its delivery to subscribers.
 * @public
 */
export interface CDCMetrics {
  /** The identifier of the database that the change comes from. */
  databaseId: string
  /** The table that contains the changed row. */
  table: string
  /** Whether the change inserted, updated, or deleted the row. */
  operation: ChangeOperation
  /** The number of subscribers whose filter matches the event and that Sirannon delivers it to, which is zero when none matches. */
  subscriberCount: number
}

/** The callbacks that receive Sirannon's metrics.
 * @public
 */
export interface MetricsConfig {
  /** Sirannon calls this once each statement finishes, whether the statement succeeded or threw. */
  onQueryComplete?: (metrics: QueryMetrics) => void
  /** Sirannon calls this when the registry opens a database. */
  onConnectionOpen?: (metrics: ConnectionMetrics) => void
  /** Sirannon calls this when the registry closes a database. */
  onConnectionClose?: (metrics: ConnectionMetrics) => void
  /** The callback for each change event that Sirannon dispatches to a database's subscribers, both those of `Database.on` and those of a server's WebSocket connections. */
  onCDCEvent?: (metrics: CDCMetrics) => void
}
