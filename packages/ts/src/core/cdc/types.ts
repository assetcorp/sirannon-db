export type {
  ChangeEvent,
  ChangeOperation,
  Subscription,
  SubscriptionBuilder,
} from '../types.js'

/**
 * One row as it is stored in the change log table.
 *
 * @internal
 */
export interface ChangeRow {
  seq: number
  table_name: string
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  row_id: number | string
  changed_at: number
  old_data: string | null
  new_data: string | null
  node_id?: string
  tx_id?: string
  hlc?: string
}

/**
 * One column of a watched table, as read from the SQLite catalogue.
 *
 * @internal
 */
export interface ColumnInfo {
  cid: number
  name: string
  type: string
  notnull: number
  dflt_value: string | null
  pk: number
}

/**
 * Trigger state a change tracker holds for one watched table.
 *
 * @internal
 */
export interface WatchedTableInfo {
  table: string
  columns: string[]
  pkColumns: string[]
}

/**
 * How long a change tracker keeps changes, and how much it reads at a time.
 *
 * @public
 */
export interface ChangeTrackerOptions {
  /**
   * Milliseconds a change stays readable before it is pruned. Default: 3_600_000 (one hour).
   */
  retention?: number
  /**
   * Name of the table changes are recorded in.
   */
  changesTable?: string
  /**
   * Changes read in one poll. Default: 1000.
   */
  pollBatchSize?: number
}
