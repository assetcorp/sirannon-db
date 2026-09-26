export type {
  ChangeEvent,
  ChangeOperation,
  Subscription,
  SubscriptionBuilder,
} from '../types.js'

/**
 * Describes one row of the change log table.
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
 * Describes one column of a watched table, as `PRAGMA table_info` returns it.
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
 * Describes one watched table with the columns and primary-key columns that its change-capture triggers use.
 *
 * @internal
 */
export interface WatchedTableInfo {
  table: string
  columns: string[]
  pkColumns: string[]
}

/**
 * Configures a change tracker's retention period, change log table, and poll size.
 *
 * @public
 */
export interface ChangeTrackerOptions {
  /**
   * The age in milliseconds after which the tracker's cleanup can delete a change, which defaults to `3_600_000`, one hour.
   */
  retention?: number
  /**
   * The name of the change log table, which defaults to `_sirannon_changes`.
   */
  changesTable?: string
  /**
   * The number of changes that one poll requests, which defaults to 1000. A poll can return more, so that it ends on a transaction boundary.
   */
  pollBatchSize?: number
}
