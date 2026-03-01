export type {
  ChangeEvent,
  ChangeOperation,
  Subscription,
  SubscriptionBuilder,
} from '../types.js'

export interface ChangeRow {
  seq: number
  table_name: string
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  row_id: number | string
  changed_at: number
  old_data: string | null
  new_data: string | null
}

export interface ColumnInfo {
  cid: number
  name: string
  type: string
  notnull: number
  dflt_value: string | null
  pk: number
}

export interface WatchedTableInfo {
  table: string
  columns: string[]
  pkColumns: string[]
}

export interface ChangeTrackerOptions {
  retention?: number
  changesTable?: string
  pollBatchSize?: number
}
