export interface ChangeRow {
  seq: number
  table_name: string
  operation: string
  row_id: string
  changed_at: number
  old_data: string | null
  new_data: string | null
  node_id: string
  tx_id: string
  hlc: string
}

export interface ColumnInfoRow {
  name: string
  pk: number
}
