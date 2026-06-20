export interface CDCEvent {
  type: 'insert' | 'update' | 'delete'
  table: string
  row: Record<string, unknown>
  oldRow?: Record<string, unknown>
  seq: bigint
  timestamp: number
}
