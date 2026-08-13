import type { Migration } from '@delali/sirannon-db'

export const DATABASE_ID = 'field-service'

export const WORK_ORDERS_TABLE = 'work_orders'

export const WORK_ORDER_STATUSES = ['scheduled', 'in_progress', 'done'] as const

export type WorkOrderStatus = (typeof WORK_ORDER_STATUSES)[number]

export interface WorkOrder {
  id: string
  site: string
  task: string
  status: WorkOrderStatus
  technician: string
  note: string
  updated_at: string
}

export const migrations: readonly Migration[] = [
  {
    version: 1,
    name: 'create_work_orders',
    up: `CREATE TABLE work_orders (
  id TEXT PRIMARY KEY,
  site TEXT NOT NULL,
  task TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'scheduled',
  technician TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
)`,
    down: 'DROP TABLE work_orders',
  },
]

export const SEED_WORK_ORDERS: readonly Pick<WorkOrder, 'id' | 'site' | 'task'>[] = [
  { id: 'wo-1041', site: 'Northgate substation', task: 'Replace the isolation transformer' },
  { id: 'wo-1042', site: 'Harbour pumping station', task: 'Inspect the switchgear' },
  { id: 'wo-1043', site: 'Riverside intake', task: 'Clear the intake screen' },
  { id: 'wo-1044', site: 'Eastfield depot', task: 'Recalibrate the flow meter' },
]

export const SEED_UPDATED_AT = '2026-01-05T08:00:00.000Z'

export const SEED_INSERT_SQL = `INSERT INTO ${WORK_ORDERS_TABLE} (id, site, task, status, technician, note, updated_at)
VALUES (?, ?, ?, 'scheduled', '', '', ?)`

export function isWorkOrderStatus(value: string): value is WorkOrderStatus {
  return (WORK_ORDER_STATUSES as readonly string[]).includes(value)
}
