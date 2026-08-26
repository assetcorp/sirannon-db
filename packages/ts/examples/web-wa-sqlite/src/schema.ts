import type { Migration } from '@delali/sirannon-db'

export const DATABASE_ID = 'field-service'

export const WORK_ORDERS_TABLE = 'work_orders'

export const WORK_ORDER_STATUSES = ['scheduled', 'in_progress', 'done'] as const

export const MAX_SITE_LENGTH = 80
export const MAX_TASK_LENGTH = 120
export const MAX_TECHNICIAN_LENGTH = 40
export const MAX_NOTE_LENGTH = 200
export const MAX_TIMESTAMP_LENGTH = 32
export const MAX_WORK_ORDERS = 500

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
  {
    version: 2,
    name: 'limit_work_order_text',
    up: `CREATE TABLE work_orders_bounded (
  id TEXT PRIMARY KEY,
  site TEXT NOT NULL CHECK (length(site) <= ${MAX_SITE_LENGTH}),
  task TEXT NOT NULL CHECK (length(task) <= ${MAX_TASK_LENGTH}),
  status TEXT NOT NULL DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'in_progress', 'done')),
  technician TEXT NOT NULL DEFAULT '' CHECK (length(technician) <= ${MAX_TECHNICIAN_LENGTH}),
  note TEXT NOT NULL DEFAULT '' CHECK (length(note) <= ${MAX_NOTE_LENGTH}),
  updated_at TEXT NOT NULL CHECK (length(updated_at) <= ${MAX_TIMESTAMP_LENGTH})
);
INSERT INTO work_orders_bounded (id, site, task, status, technician, note, updated_at)
SELECT id,
       substr(site, 1, ${MAX_SITE_LENGTH}),
       substr(task, 1, ${MAX_TASK_LENGTH}),
       CASE WHEN status IN ('scheduled', 'in_progress', 'done') THEN status ELSE 'scheduled' END,
       substr(technician, 1, ${MAX_TECHNICIAN_LENGTH}),
       substr(note, 1, ${MAX_NOTE_LENGTH}),
       substr(updated_at, 1, ${MAX_TIMESTAMP_LENGTH})
FROM work_orders;
DROP TABLE work_orders;
ALTER TABLE work_orders_bounded RENAME TO work_orders`,
    down: `CREATE TABLE work_orders_unbounded (
  id TEXT PRIMARY KEY,
  site TEXT NOT NULL,
  task TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'scheduled',
  technician TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
INSERT INTO work_orders_unbounded (id, site, task, status, technician, note, updated_at)
SELECT id, site, task, status, technician, note, updated_at FROM work_orders;
DROP TABLE work_orders;
ALTER TABLE work_orders_unbounded RENAME TO work_orders`,
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
