import { Database } from '@delali/sirannon-db'
import {
  type SnapshotOutcome,
  type SnapshotProgress,
  SyncController,
  type SyncStatus,
} from '@delali/sirannon-db/client'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'
import type { LiveDatabase } from '@delali/sirannon-db/react'
import {
  DATABASE_ID,
  migrations,
  SEED_INSERT_SQL,
  SEED_UPDATED_AT,
  SEED_WORK_ORDERS,
  WORK_ORDERS_TABLE,
} from '../schema'

export const WORK_ORDERS_QUERY = `SELECT id, site, task, status, technician, note, updated_at
FROM ${WORK_ORDERS_TABLE} ORDER BY site, task`

export interface DeviceSessionHooks {
  onStatusChange: (status: SyncStatus) => void
  onServerChange: () => void
  onResyncRequired: () => void
  onSnapshotProgress: (progress: SnapshotProgress) => void
  onSnapshotComplete: (outcome: SnapshotOutcome) => void
}

export interface FieldDevice {
  readonly name: string
  readonly db: Database
  readonly liveDb: LiveDatabase
  readonly sync: SyncController
  readonly neverSynced: boolean
}

export async function openFieldDevice(
  name: string,
  serverUrl: string,
  hooks: DeviceSessionHooks,
): Promise<FieldDevice> {
  const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
  const db = await Database.create(DATABASE_ID, `/${DATABASE_ID}-${name}.db`, driver, {
    readPoolSize: 1,
    walMode: false,
  })

  await db.migrate([...migrations])
  await db.watch(WORK_ORDERS_TABLE)

  const neverSynced = (await db.deviceSync().getPullState()) === null
  if (neverSynced) {
    await seedIfEmpty(db)
  }

  const sync = new SyncController(db, {
    url: serverUrl,
    databaseId: DATABASE_ID,
    tables: [WORK_ORDERS_TABLE],
    pushIntervalMs: 500,
    onStatusChange: hooks.onStatusChange,
    onChange: hooks.onServerChange,
    onResyncRequired: hooks.onResyncRequired,
    onSnapshotProgress: hooks.onSnapshotProgress,
    onSnapshotComplete: hooks.onSnapshotComplete,
  })

  const liveDb: LiveDatabase = db
  return { name, db, liveDb, sync, neverSynced }
}

async function seedIfEmpty(db: Database): Promise<void> {
  const existing = await db.queryOne<{ count: number }>(`SELECT count(*) AS count FROM ${WORK_ORDERS_TABLE}`)
  if ((existing?.count ?? 0) > 0) return
  for (const order of SEED_WORK_ORDERS) {
    await db.execute(SEED_INSERT_SQL, [order.id, order.site, order.task, SEED_UPDATED_AT])
  }
}

export async function closeFieldDevice(device: FieldDevice): Promise<void> {
  await device.sync.stop().catch(() => undefined)
  if (!device.db.closed) {
    await device.db.close().catch(() => undefined)
  }
}

export async function createWorkOrder(device: FieldDevice, site: string, task: string): Promise<void> {
  await device.db.execute(
    `INSERT INTO ${WORK_ORDERS_TABLE} (id, site, task, status, technician, note, updated_at)
     VALUES (?, ?, ?, 'scheduled', '', '', ?)`,
    [crypto.randomUUID(), site, task, new Date().toISOString()],
  )
  device.sync.triggerPush()
}

export async function claimWorkOrder(device: FieldDevice, id: string): Promise<void> {
  await device.db.execute(
    `UPDATE ${WORK_ORDERS_TABLE} SET status = 'in_progress', technician = ?, updated_at = ? WHERE id = ?`,
    [device.name, new Date().toISOString(), id],
  )
  device.sync.triggerPush()
}

export async function completeWorkOrder(device: FieldDevice, id: string, note: string): Promise<void> {
  await device.db.execute(`UPDATE ${WORK_ORDERS_TABLE} SET status = 'done', note = ?, updated_at = ? WHERE id = ?`, [
    note,
    new Date().toISOString(),
    id,
  ])
  device.sync.triggerPush()
}

export async function reopenWorkOrder(device: FieldDevice, id: string): Promise<void> {
  await device.db.execute(
    `UPDATE ${WORK_ORDERS_TABLE} SET status = 'scheduled', technician = '', note = '', updated_at = ? WHERE id = ?`,
    [new Date().toISOString(), id],
  )
  device.sync.triggerPush()
}
