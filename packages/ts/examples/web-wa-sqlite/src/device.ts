import { Database } from '@delali/sirannon-db'
import { type SnapshotOutcome, type SnapshotProgress, SyncController } from '@delali/sirannon-db/client'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'
import { DATABASE_ID, migrations, WORK_ORDERS_TABLE, type WorkOrder } from './schema'

const DEVICE_NAME_RE = /^[a-z0-9][a-z0-9-]{0,31}$/
const DEFAULT_DEVICE_NAME = 'van-1'

export interface DeviceHooks {
  onServerChange: () => void
  onResyncRequired: () => void
  onSnapshotProgress: (progress: SnapshotProgress) => void
  onSnapshotComplete: (outcome: SnapshotOutcome) => void
}

export interface FieldDevice {
  readonly name: string
  readonly db: Database
  readonly sync: SyncController
  readonly neverSynced: boolean
}

export function deviceNameFromLocation(search: string): string {
  const requested = new URLSearchParams(search).get('device')?.trim().toLowerCase()
  if (requested === undefined || requested.length === 0) {
    return DEFAULT_DEVICE_NAME
  }
  if (!DEVICE_NAME_RE.test(requested)) {
    throw new Error(
      `Device name '${requested}' is not usable. Use lowercase letters, digits, and hyphens, up to 32 characters.`,
    )
  }
  return requested
}

export async function openFieldDevice(name: string, serverUrl: string, hooks: DeviceHooks): Promise<FieldDevice> {
  const driver = waSqlite({ vfs: 'IDBBatchAtomicVFS' })
  const db = await Database.create(DATABASE_ID, `/${DATABASE_ID}-${name}.db`, driver, {
    readPoolSize: 1,
    walMode: false,
    cdcPollInterval: 50,
  })

  await db.migrate([...migrations])
  await db.watch(WORK_ORDERS_TABLE)

  const neverSynced = (await db.deviceSync().getPullState()) === null

  const sync = new SyncController(db, {
    url: serverUrl,
    databaseId: DATABASE_ID,
    tables: [WORK_ORDERS_TABLE],
    pushIntervalMs: 500,
    onChange: hooks.onServerChange,
    onResyncRequired: hooks.onResyncRequired,
    onSnapshotProgress: hooks.onSnapshotProgress,
    onSnapshotComplete: hooks.onSnapshotComplete,
  })

  return { name, db, sync, neverSynced }
}

export function openWorkOrders(db: Database) {
  return db.live<WorkOrder>(
    `SELECT id, site, task, status, technician, note, updated_at FROM ${WORK_ORDERS_TABLE} ORDER BY site, task`,
  )
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
