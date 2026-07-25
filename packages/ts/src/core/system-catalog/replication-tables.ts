import type { SQLiteConnection } from '../driver/types.js'
import { ensureAppliedChangesTable } from './applied-changes-table.js'
import { ensureColumnVersionsTable } from './column-versions-table.js'
import { ensurePeerStateTable } from './peer-state-table.js'
import { ensureSyncStateTable } from './sync-state-table.js'

export async function ensureBatchApplyTables(conn: SQLiteConnection): Promise<void> {
  await ensureAppliedChangesTable(conn)
  await ensureColumnVersionsTable(conn)
}

export async function ensureReplicationStateTables(conn: SQLiteConnection): Promise<void> {
  await ensurePeerStateTable(conn)
  await ensureBatchApplyTables(conn)
  await ensureSyncStateTable(conn)
}
