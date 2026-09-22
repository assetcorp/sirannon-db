import type { SQLiteConnection } from '../driver/types.js'
import { CHANGES_TABLE, DEVICE_CURSORS_TABLE } from '../internal-tables.js'
import {
  deleteExpiredDeviceCursors,
  selectMinDeviceCursorBoundary,
  selectTableExists,
} from '../system-catalog/index.js'
import type { ChangeTracker } from './change-tracker.js'

export const DEFAULT_DEVICE_CURSOR_RETENTION_MS = 30 * 24 * 3_600_000

export interface DeviceRetentionPolicy {
  cursorRetentionMs: number
  maxChangesHeldForDevice: number
}

export interface DeviceRetentionOptions {
  deviceCursorRetention?: number
  maxChangesHeldForDevice?: number
}

export interface ChangeRetentionOptions extends DeviceRetentionOptions {
  cdcRetention?: number
}

export function resolveDeviceRetentionPolicy(
  options: DeviceRetentionOptions | undefined,
  fallback?: DeviceRetentionOptions,
): DeviceRetentionPolicy {
  return {
    cursorRetentionMs:
      options?.deviceCursorRetention ?? fallback?.deviceCursorRetention ?? DEFAULT_DEVICE_CURSOR_RETENTION_MS,
    maxChangesHeldForDevice: options?.maxChangesHeldForDevice ?? fallback?.maxChangesHeldForDevice ?? 0,
  }
}

async function bothTablesExist(conn: SQLiteConnection, changesTable: string): Promise<boolean> {
  if (!(await selectTableExists(conn, DEVICE_CURSORS_TABLE))) return false
  return selectTableExists(conn, changesTable)
}

export async function dropExpiredDeviceCursors(
  conn: SQLiteConnection,
  policy: DeviceRetentionPolicy,
  changesTable: string = CHANGES_TABLE,
): Promise<number> {
  if (!(await bothTablesExist(conn, changesTable))) return 0

  const cutoff = Date.now() / 1000 - policy.cursorRetentionMs / 1000
  return deleteExpiredDeviceCursors(conn, changesTable, {
    idleCutoff: cutoff,
    changeAgeCutoff: cutoff,
    maxChangesHeldForDevice: policy.maxChangesHeldForDevice,
  })
}

export async function resolveMinDeviceCursorBoundary(
  conn: SQLiteConnection,
  changesTable: string = CHANGES_TABLE,
): Promise<bigint | null> {
  if (!(await bothTablesExist(conn, changesTable))) return null
  return selectMinDeviceCursorBoundary(conn, changesTable)
}

export async function applyDeviceCursorBoundary(
  conn: SQLiteConnection,
  tracker: ChangeTracker,
  policy: DeviceRetentionPolicy,
): Promise<void> {
  const changesTable = tracker.changeLogTable
  await dropExpiredDeviceCursors(conn, policy, changesTable)
  const boundary = await resolveMinDeviceCursorBoundary(conn, changesTable)
  if (boundary === null) {
    tracker.clearPruneBoundary('device-cursors')
    return
  }
  tracker.setPruneBoundary('device-cursors', boundary)
}
