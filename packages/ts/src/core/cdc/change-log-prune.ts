import type { SQLiteConnection } from '../driver/types.js'
import { DEVICE_CURSORS_TABLE } from '../internal-tables.js'
import { selectMinChangedAt, selectMinDeviceCursorWindowStart, selectTableExists } from '../system-catalog/index.js'
import type { ChangeTracker } from './change-tracker.js'
import { applyDeviceCursorBoundary, type DeviceRetentionPolicy } from './device-retention.js'

const EXPIRY_SLACK_MS = 10

/**
 * Drops the expired device cursors, prunes the change log, and returns when the next prune can remove more, so that a
 * change feed of a database that nobody writes to prunes again only when a change or a cursor passes its window.
 *
 * @param conn - The writer connection, which the caller holds under the writer lock.
 * @param tracker - The tracker whose change log and prune boundaries the prune uses.
 * @param policy - How long and how many changes a device cursor can hold back.
 * @param retentionMs - How long the change log keeps a change.
 * @returns The time in milliseconds since the Unix epoch at which the oldest remaining change or device cursor passes
 * its window, or `null` when neither will without another write.
 * @internal
 */
export async function pruneChangeLog(
  conn: SQLiteConnection,
  tracker: ChangeTracker,
  policy: DeviceRetentionPolicy,
  retentionMs: number,
): Promise<number | null> {
  await applyDeviceCursorBoundary(conn, tracker, policy)
  await tracker.cleanup(conn)

  const changesTable = tracker.changeLogTable
  if (!(await selectTableExists(conn, changesTable))) return null
  const now = Date.now()
  const oldestChange = await selectMinChangedAt(conn, changesTable)
  let next = oldestChange === null ? null : futureOrNull(oldestChange * 1000 + retentionMs + EXPIRY_SLACK_MS, now)
  if (await selectTableExists(conn, DEVICE_CURSORS_TABLE)) {
    const windowStart = await selectMinDeviceCursorWindowStart(conn, changesTable)
    if (windowStart !== null) {
      next = earlierOf(next, futureOrNull(windowStart * 1000 + policy.cursorRetentionMs + EXPIRY_SLACK_MS, now))
    }
  }
  return next
}

function futureOrNull(at: number, now: number): number | null {
  return Number.isFinite(at) && at > now ? at : null
}

function earlierOf(a: number | null, b: number | null): number | null {
  if (a === null) return b
  if (b === null) return a
  return a < b ? a : b
}
