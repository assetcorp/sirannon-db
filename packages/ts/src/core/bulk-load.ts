import { synchronousPragmaValue } from './driver/synchronous.js'
import type { SQLiteConnection, SynchronousLevel } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BulkLoadDurability, BulkLoadResult } from './types.js'

const DEFAULT_LOAD_DURABILITY: BulkLoadDurability = 'off'
const CHECKPOINT_ATTEMPTS = 2

export function isBulkLoadDurability(value: unknown): value is BulkLoadDurability {
  return value === 'off' || value === 'normal'
}

interface WalCheckpointRow {
  busy: number
}

export interface BulkLoadRun {
  writer: SQLiteConnection
  configuredSynchronous: SynchronousLevel
  walMode: boolean
  durability: BulkLoadDurability | undefined
  loadRows: () => Promise<BulkLoadResult>
}

/**
 * Run a bulk load with relaxed writer durability, then restore the
 * operator-configured level. The caller must hold the database's writer lock
 * for the whole call, so no other write commits under the relaxed level.
 *
 * Interruption safety, by failure mode:
 * - A load statement fails: the surrounding transaction rolls back, the
 *   configured level is restored, and the database is exactly as it was before
 *   the load; the operator recovers by re-running the load.
 * - The process crashes mid-load: `PRAGMA synchronous` is connection state
 *   that SQLite never stores in the database file, so the next open re-applies
 *   the configured level, and the uncommitted transaction rolls back from the
 *   WAL on open.
 * - Power loss or an OS crash during a load at 'off': SQLite documents that
 *   the file may be corrupted, which is why 'off' fits only a load the
 *   operator can re-run from scratch; 'normal' keeps WAL corruption safety.
 *
 * The restore runs on both the success and the failure path. A restore pragma
 * only fails when the writer connection itself is gone, so on a committed load
 * that failure is reported as 'DURABILITY_RESTORE_FAILED', which tells the
 * operator the load committed and must not be re-run; on a failed load the
 * original load error stays dominant.
 *
 * On success the WAL is checkpointed after the restore, so the loaded rows are
 * fsync'd into the main database file at the restored level before this
 * resolves. The checkpoint runs synchronously in the engine and blocks the
 * event loop for the duration of the WAL flush, which grows with the size of
 * the load.
 */
export async function runBulkLoad(run: BulkLoadRun): Promise<BulkLoadResult> {
  const durability = run.durability ?? DEFAULT_LOAD_DURABILITY
  if (!isBulkLoadDurability(durability)) {
    throw new SirannonError("Bulk load durability must be 'off' or 'normal'", 'INVALID_DURABILITY')
  }

  const restorePragma = `PRAGMA synchronous = ${synchronousPragmaValue(run.configuredSynchronous)}`
  await run.writer.exec(`PRAGMA synchronous = ${synchronousPragmaValue(durability)}`)

  let result: BulkLoadResult | undefined
  let loadError: unknown
  try {
    result = await run.loadRows()
  } catch (err) {
    loadError = err
  }

  try {
    await run.writer.exec(restorePragma)
  } catch {
    if (loadError !== undefined) throw loadError
    throw new SirannonError(
      'Bulk load committed but durability could not be restored because the writer connection failed',
      'DURABILITY_RESTORE_FAILED',
    )
  }

  if (loadError !== undefined || result === undefined) {
    throw loadError
  }

  if (run.walMode) {
    await checkpoint(run.writer)
  }
  return result
}

/**
 * Flush the WAL into the main database file and fsync it at the restored
 * durability. `wal_checkpoint(TRUNCATE)` reports `busy` rather than throwing
 * when a reader holds the WAL; the load has already committed, so a busy
 * result is not fatal. A single retry clears a transient reader, and a
 * persistent reader defers the final flush to the next checkpoint rather than
 * failing a committed load.
 */
async function checkpoint(writer: SQLiteConnection): Promise<void> {
  for (let attempt = 0; attempt < CHECKPOINT_ATTEMPTS; attempt++) {
    const stmt = await writer.prepare('PRAGMA wal_checkpoint(TRUNCATE)')
    const row = await stmt.get<WalCheckpointRow>()
    if (!row || row.busy === 0) return
  }
}
