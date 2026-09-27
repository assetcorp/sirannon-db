import { synchronousPragmaValue } from './driver/synchronous.js'
import type { SQLiteConnection, SynchronousLevel } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BulkLoadDurability, BulkLoadResult } from './types.js'

const DEFAULT_LOAD_DURABILITY: BulkLoadDurability = 'off'
const CHECKPOINT_ATTEMPTS = 3
const CHECKPOINT_RETRY_DELAY_MS = 50

/**
 * Returns true when the value is `'off'` or `'normal'`, the two durability levels that a bulk load accepts.
 *
 * @internal
 */
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
  checkpoint?: boolean
  capturesChangeLog?: boolean
  loadRows: () => Promise<BulkLoadResult>
}

/**
 * Executes a bulk load at a relaxed synchronous level, then restores the level
 * that the operator configured. The caller must hold the writer lock for the
 * whole call so that no other write commits at the relaxed level.
 *
 * Each failure leaves the database in a known state:
 * - When a load statement fails, SQLite rolls the transaction back and this
 *   function restores the configured level, so the operator can re-run the load.
 * - When the process crashes during a load, SQLite rolls the uncommitted
 *   transaction back from the WAL on the next open, and that open applies the
 *   configured level again, because SQLite keeps `PRAGMA synchronous` on the
 *   connection and never stores it in the file.
 * - When the power fails or the operating system crashes during a load at
 *   'off', SQLite documents that the file can be corrupted, so 'off' suits only
 *   a load that the operator can re-run from scratch, while 'normal' keeps the
 *   WAL safe from corruption.
 *
 * This function restores the level on the success path and on the failure
 * path. When the restore fails after a committed load, it throws
 * 'DURABILITY_RESTORE_FAILED' so that the operator knows that the load
 * committed and must not be re-run; when the load itself failed, it rethrows
 * the load error.
 *
 * After a load that committed and changed rows, this function checkpoints the
 * WAL at the restored level, which copies the loaded pages into the main
 * database file and fsyncs them. While a reader holds the WAL open, SQLite can
 * leave pages uncopied, so this function tries the checkpoint up to three times
 * and then returns without failing the committed load, leaving the remaining
 * pages for a later checkpoint. On a driver that calls SQLite on the main
 * thread, the checkpoint blocks the event loop for as long as the flush takes,
 * which grows with the size of the load.
 *
 * A caller that imports in several batches should pass `checkpoint: false` on
 * every load except the last, so that it pays for one fsyncing checkpoint at
 * the end. Each of those loads still restores the configured level, while
 * SQLite's automatic checkpoint keeps the WAL bounded at the relaxed level
 * without an fsync.
 *
 * On a database that captures its own change log, this function skips the
 * checkpoint whatever the caller asked for, because a checkpoint before the
 * capture would let SQLite overwrite the frames that this load wrote. The
 * backup cycle checkpoints once it has captured those frames.
 *
 * @internal
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

  if (loadError !== undefined) {
    throw loadError
  }
  if (result === undefined) {
    throw new SirannonError('Bulk load completed without a result', 'INTERNAL_ERROR')
  }

  if ((run.checkpoint ?? true) && run.walMode && !run.capturesChangeLog && result.changes > 0) {
    await checkpoint(run.writer)
  }
  return result
}

async function checkpoint(writer: SQLiteConnection): Promise<void> {
  for (let attempt = 0; attempt < CHECKPOINT_ATTEMPTS; attempt++) {
    if (attempt > 0) {
      await delay(attempt * CHECKPOINT_RETRY_DELAY_MS)
    }
    const stmt = await writer.prepare('PRAGMA wal_checkpoint(TRUNCATE)')
    const row = await stmt.get<WalCheckpointRow>()
    if (!row || row.busy === 0) return
  }
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
