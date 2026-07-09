import { synchronousPragmaValue } from './driver/synchronous.js'
import type { SQLiteConnection, SynchronousLevel } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BulkLoadDurability, ExecuteResult } from './types.js'

const DEFAULT_LOAD_DURABILITY: BulkLoadDurability = 'off'

export function isBulkLoadDurability(value: unknown): value is BulkLoadDurability {
  return value === 'off' || value === 'normal'
}

export interface BulkLoadRun {
  writer: SQLiteConnection
  configuredSynchronous: SynchronousLevel
  walMode: boolean
  durability: BulkLoadDurability | undefined
  execute: () => Promise<ExecuteResult[]>
}

/**
 * Run a bulk load with relaxed writer durability, then restore the
 * operator-configured level.
 *
 * Interruption safety, by failure mode:
 * - Load statement fails: the surrounding transaction rolls back, the
 *   configured level is restored best-effort, and the database is exactly as
 *   it was before the load; the operator recovers by re-running the load.
 * - Process crash mid-load: `PRAGMA synchronous` is connection state, never
 *   stored in the database file, so the next open re-applies the configured
 *   level; the uncommitted transaction rolls back via the WAL on open.
 * - Power loss or OS crash during a load at 'off': SQLite documents the file
 *   may be corrupted, which is why 'off' is sanctioned only for loads that
 *   can be re-run from scratch; 'normal' keeps WAL corruption safety.
 * - Restore failure on the error path is swallowed: the restore pragma can
 *   only fail when the connection itself is gone, and a dead connection
 *   cannot retain the relaxed level.
 *
 * On success the WAL is checkpointed (TRUNCATE) after the restore, so the
 * loaded rows are synced into the main database file before this resolves.
 * A checkpoint failure is propagated: the load is committed but not yet
 * power-loss durable, and the caller must not be told otherwise.
 */
export async function runBulkLoad(run: BulkLoadRun): Promise<ExecuteResult[]> {
  const durability = run.durability ?? DEFAULT_LOAD_DURABILITY
  if (!isBulkLoadDurability(durability)) {
    throw new SirannonError("Bulk load durability must be 'off' or 'normal'", 'INVALID_DURABILITY')
  }

  const restorePragma = `PRAGMA synchronous = ${synchronousPragmaValue(run.configuredSynchronous)}`
  await run.writer.exec(`PRAGMA synchronous = ${synchronousPragmaValue(durability)}`)

  let results: ExecuteResult[]
  try {
    results = await run.execute()
  } catch (err) {
    try {
      await run.writer.exec(restorePragma)
    } catch {
      /* connection is gone; the relaxed level died with it */
    }
    throw err
  }

  await run.writer.exec(restorePragma)
  if (run.walMode) {
    await run.writer.exec('PRAGMA wal_checkpoint(TRUNCATE)')
  }
  return results
}
