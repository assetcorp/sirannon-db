import type { SQLiteConnection } from '../driver/types.js'

const CHECKPOINT_ATTEMPTS = 3
const CHECKPOINT_RETRY_DELAY_MS = 50

interface CheckpointRow {
  busy: number | bigint
  log: number | bigint
  checkpointed: number | bigint
}

/** What one checkpoint moved out of the write-ahead log.
 * @internal
 */
export interface CheckpointResult {
  /** Whether a reader or a writer stopped it part-way. */
  busy: boolean
  /** How many frames the log still holds. */
  framesInLog: number
  /** How many frames went back into the database file. */
  framesCheckpointed: number
  /** Whether the log is now empty. The next write starts a fresh one. */
  emptied: boolean
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Folds the write-ahead log back into the database file and truncates it.
 *
 * A reader holding pages can stop the fold part-way. That is not a failure:
 * the frames stay where they are and the next capture picks up from there, so
 * this tries a few times and then reports what happened.
 *
 * The statement has to run on the connection that writes. SQLite sends any copy
 * in progress back to page one when a truncating checkpoint arrives on some
 * other connection.
 *
 * @param conn - The connection that writes.
 * @returns What moved, and whether the log is now empty.
 */
export async function checkpointLog(conn: SQLiteConnection): Promise<CheckpointResult> {
  let last: CheckpointResult = { busy: true, framesInLog: 0, framesCheckpointed: 0, emptied: false }

  for (let attempt = 0; attempt < CHECKPOINT_ATTEMPTS; attempt++) {
    if (attempt > 0) await delay(attempt * CHECKPOINT_RETRY_DELAY_MS)
    const stmt = await conn.prepare('PRAGMA wal_checkpoint(TRUNCATE)')
    const row = await stmt.get<CheckpointRow>()
    if (!row) return last
    const busy = Number(row.busy) !== 0
    const framesInLog = Number(row.log)
    last = {
      busy,
      framesInLog,
      framesCheckpointed: Number(row.checkpointed),
      emptied: !busy && framesInLog === 0,
    }
    if (last.emptied) return last
  }

  return last
}
