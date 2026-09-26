import type { SQLiteConnection } from '../driver/types.js'

const CHECKPOINT_ATTEMPTS = 3
const CHECKPOINT_RETRY_DELAY_MS = 50

interface CheckpointRow {
  busy: number | bigint
  log: number | bigint
  checkpointed: number | bigint
}

/** The frames that one checkpoint moves out of the write-ahead log, and the state of the log afterwards.
 * @internal
 */
export interface CheckpointResult {
  /** Whether another reader or writer kept SQLite from finishing the checkpoint. */
  busy: boolean
  /** The number of frames that remain in the log. */
  framesInLog: number
  /** The number of frames that SQLite copies back into the database file. */
  framesCheckpointed: number
  /** Whether the log is empty after the checkpoint, in which case SQLite starts a new log at the next write. */
  emptied: boolean
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * Copies the write-ahead log back into the database file and truncates the log.
 *
 * A reader that holds part of the log can stop the checkpoint before it
 * finishes. The frames then stay in the log for the next capture to read, so
 * this function tries up to three times and returns the result of the last try.
 *
 * The statement must run on the writer connection, because SQLite restarts any
 * copy in progress from page one when another connection runs a truncating
 * checkpoint.
 *
 * @param conn - The writer connection.
 * @returns The frames that the checkpoint moves, and whether the log is empty afterwards.
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
