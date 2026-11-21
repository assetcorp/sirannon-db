import type { SQLiteConnection } from '../driver/types.js'
import { readBusyTimeout, setBusyTimeout } from '../system-catalog/index.js'

const CHECKPOINT_ATTEMPTS = 3
const CHECKPOINT_RETRY_DELAY_MS = 50
const NO_WRITE_AHEAD_LOG = -1
const LOG_ABSENT: CheckpointResult = { busy: false, framesInLog: 0, framesCheckpointed: 0, emptied: true }

interface CheckpointRow {
  busy: number | bigint
  log: number | bigint
  checkpointed: number | bigint
}

/**
 * The frames that one checkpoint moves out of the write-ahead log, and the state of the log afterwards.
 *
 * @public
 */
export interface CheckpointResult {
  /** Whether a reader, or another connection that writes, kept SQLite from finishing the checkpoint. */
  busy: boolean
  /** The number of frames in the log, or zero when another connection holds the checkpoint lock and SQLite reports no count. */
  framesInLog: number
  /**
   * The number of frames that SQLite copies back into the database file when the log stays in place. SQLite reports
   * zero here and in {@link CheckpointResult.framesInLog} once it empties the log. When another connection holds the
   * checkpoint lock, SQLite reports no counts, so Sirannon sets both counts to zero and {@link CheckpointResult.busy}
   * to true.
   */
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
 * A database without a write-ahead log has nothing to checkpoint, so this
 * function reports an empty log for it.
 * When another connection holds the checkpoint lock, SQLite reports no counts,
 * so Sirannon sets both counts to zero and reports busy after the last try.
 *
 * @param conn - The writer connection.
 * @returns The frames that the checkpoint moves, and whether the log is empty afterwards.
 * @internal
 */
export async function checkpointLog(conn: SQLiteConnection): Promise<CheckpointResult> {
  let last: CheckpointResult = { busy: true, framesInLog: 0, framesCheckpointed: 0, emptied: false }

  for (let attempt = 0; attempt < CHECKPOINT_ATTEMPTS; attempt++) {
    if (attempt > 0) await delay(attempt * CHECKPOINT_RETRY_DELAY_MS)
    const stmt = await conn.prepare('PRAGMA wal_checkpoint(TRUNCATE)')
    const row = await stmt.get<CheckpointRow>()
    if (!row) return last
    const busy = Number(row.busy) !== 0
    if (!busy && Number(row.log) === NO_WRITE_AHEAD_LOG) return { ...LOG_ABSENT }
    const framesInLog = Math.max(0, Number(row.log))
    last = {
      busy,
      framesInLog,
      framesCheckpointed: Math.max(0, Number(row.checkpointed)),
      emptied: !busy && framesInLog === 0,
    }
    if (last.emptied) return last
  }

  return last
}

/**
 * Checkpoints the log as {@link checkpointLog} does, except that each try reports busy at once when a reader holds the
 * log, so the writer lock stays held for the retry delays alone and never for the connection's busy timeout.
 *
 * Sirannon restores the connection's busy timeout afterwards, whether the checkpoint succeeds or throws.
 *
 * @param conn - The writer connection.
 * @returns The frames that the checkpoint moves, and whether the log is empty afterwards.
 * @internal
 */
export async function checkpointLogWithoutWaiting(conn: SQLiteConnection): Promise<CheckpointResult> {
  const busyTimeout = await readBusyTimeout(conn)
  try {
    await setBusyTimeout(conn, 0)
    return await checkpointLog(conn)
  } finally {
    await setBusyTimeout(conn, busyTimeout)
  }
}
