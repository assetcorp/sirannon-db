/**
 * The body that `POST /db/{id}/checkpoint` returns once the checkpoint ends.
 *
 * @public
 */
export interface CheckpointResponse {
  /** Whether a reader, or another connection that writes, kept SQLite from finishing the checkpoint. */
  busy: boolean
  /** The number of frames that remain in the write-ahead log. */
  framesInLog: number
  /**
   * The number of frames that SQLite copied back into the database file when the log stays in place. SQLite reports
   * zero here and in `framesInLog` once it empties the log.
   */
  framesCheckpointed: number
  /** Whether the log is empty after the checkpoint, in which case SQLite starts a new log at the next write. */
  emptied: boolean
}
