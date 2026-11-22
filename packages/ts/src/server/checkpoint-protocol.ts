/**
 * The body that `POST /db/{id}/checkpoint` returns once the checkpoint ends.
 *
 * @public
 */
export interface CheckpointResponse {
  /** Whether a reader, or another connection that writes, kept SQLite from finishing the checkpoint. */
  busy: boolean
  /** The number of frames in the write-ahead log, or zero when another connection holds the checkpoint lock and SQLite reports no count. */
  framesInLog: number
  /**
   * The number of frames that SQLite copied back into the database file when the log stays in place. SQLite reports
   * zero here and in `framesInLog` once it empties the log. When another connection holds the checkpoint lock, SQLite
   * reports no counts, so Sirannon sets both counts to zero and `busy` to true.
   */
  framesCheckpointed: number
  /** Whether the log is empty after the checkpoint, in which case SQLite starts a new log at the next write. */
  emptied: boolean
}
