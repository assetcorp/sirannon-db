import type { SQLiteDriver } from '../driver/types.js'
import type { BackupDestination } from './destination.js'

/** The number of change pieces in one batch when the caller sets none.
 * @internal
 */
export const DEFAULT_RESTORE_BATCH_SIZE = 16

/**
 * The largest number of change pieces in one batch.
 *
 * The batch size sets the upper limit on the log that a restore writes next to
 * the database, so without a maximum a whole chain could go into one log, and
 * the working space would then grow with the size of the database. At the
 * default capture interval of one minute, this number covers close to three
 * days of change pieces, which is longer than the one day that a chain lasts
 * before a new full copy replaces it.
 *
 * @internal
 */
export const MAX_RESTORE_BATCH_SIZE = 4096

/** The progress of one restore, which Sirannon reports after every piece that it fetches and after every batch.
 * @public
 */
export interface BackupRestoreProgress {
  /** Whether Sirannon is fetching the full copy or applying the change pieces on top of it. */
  phase: 'full-copy' | 'changes'
  /** The number of pieces fetched so far. */
  piecesFetched: number
  /** The number of bytes fetched so far. */
  bytesFetched: number
  /** The number of change pieces applied so far. */
  changesApplied: number
  /** The total number of change pieces in the plan. */
  changesTotal: number
}

/**
 * The settings for rebuilding a database at a moment that you name.
 *
 * @public
 */
export interface BackupRestoreOptions {
  /** The destination that holds the backups and their records. */
  destination: BackupDestination
  /** The driver that Sirannon opens the rebuilt database through, so that it can checkpoint each batch of changes into the file. */
  driver: SQLiteDriver
  /** The path that Sirannon writes the rebuilt database to. A file at that path stops the restore unless you set {@link BackupRestoreOptions.replaceExisting}. */
  destPath: string
  /**
   * Whether to replace a database that already exists at that path. Defaults to
   * false, because a restore can delete the write-ahead log next to that path,
   * and with it any commit that the log still holds. Set this when you intend
   * to restore over a database that you no longer need.
   */
  replaceExisting?: boolean
  /** The moment to restore to, in epoch milliseconds. Defaults to now, which restores the newest backup at the destination. */
  moment?: number
  /** The name that Sirannon stores the list of chains under. Defaults to `sirannon-backup-chain`. */
  chainName?: string
  /**
   * The number of change pieces to apply between one checkpoint and the next.
   * Defaults to 16, with a maximum of 4096. This number sets the upper limit on
   * the log that the restore writes next to the database, so lower it where
   * disk space is short, and raise it where a long chain needs too many
   * checkpoints.
   */
  batchSize?: number
  /** The number of milliseconds that one call to the destination can take before Sirannon fails the restore. Defaults to 10 minutes, and zero removes the deadline. */
  destinationTimeoutMs?: number
  /** Called after every piece that Sirannon fetches, and after every batch. */
  onProgress?: (progress: BackupRestoreProgress) => void
}

/** The report of one finished restore.
 * @public
 */
export interface BackupRestoreReport {
  /** The chain that Sirannon restores from. */
  chainId: string
  /** The path of the rebuilt database. */
  destPath: string
  /** The name that Sirannon stores the full copy of the chain under. */
  baseName: string
  /** The moment, in epoch milliseconds, that the rebuilt database reflects, which is the capture time of the last change piece applied, or the finish time of the full copy where Sirannon applies none. */
  restoresTo: number
  /** The number of stored pieces that Sirannon fetches, for the full copy and every change piece. */
  pieceCount: number
  /** The number of bytes that Sirannon fetches. */
  bytesFetched: number
  /** The number of change pieces that Sirannon applies. */
  changesApplied: number
  /** The number of log frames in those pieces. */
  framesApplied: number
  /** The number of batches that Sirannon applies the change pieces in, each with a checkpoint of its own. */
  batchCount: number
  /** The moment, in epoch milliseconds, that the restore started. */
  startedAt: number
  /** The moment, in epoch milliseconds, that the restore finished. */
  finishedAt: number
  /** The length of the restore, in milliseconds. */
  durationMs: number
}
