import type { SQLiteDriver } from '../driver/types.js'
import type { BackupDestination } from './destination.js'

/** How many change pieces one batch replays when nobody sets a number.
 * @internal
 */
export const DEFAULT_RESTORE_BATCH_SIZE = 16

/** How far one restore has got, reported after every piece it fetches.
 * @public
 */
export interface BackupRestoreProgress {
  /** Whether the restore is fetching the full copy or replaying the change pieces on top of it. */
  phase: 'full-copy' | 'changes'
  /** Pieces the restore has fetched. */
  piecesFetched: number
  /** Bytes it has fetched. */
  bytesFetched: number
  /** Change pieces it has replayed. */
  changesApplied: number
  /** Change pieces the plan holds altogether. */
  changesTotal: number
}

/**
 * How Sirannon rebuilds a database from a moment you name.
 *
 * @public
 */
export interface BackupRestoreOptions {
  /** Where the backups and their records are stored. */
  destination: BackupDestination
  /** Driver the restore opens the rebuilt database through, so it can fold each batch of changes in. */
  driver: SQLiteDriver
  /** Path Sirannon writes the rebuilt database to. A file already there stops the restore unless you set {@link BackupRestoreOptions.replaceExisting}. */
  destPath: string
  /**
   * Whether to replace a database already at that path. It defaults to false,
   * because a restore removes the write-ahead log beside the path it writes to
   * and any commit that log still held would go with it. Set this where you
   * mean to restore over a database you no longer want.
   */
  replaceExisting?: boolean
  /** Epoch milliseconds you want back. Defaults to now, which reaches the newest backup the destination holds. */
  moment?: number
  /** Name the list of chains is stored under. Defaults to `sirannon-backup-chain`. */
  chainName?: string
  /**
   * How many change pieces to replay between one checkpoint and the next.
   * Defaults to 16. This is what bounds the log the restore writes beside the
   * database, so lower it where disk is tight and raise it where a long chain
   * takes too many checkpoints.
   */
  batchSize?: number
  /** Milliseconds one call to the destination may take before the restore stops with an error. Defaults to 10 minutes, and zero leaves the calls unbounded. */
  destinationTimeoutMs?: number
  /** Called after every piece the restore fetches. */
  onProgress?: (progress: BackupRestoreProgress) => void
}

/** What one finished restore produced.
 * @public
 */
export interface BackupRestoreReport {
  /** The chain the restore read. */
  chainId: string
  /** Path of the rebuilt database. */
  destPath: string
  /** Name the full copy underneath it is stored under. */
  baseName: string
  /** Epoch milliseconds the rebuilt database reflects, which is when the last piece replayed was captured. */
  restoresTo: number
  /** Pieces the restore fetched, counting the full copy and every change piece. */
  pieceCount: number
  /** Bytes it fetched. */
  bytesFetched: number
  /** Change pieces it replayed. */
  changesApplied: number
  /** Log frames those pieces held. */
  framesApplied: number
  /** Batches it replayed them in, each one folded into the database by a checkpoint of its own. */
  batchCount: number
  /** Epoch milliseconds the restore started. */
  startedAt: number
  /** Epoch milliseconds it finished. */
  finishedAt: number
  /** Milliseconds it took. */
  durationMs: number
}
