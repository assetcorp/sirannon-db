import type { SQLiteConnection } from '../driver/types.js'
import type { BackupDestination } from './destination.js'
import type { BackupRunReport, BackupToDestinationOptions } from './report.js'

/** How long the cycle waits between captures when nobody sets an interval.
 * @internal
 */
export const DEFAULT_CAPTURE_INTERVAL_MS = 60_000

/** How long a chain runs before a fresh full copy starts a new one, when nobody sets a limit.
 * @internal
 */
export const DEFAULT_FULL_COPY_INTERVAL_MS = 24 * 60 * 60 * 1000

/** What the backups are named after at the destination, when nobody sets a prefix.
 * @internal
 */
export const DEFAULT_BACKUP_NAME_PREFIX = 'sirannon-backup'

/**
 * How Sirannon runs the cycle that captures a database's change log and then
 * checkpoints it.
 *
 * Setting this takes checkpointing away from SQLite. It has to: a checkpoint
 * lets SQLite overwrite log frames nothing has captured yet, and it says
 * nothing when it does.
 *
 * @public
 */
export interface BackupCycleOptions {
  /** Where the full copy, the change pieces, and the chain records go. */
  destination: BackupDestination
  /**
   * How long to wait between captures, in milliseconds. Defaults to 60000. The
   * shorter this is, the smaller the log grows and the fewer writes an unclean
   * stop leaves uncaptured. At zero the cycle runs only when you ask it to.
   */
  intervalMs?: number
  /**
   * How long a chain runs before a fresh full copy starts a new one, in
   * milliseconds. Defaults to 24 hours. Restoring means replaying every piece
   * since the full copy, so this is what bounds how long that takes.
   */
  fullCopyIntervalMs?: number
  /** Name to store the list of chains under. Defaults to `sirannon-backup-chain`. */
  chainName?: string
  /** What to name the backups after at the destination. Defaults to `sirannon-backup`. */
  namePrefix?: string
  /** Size of one whole piece, in bytes. Defaults to 16 MiB. */
  pieceBytes?: number
  /** Whether to fingerprint each backup. Defaults to true. */
  fingerprint?: boolean
  /**
   * Where to stage a capture before it goes out, and where the full copy writes
   * its own local file. Defaults to a directory beside the database file, so a
   * capture that has yet to go out is still there after a restart.
   */
  stagingDir?: string
  /** How many pages SQLite moves in one step of the full copy. */
  pagesPerStep?: number
  /** How many restarts the full copy tolerates before it gives up. */
  restartLimit?: number
  /** How long the full copy may move no pages at all, in milliseconds, before it gives up. */
  stallTimeoutMs?: number
  /** How many steps the full copy may take without reaching a page it had not already copied. */
  noProgressStepLimit?: number
  /** Called with the report of every backup the cycle finishes. */
  onRun?: (report: BackupRunReport) => void
  /**
   * Called when a capture, a transfer, or a checkpoint fails. Set this one. A
   * cycle that stops running while writes carry on lets the log grow without
   * bound, and without a callback here nothing reports that.
   */
  onError?: (error: Error) => void
}

/** What the cycle needs beyond the operator's own options.
 * @internal
 */
export interface BackupCycleRequest extends BackupCycleOptions {
  /** Identifier of the database to capture from. */
  databaseId: string
  /** Path of its file. */
  sourcePath: string
  /** Runs an operation with nothing else holding the writer. */
  runExclusive: (op: () => Promise<void>) => Promise<void>
  /** Hands back the connection that writes. No other connection may checkpoint. */
  acquireWriter: () => SQLiteConnection
  /** Copies the whole database to the destination. Every chain starts with one of these. */
  fullCopy: (options: BackupToDestinationOptions) => Promise<BackupRunReport>
}
