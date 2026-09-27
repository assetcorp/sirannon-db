import type { SQLiteConnection } from '../driver/types.js'
import type { BackupDestination } from './destination.js'
import type { BackupGroupSource, BackupNodePreference, BackupSkip } from './preferred-node.js'
import type { BackupProgress, BackupRunReport, BackupToDestinationOptions } from './report.js'

/** The interval between captures, in milliseconds, when the operator sets none.
 * @internal
 */
export const DEFAULT_CAPTURE_INTERVAL_MS = 60_000

/** The age of a chain, in milliseconds, at which a new full copy starts a new chain, when the operator sets none.
 * @internal
 */
export const DEFAULT_FULL_COPY_INTERVAL_MS = 24 * 60 * 60 * 1000

/** The prefix of backup names at the destination, when the operator sets none.
 * @internal
 */
export const DEFAULT_BACKUP_NAME_PREFIX = 'sirannon-backup'

/**
 * Returns the directory that holds staged captures when the operator names
 * none. The directory is next to the database file, so that a capture that
 * Sirannon has yet to send stays on disk across a restart.
 *
 * @param sourcePath - The path of the database file.
 * @returns The path of that directory.
 *
 * @internal
 */
export function defaultStagingDir(sourcePath: string): string {
  return `${sourcePath}-backup`
}

/**
 * The settings for the cycle that captures the write-ahead log of a database
 * and then checkpoints it.
 *
 * When a database has these options, Sirannon turns off the automatic
 * checkpoints of SQLite, because a checkpoint lets SQLite overwrite log frames
 * that Sirannon has yet to capture, and SQLite reports success either way.
 *
 * @public
 */
export interface BackupCycleOptions {
  /** The destination that stores the full copy, the change pieces, and the chain records. */
  destination: BackupDestination
  /**
   * The interval between captures, in milliseconds. Defaults to 60000. A
   * shorter interval keeps the log smaller and leaves fewer writes outside any
   * backup after an unclean stop. At zero, the cycle takes a turn only when you
   * call `runOnce`.
   */
  intervalMs?: number
  /**
   * The age of a chain, in milliseconds, at which a new full copy starts a new
   * chain. Defaults to 24 hours. A restore applies every change piece since the
   * full copy, so this interval sets the upper limit on the time that a restore
   * takes.
   */
  fullCopyIntervalMs?: number
  /** The name to store the list of chains under. Defaults to `sirannon-backup-chain`. */
  chainName?: string
  /** The prefix of backup names at the destination. Defaults to `sirannon-backup`. */
  namePrefix?: string
  /** The size of one whole piece, in bytes. Defaults to 16 MiB. */
  pieceBytes?: number
  /** Whether to compute the SHA-256 of each backup. Defaults to true. */
  fingerprint?: boolean
  /**
   * The directory that holds a capture before Sirannon sends it. Defaults to a
   * directory next to the database file, so that a capture that Sirannon has
   * yet to send stays on disk across a restart. When you set it, the staged
   * full copy also writes its local file here.
   */
  stagingDir?: string
  /** The number of pages that SQLite copies in one step of the full copy. */
  pagesPerStep?: number
  /** The number of restarts from page one that Sirannon allows before it fails the full copy. */
  restartLimit?: number
  /** The number of milliseconds that can pass without a completed step before Sirannon fails the full copy. */
  stallTimeoutMs?: number
  /** The number of milliseconds that one call to the destination can take before Sirannon fails it. Defaults to 10 minutes, and zero removes the deadline. */
  destinationTimeoutMs?: number
  /**
   * The largest size in bytes that the write-ahead log can reach while the
   * cycle captures nothing. By default there is no limit, which keeps the chain
   * whole and lets the log grow for as long as the captures stop.
   *
   * Set it where free disk space matters more than an unbroken chain. Sirannon
   * measures the log after any turn that captures nothing. Once the log passes
   * this size, Sirannon empties it and reports `BACKUP_CHAIN_BROKEN`, so no
   * backup holds the writes from that log, and the next turn that the node takes
   * starts a new chain with a full copy.
   */
  maxUncapturedLogBytes?: number
  /** The number of steps that can pass without SQLite copying a new page before Sirannon fails the full copy. */
  noProgressStepLimit?: number
  /**
   * The source of the identity of this node and the membership of its
   * replication group. Every node of a group has the same cycle, and each turn
   * reads this source before it copies anything. Leave it out on a single-node
   * deployment, where that one node takes every turn.
   */
  replicationGroup?: BackupGroupSource
  /**
   * The node of that group that takes its backups. Defaults to `'replica'`, so
   * that the primary keeps its capacity for writes. When the group has no
   * eligible replica, Sirannon picks the primary.
   */
  preferredNode?: BackupNodePreference
  /** Called with the report of every backup that the cycle finishes. */
  onRun?: (report: BackupRunReport) => void
  /**
   * Called after each step of the copy and after each stored piece, with the
   * counters of the turn in progress. The cycle also records the latest
   * counters in its status, so use this callback when you want every step, and
   * read the status when you want the current figure.
   */
  onProgress?: (progress: BackupProgress) => void
  /**
   * Called with every turn that the cycle skips, and the reason for the skip. A
   * node that takes none of the backups of its group reports a skip on every
   * turn.
   */
  onSkip?: (skip: BackupSkip) => void
  /**
   * Called when a capture, a transfer, or a checkpoint fails. Set this
   * callback, because when turns keep failing while writes continue, the log
   * grows without limit, and this callback reports each failure as it happens.
   */
  onError?: (error: Error) => void
}

/** The settings of the operator, plus the database details and callbacks that the cycle needs.
 * @internal
 */
export interface BackupCycleRequest extends BackupCycleOptions {
  /** The identifier of the database to capture from. */
  databaseId: string
  /** The path of its file. */
  sourcePath: string
  /** Runs an operation while no other operation holds the writer. */
  runExclusive: (op: () => Promise<void>) => Promise<void>
  /** Returns the writer connection, which is the only connection that may checkpoint. */
  acquireWriter: () => SQLiteConnection
  /** Copies the whole database to the destination as the full copy that starts each chain. */
  fullCopy: (options: BackupToDestinationOptions) => Promise<BackupRunReport>
}
