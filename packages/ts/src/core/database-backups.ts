import type { BackupCapabilities } from './backup/capabilities.js'
import type { BackupChain, BackupChainRecord } from './backup/chain.js'
import type { BackupRestorePlan, BackupSafeToDeleteOptions } from './backup/chain-queries.js'
import type { CheckpointResult } from './backup/checkpoint.js'
import type { BackupCycleStatus } from './backup/cycle-status.js'
import type { BackupFileReport, BackupRunReport, BackupToDestinationOptions } from './backup/report.js'
import type { BackupVerifyResult } from './backup/verify.js'
import type { BackupChainLocation } from './database-backup.js'
import { DatabaseLifecycle } from './database-lifecycle.js'
import { SirannonError } from './errors.js'
import type { BackupScheduleOptions } from './types.js'

/**
 * Gives an open database its backup methods: one-off copies, a cron schedule,
 * the cycle that captures the change log, and queries over the chain of
 * backups that the cycle writes.
 *
 * Open a database through {@link Sirannon.open}, which returns a
 * {@link Database} that extends this class.
 *
 * @public
 */
export class DatabaseBackups extends DatabaseLifecycle {
  /**
   * Copies this database to a file while it stays open for reads and writes.
   *
   * Sirannon holds the writer only until SQLite copies the first step of pages,
   * so later writes commit between the steps of the copy and wait for none of it.
   *
   * @param destPath - The path of the file that Sirannon writes the copy to.
   * @returns The run identifier, the timings, the number of pages copied, and the size of the file in bytes.
   */
  async backup(destPath: string): Promise<BackupFileReport> {
    this.ensureOpen()
    this.ensureWriterCanWait()
    return this.runtime.backups.backup(destPath)
  }

  /**
   * Copies this database to a destination that you supply, in fixed-size
   * pieces, while it stays open for reads and writes.
   *
   * Sirannon includes no storage client, so you connect object storage, or any
   * other store, through the destination.
   *
   * Where this runtime has the compiled streaming extension, Sirannon sends each
   * piece to the destination as SQLite writes it, so the copy needs no local
   * disk. On every other runtime, Sirannon writes one local file first, which
   * needs local disk equal to the size of the backup.
   * {@link DatabaseBackups.backupCapabilities} reports which of the two routes
   * applies to this runtime.
   *
   * @param options - The destination, the naming, the piece size, and the progress callback.
   * @returns The run identifier, the timings, what the copy wrote, and how many times the copy restarted.
   */
  async backupTo(options: BackupToDestinationOptions): Promise<BackupRunReport> {
    this.ensureOpen()
    this.ensureWriterCanWait()
    return this.runtime.backups.backupTo(options)
  }

  /**
   * Reports which backup operations this database's runtime supports, so that
   * you can check them before you start a backup.
   *
   * @returns Whether this runtime can copy the database, whether a copy needs local disk, and whether it can schedule copies.
   */
  backupCapabilities(): BackupCapabilities {
    return this.runtime.backups.capabilities()
  }

  /**
   * Starts copying this database to a directory on a cron schedule, and deletes
   * the oldest copies beyond {@link BackupScheduleOptions.maxFiles}.
   *
   * @param options - The cron expression, destination directory, retention, and time zone, with the callbacks that Sirannon calls after each finished copy and after each failure.
   */
  scheduleBackup(options: BackupScheduleOptions): void {
    this.ensureOpen()
    this.runtime.backups.schedule(options)
  }

  /**
   * Takes one turn of the backup cycle now, without waiting for the interval. A
   * turn sends any capture that is still waiting, captures the log frames
   * written since the previous turn, and then checkpoints the log.
   *
   * Only a database opened with the `backups` option has a cycle, and on any
   * other database this throws `BACKUP_UNSUPPORTED`. On a node whose replication
   * group takes its backups from another node, Sirannon writes nothing on the
   * turn and reports a skip through `onSkip`.
   *
   * @returns What Sirannon wrote on the turn, or undefined when it wrote nothing.
   */
  async captureBackupChanges(): Promise<BackupRunReport | undefined> {
    this.ensureOpen()
    this.ensureWriterCanWait()
    return this.runtime.backups.captureChanges()
  }

  /**
   * Copies the write-ahead log back into the database file and reports whether
   * the checkpoint finished.
   *
   * Sirannon runs a `TRUNCATE` checkpoint on the writer connection under the
   * writer lock, so the checkpoint waits for the writes in progress, and no
   * other write starts until it ends. A reader that holds part of the log can
   * stop the checkpoint before it finishes, so Sirannon tries up to three times
   * and reports the last try. Each try reports busy at once when a reader holds
   * the log, so a held reader keeps the writer for the 150 milliseconds between
   * tries and no longer. A database without a write-ahead log reports an empty
   * log.
   *
   * On a database opened with the `backups` option, the backup cycle runs every
   * checkpoint, because it has to capture the log first. This method then takes
   * one turn of the cycle, as {@link DatabaseBackups.captureBackupChanges} does,
   * and reports the checkpoint of that turn. When that turn starts a new chain
   * with a full copy, which checkpoints nothing, Sirannon takes a second turn.
   * Either turn can take as long as a full copy of the database.
   *
   * Call this in place of `PRAGMA wal_checkpoint`, which the query and execute
   * methods refuse with `FORBIDDEN_SQL`.
   *
   * This method throws `READ_ONLY` on a read-only database, and
   * `CHECKPOINT_IN_TRANSACTION` inside a transaction callback. On a database with
   * backups, it throws the error that stops the turn, and `BACKUP_ERROR` when
   * the cycle skips the turn and runs no checkpoint.
   *
   * @returns Whether a reader stopped the checkpoint, the frames left in the log, the frames that SQLite copied into the database file, and whether the log is empty afterwards.
   */
  async checkpoint(): Promise<CheckpointResult> {
    this.ensureWritable()
    this.ensureWriterCanWait()
    if (this.runtime.writerLock.isHeld()) {
      throw new SirannonError(
        'SQLite checkpoints the log only outside a transaction, so a call inside a transaction callback fails; call checkpoint after the transaction ends',
        'CHECKPOINT_IN_TRANSACTION',
      )
    }
    return this.runtime.backups.checkpoint()
  }

  /**
   * Lists the backup chains at the destination.
   *
   * @returns One entry per chain, newest first, each with its full copy and its change pieces in order.
   */
  async backupChain(): Promise<BackupChain[]> {
    this.ensureNotClosed()
    return this.runtime.backups.chains()
  }

  /**
   * Returns what this database's backup cycle is doing now, and the outcome of
   * its recent turns.
   *
   * You can poll this while a turn that you started with
   * {@link DatabaseBackups.captureBackupChanges} is still under way, and during
   * a full copy of a large database, the status includes the pages that SQLite
   * has copied so far.
   *
   * @returns Whether a turn is under way, how far it has got, and the last run, skip, and failure.
   */
  backupStatus(): BackupCycleStatus {
    this.ensureNotClosed()
    return this.runtime.backups.status()
  }

  /**
   * Fetches one of this database's backups from the destination and checks it
   * against the record that Sirannon wrote with that backup.
   *
   * A restore fails on a damaged piece only after the restore has started, so
   * call this beforehand to check the backup. Sirannon fetches every piece in
   * order, computes a SHA-256 over the bytes as they arrive, and compares that
   * digest and the byte count with the record. Sirannon keeps one piece in
   * memory at a time, so checking a large full copy needs no local disk.
   *
   * This method throws `BACKUP_DESTINATION_ERROR` when a piece is missing, or
   * when the byte count or the digest differs from the record.
   *
   * @param name - The name that the backup is stored under, as each entry from {@link DatabaseBackups.backupChain} gives it.
   * @returns The number of pieces read, their total size in bytes, and the digest when the backup recorded one.
   */
  async verifyBackup(name: string): Promise<BackupVerifyResult> {
    this.ensureNotClosed()
    return this.runtime.backups.verify(name)
  }

  /**
   * Returns where this database's backups are stored.
   *
   * Pass the destination, chain name, and deadline from the result to
   * `restoreBackup` to rebuild this database from its backups at a path that
   * you name.
   *
   * @returns The destination, the name that the chains are listed under, and the directory where Sirannon stages captures.
   */
  backupLocation(): BackupChainLocation {
    this.ensureNotClosed()
    return this.runtime.backups.location()
  }

  /**
   * Returns the full copy and the change pieces that a restore to a given
   * moment would apply.
   *
   * The plan also includes the moment that the restored database would reflect,
   * which is the capture time of its last piece. Each piece contains every write
   * from one capture interval, so a restore stops at a piece boundary, which
   * can fall before the millisecond that you name.
   *
   * @param moment - The moment to restore to, in epoch milliseconds.
   * @returns The full copy, the change pieces to apply in order, and the moment that the result reflects.
   */
  async backupRestorePlan(moment: number): Promise<BackupRestorePlan> {
    this.ensureNotClosed()
    return this.runtime.backups.restorePlan(moment)
  }

  /**
   * Returns the backup records that no restore needs any longer, so that you
   * can delete them from the destination yourself.
   *
   * When you pass the earliest moment that a restore must still reach, the list
   * also includes every chain whose full copy finished before the full copy
   * that a restore to that moment would start from.
   *
   * @param options - The earliest moment that a restore must still reach.
   * @returns The records that you can delete, oldest chain first.
   */
  async backupPiecesSafeToDelete(options?: BackupSafeToDeleteOptions): Promise<BackupChainRecord[]> {
    this.ensureNotClosed()
    return this.runtime.backups.piecesSafeToDelete(options)
  }
}
