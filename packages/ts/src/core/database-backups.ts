import type { BackupCapabilities } from './backup/capabilities.js'
import type { BackupChain, BackupChainRecord } from './backup/chain.js'
import type { BackupRestorePlan, BackupSafeToDeleteOptions } from './backup/chain-queries.js'
import type { BackupRunReport, BackupToDestinationOptions } from './backup/report.js'
import { DatabaseLifecycle } from './database-lifecycle.js'
import type { BackupScheduleOptions } from './types.js'

/**
 * Everything an open database does about its backups: one-off copies, the cron
 * schedule, the cycle that captures its change log, and the questions its chain
 * of backups can answer.
 *
 * Open a database through {@link Sirannon.open}. What you get back is a
 * {@link Database}, which is built on this.
 *
 * @public
 */
export class DatabaseBackups extends DatabaseLifecycle {
  /**
   * Copies this database to a file while it stays open for reads and writes.
   *
   * SQLite moves the pages in steps on the connection that writes, so a write
   * runs in the gap between two steps rather than waiting for the whole copy.
   *
   * @param destPath - Path the copy is written to.
   */
  async backup(destPath: string): Promise<void> {
    this.ensureOpen()
    await this.runtime.backups.backup(destPath)
  }

  /**
   * Copies this database to a destination you supply, in fixed-size pieces,
   * while it stays open for reads and writes.
   *
   * Sirannon carries no storage client, so the destination is where you
   * connect object storage or anything else that moves bytes.
   *
   * The pieces reach the destination as SQLite writes them where this runtime
   * carries the compiled streaming extension, so the run needs no local disk.
   * Every other runtime writes one local file first and needs local disk equal
   * to the backup. {@link DatabaseBackups.backupCapabilities} states which of
   * the two this one does.
   *
   * @param options - Destination, naming, piece size, and progress reporting.
   * @returns The run identifier, the timings, what the run wrote, and how often the copy restarted.
   */
  async backupTo(options: BackupToDestinationOptions): Promise<BackupRunReport> {
    this.ensureOpen()
    return this.runtime.backups.backupTo(options)
  }

  /**
   * Reports which backup operations this database's runtime supports, so you
   * learn before a run rather than when one fails.
   *
   * @returns What this runtime copies, whether it needs local disk, and whether it schedules.
   */
  backupCapabilities(): BackupCapabilities {
    return this.runtime.backups.capabilities()
  }

  /**
   * Starts repeating backups on a cron schedule, keeping a bounded number of files.
   *
   * @param options - Cron expression, destination directory, retention, time zone, and failure callback.
   */
  scheduleBackup(options: BackupScheduleOptions): void {
    this.ensureOpen()
    this.runtime.backups.schedule(options)
  }

  /**
   * Runs one turn of the checkpoint cycle now instead of waiting for its
   * interval. A turn sends any capture still waiting, reads the log frames
   * written since the previous turn, and then checkpoints the log.
   *
   * A database needs the `backups` option to have a cycle at all. On a node its
   * replication group backs up from somewhere else, the turn writes nothing and
   * reports a skip through `onSkip`.
   *
   * @returns What the turn wrote, or undefined where it wrote nothing.
   */
  async captureBackupChanges(): Promise<BackupRunReport | undefined> {
    this.ensureOpen()
    return this.runtime.backups.captureChanges()
  }

  /**
   * Lists what the backup destination holds.
   *
   * @returns One entry per chain, newest first, each with its full copy and its change pieces in order.
   */
  async backupChain(): Promise<BackupChain[]> {
    this.ensureNotClosed()
    return this.runtime.backups.chains()
  }

  /**
   * Works out what a restore to a given moment has to read.
   *
   * The result also tells you the moment it would actually reach, which is when
   * the last piece was captured. One piece covers every write in the interval
   * it was taken over, so a restore arrives at a piece boundary, not at the
   * exact millisecond you named.
   *
   * @param moment - Epoch milliseconds you want back.
   * @returns The full copy, the change pieces to apply in order, and the moment the result reflects.
   */
  async backupRestorePlan(moment: number): Promise<BackupRestorePlan> {
    this.ensureNotClosed()
    return this.runtime.backups.restorePlan(moment)
  }

  /**
   * Tells you which backups no restore still needs, so you can delete them
   * knowing exactly what you give up. Name the earliest moment you still want
   * to reach and the answer also covers the older chains a newer full copy
   * already spans.
   *
   * Sirannon lists them and deletes nothing. The destination is yours.
   *
   * @param options - How far back you still want to be able to restore.
   * @returns The records you may delete, oldest first.
   */
  async backupPiecesSafeToDelete(options?: BackupSafeToDeleteOptions): Promise<BackupChainRecord[]> {
    this.ensureNotClosed()
    return this.runtime.backups.piecesSafeToDelete(options)
  }
}
