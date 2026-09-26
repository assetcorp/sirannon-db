import type { BackupRestoreProgress, BackupRestoreReport } from '../core/backup/restore-options.js'
import { SirannonError } from '../core/errors.js'

function describeFailure(err: unknown): { code: string; message: string } {
  return {
    code: err instanceof SirannonError ? err.code : 'INTERNAL_ERROR',
    message: err instanceof SirannonError ? err.message : 'An unexpected error occurred',
  }
}

/** The state of one database's restore. `idle` means that no restore has started since the server started.
 * @public
 */
export type BackupRestoreState = 'idle' | 'running' | 'done' | 'failed'

/** What the server reports about the restore of one database.
 * @public
 */
export interface BackupRestoreStatus {
  /** The state of the restore. */
  state: BackupRestoreState
  /** The moment, in epoch milliseconds, that the caller asked to restore to. */
  moment?: number
  /** When the restore started, in epoch milliseconds. */
  startedAt?: number
  /** When the restore finished, in epoch milliseconds, whether it succeeded or failed. */
  finishedAt?: number
  /** The restore's progress, which the server updates after it fetches each piece. */
  progress?: BackupRestoreProgress
  /** The report of a finished restore. */
  report?: BackupRestoreReport
  /**
   * The error that stopped the rebuilt database from opening again. The rebuilt
   * data is on disk, but the registry has no database open under the identifier,
   * so restart the process when you see this error.
   */
  reopenError?: { code: string; message: string }
  /** The error that stopped a failed restore. */
  error?: { code: string; message: string }
}

/**
 * Records the state of each database's restore, so that a caller who triggers
 * one can read its outcome from a separate route.
 *
 * The server closes the database while it rebuilds it, so the server cannot
 * store the record in that database. It keeps one entry per identifier in
 * memory, and each new restore replaces the previous entry.
 *
 * @internal
 */
export class BackupRestoreRuns {
  private readonly runs = new Map<string, BackupRestoreStatus>()

  /**
   * Marks a restore of one database as started, unless another restore of it is in progress.
   *
   * @param databaseId - The identifier of the database that the server rebuilds.
   * @param moment - The moment, in epoch milliseconds, that the caller asked to restore to.
   * @returns True when this call claims the database, and false when another restore of it is in progress.
   */
  claim(databaseId: string, moment: number): boolean {
    if (this.runs.get(databaseId)?.state === 'running') return false
    this.runs.set(databaseId, { state: 'running', moment, startedAt: Date.now() })
    return true
  }

  /**
   * Records the progress of one database's restore.
   *
   * @param databaseId - The identifier of the database that the server is rebuilding.
   * @param progress - Pieces fetched, bytes fetched, and change pieces replayed.
   */
  progressed(databaseId: string, progress: BackupRestoreProgress): void {
    const run = this.runs.get(databaseId)
    if (run?.state === 'running') run.progress = progress
  }

  /**
   * Records a restore that rebuilt the database.
   *
   * When the database fails to open again, this method records that error beside
   * the report and keeps the state `done`, because Sirannon replaced the data
   * either way, and an operator who saw only the failure could believe that the
   * data was unchanged.
   *
   * @param databaseId - The identifier of the rebuilt database.
   * @param report - The restore's report.
   * @param reopenFailure - The error that stopped the database from opening again, if there was one.
   */
  finished(databaseId: string, report: BackupRestoreReport, reopenFailure?: unknown): void {
    this.runs.set(databaseId, {
      state: 'done',
      moment: this.runs.get(databaseId)?.moment ?? report.restoresTo,
      startedAt: report.startedAt,
      finishedAt: report.finishedAt,
      report,
      ...(reopenFailure === undefined ? {} : { reopenError: describeFailure(reopenFailure) }),
    })
  }

  /**
   * Records a failed restore under the code of the error that stopped it.
   *
   * The record keeps the message of a `SirannonError` only. For any other error,
   * it stores the message that the HTTP layer sends for an unexpected failure,
   * because a runtime error can include file paths and other internal details of
   * the server's machine.
   *
   * @param databaseId - The identifier of the database that the server was rebuilding.
   * @param err - The error that stopped the restore.
   */
  failed(databaseId: string, err: unknown): void {
    const previous = this.runs.get(databaseId)
    this.runs.set(databaseId, {
      state: 'failed',
      ...(previous?.moment === undefined ? {} : { moment: previous.moment }),
      ...(previous?.startedAt === undefined ? {} : { startedAt: previous.startedAt }),
      finishedAt: Date.now(),
      error: describeFailure(err),
    })
  }

  /**
   * Returns the status of one database's restore.
   *
   * @param databaseId - The identifier of the database.
   * @returns The restore's status, or an `idle` status when no restore has started.
   */
  read(databaseId: string): BackupRestoreStatus {
    return this.runs.get(databaseId) ?? { state: 'idle' }
  }
}
