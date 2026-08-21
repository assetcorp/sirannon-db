import type { BackupRestoreProgress, BackupRestoreReport } from '../core/backup/restore-options.js'
import { SirannonError } from '../core/errors.js'

function describeFailure(err: unknown): { code: string; message: string } {
  return {
    code: err instanceof SirannonError ? err.code : 'INTERNAL_ERROR',
    message: err instanceof SirannonError ? err.message : 'An unexpected error occurred',
  }
}

/** Where one database's restore stands. `idle` means that none has started since the server did.
 * @public
 */
export type BackupRestoreState = 'idle' | 'running' | 'done' | 'failed'

/** What the server reports about the restore of one database.
 * @public
 */
export interface BackupRestoreStatus {
  /** Where the restore stands. */
  state: BackupRestoreState
  /** Epoch milliseconds the caller asked to be taken back to. */
  moment?: number
  /** Epoch milliseconds the restore started. */
  startedAt?: number
  /** Epoch milliseconds it finished, whether it succeeded or failed. */
  finishedAt?: number
  /** How far it has got, reported after every piece it fetches. */
  progress?: BackupRestoreProgress
  /** What a finished restore produced. */
  report?: BackupRestoreReport
  /**
   * Why the database it replaced would not open again. The rebuilt data is on
   * disk and the registry has nothing open under the identifier, so an operator
   * who sees this restarts the process.
   */
  reopenError?: { code: string; message: string }
  /** Why a failed restore stopped. */
  error?: { code: string; message: string }
}

/**
 * Records where each database's restore stands, so that a caller who triggered
 * one reads its outcome from a route of its own.
 *
 * A restore closes the database it rebuilds, so that database can store nothing
 * about it. The server stores it, one entry per identifier, and each new
 * restore replaces the entry before it.
 *
 * @internal
 */
export class BackupRestoreRuns {
  private readonly runs = new Map<string, BackupRestoreStatus>()

  /**
   * Marks a restore of one database as under way, unless one is already running.
   *
   * @param databaseId - Identifier of the database being rebuilt.
   * @param moment - Epoch milliseconds the caller asked to be taken back to.
   * @returns True where this call claimed the database, false where a restore already has it.
   */
  claim(databaseId: string, moment: number): boolean {
    if (this.runs.get(databaseId)?.state === 'running') return false
    this.runs.set(databaseId, { state: 'running', moment, startedAt: Date.now() })
    return true
  }

  /**
   * Records how far the restore of one database has got.
   *
   * @param databaseId - Identifier of the database being rebuilt.
   * @param progress - Pieces fetched, bytes fetched, and change pieces replayed.
   */
  progressed(databaseId: string, progress: BackupRestoreProgress): void {
    const run = this.runs.get(databaseId)
    if (run?.state === 'running') run.progress = progress
  }

  /**
   * Records a restore that rebuilt the database.
   *
   * A reopen that failed is reported beside the report, and the state stays
   * `done`, because Sirannon replaced the data either way and an operator shown
   * only the failure would believe their database untouched.
   *
   * @param databaseId - Identifier of the database that was rebuilt.
   * @param report - What the restore produced.
   * @param reopenFailure - What stopped the database opening again, where anything did.
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
   * Records a restore that stopped, under the code it stopped with.
   *
   * Only a `SirannonError` passes its message through. Anything else reports
   * the same sentence the HTTP layer sends for an unexpected failure, since a
   * runtime error names the paths and the internals of the machine serving the
   * request.
   *
   * @param databaseId - Identifier of the database it was rebuilding.
   * @param err - What stopped it.
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
   * Reads where one database's restore stands.
   *
   * @param databaseId - Identifier of the database to report on.
   * @returns The restore, or an idle answer where none has run.
   */
  read(databaseId: string): BackupRestoreStatus {
    return this.runs.get(databaseId) ?? { state: 'idle' }
  }
}
