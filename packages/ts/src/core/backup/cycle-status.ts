import { SirannonError } from '../errors.js'
import type { BackupSkip } from './preferred-node.js'
import type { BackupProgress, BackupRunReport } from './report.js'

/** A failure of one turn of the cycle.
 * @public
 */
export interface BackupCycleError {
  /** The error code, which a caller matches on. */
  code: string
  /** A sentence for the log of the operator that describes the failure. */
  message: string
  /** The moment, in epoch milliseconds, that Sirannon recorded the failure. */
  at: number
  /** The time from the start of the turn to the failure, in milliseconds. */
  durationMs: number
  /** The chain that the cycle held when the failure occurred, where it held one. */
  chainId?: string
  /**
   * The progress of the backup at the failure. Sirannon leaves this field out
   * for a failure before the copy begins. For a failure part-way through, it
   * records the backup identifier, the pages left to copy, and the pieces and
   * bytes already stored at the destination.
   */
  progress?: BackupProgress
}

/** What the cycle is doing now, and the outcome of its recent turns.
 * @public
 */
export interface BackupCycleStatus {
  /** Whether a turn is in progress. */
  running: boolean
  /** The chain that the cycle extends, where it holds one. */
  chainId?: string
  /** The progress of the current turn, which Sirannon clears between turns. */
  progress?: BackupProgress
  /** The report of the most recent backup that the cycle finished. */
  lastRun?: BackupRunReport
  /** The most recent skipped turn, and the reason for the skip. */
  lastSkip?: BackupSkip
  /** The most recent failure of a turn. */
  lastError?: BackupCycleError
}

/**
 * Records what the cycle is doing, so that a caller can read the status at any
 * moment without handling every callback.
 *
 * Each backup cycle has one recorder, which a caller reads through the `status`
 * method of the cycle.
 *
 * @internal
 */
export class BackupCycleStatusRecorder {
  private turnRunning = false
  private turnStartedAt = 0
  private latestProgress: BackupProgress | undefined
  private latestRun: BackupRunReport | undefined
  private latestSkip: BackupSkip | undefined
  private latestError: BackupCycleError | undefined

  /** Marks a turn as in progress and clears the progress that the previous turn recorded. */
  turnStarted(): void {
    this.turnRunning = true
    this.turnStartedAt = Date.now()
    this.latestProgress = undefined
  }

  /** Marks the turn as finished. A caller then reads the outcome from the last run, skip, or failure. */
  turnFinished(): void {
    this.turnRunning = false
    this.turnStartedAt = 0
    this.latestProgress = undefined
  }

  /** Records the progress of the current turn. */
  progressed(progress: BackupProgress): void {
    this.latestProgress = progress
  }

  /** Records the report of a finished backup. */
  ran(report: BackupRunReport): void {
    this.latestRun = report
  }

  /** Records a skipped turn. */
  skipped(skip: BackupSkip): void {
    this.latestSkip = skip
  }

  /**
   * Records a failure with its error code, the progress of the current backup,
   * and the chain that the cycle extends.
   *
   * @param err - The error that stops the turn.
   * @param chainId - The chain that the cycle extends, where it holds one.
   */
  failed(err: Error, chainId?: string): void {
    const at = Date.now()
    this.latestError = {
      code: err instanceof SirannonError ? err.code : 'BACKUP_ERROR',
      message: err.message,
      at,
      durationMs: this.turnStartedAt === 0 ? 0 : at - this.turnStartedAt,
      ...(chainId === undefined ? {} : { chainId }),
      ...(this.latestProgress === undefined ? {} : { progress: this.latestProgress }),
    }
  }

  /**
   * Returns everything recorded so far.
   *
   * @param chainId - The chain that the cycle extends, where it holds one.
   * @returns What the cycle is doing now, and the outcome of its recent turns.
   */
  read(chainId?: string): BackupCycleStatus {
    return {
      running: this.turnRunning,
      ...(chainId === undefined ? {} : { chainId }),
      ...(this.latestProgress === undefined ? {} : { progress: this.latestProgress }),
      ...(this.latestRun === undefined ? {} : { lastRun: this.latestRun }),
      ...(this.latestSkip === undefined ? {} : { lastSkip: this.latestSkip }),
      ...(this.latestError === undefined ? {} : { lastError: this.latestError }),
    }
  }
}
