import { SirannonError } from '../errors.js'
import type { BackupSkip } from './preferred-node.js'
import type { BackupProgress, BackupRunReport } from './report.js'

/** A failure one turn of the cycle raised.
 * @public
 */
export interface BackupCycleError {
  /** Code the failure states, which is what a caller matches on. */
  code: string
  /** What happened, in a sentence an operator can read in a log. */
  message: string
  /** Epoch milliseconds the cycle raised it. */
  at: number
  /** Milliseconds from the start of the turn to the failure. */
  durationMs: number
  /** The chain the cycle was extending, where it held one. */
  chainId?: string
  /**
   * How far the run had got when it failed. A turn that fails before its copy
   * begins states none. A turn that fails part-way through states the run
   * identifier, the pages it still had to move, and the pieces and bytes that
   * had already reached the destination.
   */
  progress?: BackupProgress
}

/** What the cycle is doing now, and what its recent turns produced.
 * @public
 */
export interface BackupCycleStatus {
  /** Whether a turn is under way. */
  running: boolean
  /** The chain the cycle is extending, where it has one. */
  chainId?: string
  /** How far the turn under way has got. It is absent between turns. */
  progress?: BackupProgress
  /** What the most recent turn that wrote anything produced. */
  lastRun?: BackupRunReport
  /** The most recent turn the cycle passed over, and what it passed that turn over for. */
  lastSkip?: BackupSkip
  /** The most recent failure a turn raised. */
  lastError?: BackupCycleError
}

/**
 * Records what the cycle is doing, so that a caller can ask at any moment
 * without having to catch every callback as it fires.
 *
 * Each backup cycle owns one of these and reads it through its own `status`
 * member. Nobody builds one by hand.
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

  /** Marks a turn as under way and clears the progress the previous turn recorded. */
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

  /** Records how far the turn under way has got. */
  progressed(progress: BackupProgress): void {
    this.latestProgress = progress
  }

  /** Records what a finished turn wrote. */
  ran(report: BackupRunReport): void {
    this.latestRun = report
  }

  /** Records a turn the cycle passed over. */
  skipped(skip: BackupSkip): void {
    this.latestSkip = skip
  }

  /**
   * Records a failure, under the code a caller matches on, along with how far
   * the run had got and which chain it was extending.
   *
   * @param err - What the turn failed with.
   * @param chainId - The chain the cycle was extending, where it held one.
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
   * Reads everything recorded so far.
   *
   * @param chainId - The chain the cycle is extending, where it has one.
   * @returns What the cycle is doing, and what its recent turns produced.
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
