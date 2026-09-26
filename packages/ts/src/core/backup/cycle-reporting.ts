import { reportQuietly } from './cycle-callbacks.js'
import { toBackupError, uncapturedLogBytes } from './cycle-guard.js'
import { type BackupCycleStatus, BackupCycleStatusRecorder } from './cycle-status.js'
import type { BackupSkip } from './preferred-node.js'
import type { BackupProgress, BackupRunReport } from './report.js'

/**
 * The callbacks that a caller supplies to follow what the cycle is doing.
 *
 * @internal
 */
export interface BackupTurnAudience {
  /** Called with the report of every backup that the cycle finishes. */
  onRun?: (report: BackupRunReport) => void
  /** Called after each step of the copy and after each stored piece. */
  onProgress?: (progress: BackupProgress) => void
  /** Called with every turn that the cycle skips. */
  onSkip?: (skip: BackupSkip) => void
  /** Called with every failure of a turn. */
  onError?: (error: Error) => void
}

/**
 * Records the outcome of each turn of the cycle, and passes each outcome to the
 * callback that the caller supplies for it.
 *
 * A turn can fail inside a step that has already reported the failure to the
 * caller, and the code that started the turn then receives that same failure a
 * second time. This log therefore keeps every failure that the current turn
 * reports, so that the cycle reports each failure once.
 *
 * @internal
 */
export class BackupTurnLog {
  private readonly record = new BackupCycleStatusRecorder()
  private readonly announced = new Set<unknown>()

  constructor(
    private readonly audience: BackupTurnAudience,
    private readonly logPath: string,
  ) {}

  /**
   * Records a finished backup and passes its report to the caller.
   *
   * @param report - The report of the backup.
   */
  ran(report: BackupRunReport): void {
    this.record.ran(report)
    reportQuietly(this.audience.onRun, report)
  }

  /**
   * Records the progress of the current backup and passes it to the caller.
   *
   * @param progress - The current counters of that backup.
   */
  progressed(progress: BackupProgress): void {
    this.record.progressed(progress)
    reportQuietly(this.audience.onProgress, progress)
  }

  /** Marks a turn as in progress, and clears the failures that the previous turn reported. */
  turnStarted(): void {
    this.announced.clear()
    this.record.turnStarted()
  }

  /** Marks the turn as finished. */
  turnFinished(): void {
    this.record.turnFinished()
  }

  /**
   * Records a failure as the outcome of the turn and passes it to the caller.
   *
   * @param err - The error that stops the turn.
   * @param chainId - The chain to record the failure against, where the cycle holds one.
   */
  failed(err: unknown, chainId?: string): void {
    this.announced.add(err)
    const failure = toBackupError(err)
    this.record.failed(failure, chainId)
    reportQuietly(this.audience.onError, failure)
  }

  /**
   * Returns whether this turn has already reported a given failure to the caller.
   *
   * @param err - The failure to look for.
   * @returns Whether this turn has already reported that failure.
   */
  hasAnnounced(err: unknown): boolean {
    return this.announced.has(err)
  }

  /**
   * Records a skipped turn and passes it to the caller, with the size of the
   * write-ahead log on this node at the skip.
   *
   * @param skip - The reason that the cycle skips the turn.
   */
  async skipped(skip: BackupSkip): Promise<void> {
    const held = await uncapturedLogBytes(this.logPath)
    const passed = held === undefined ? skip : { ...skip, uncapturedLogBytes: held }
    this.record.skipped(passed)
    reportQuietly(this.audience.onSkip, passed)
  }

  /**
   * Returns everything recorded so far.
   *
   * @param chainId - The chain that the cycle extends, where it holds one.
   * @returns What the cycle is doing now, and the outcome of its recent turns.
   */
  read(chainId?: string): BackupCycleStatus {
    return this.record.read(chainId)
  }
}
