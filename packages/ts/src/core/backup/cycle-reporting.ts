import { reportQuietly } from './cycle-callbacks.js'
import { toBackupError, uncapturedLogBytes } from './cycle-guard.js'
import { type BackupCycleStatus, BackupCycleStatusRecorder } from './cycle-status.js'
import type { BackupSkip } from './preferred-node.js'
import type { BackupProgress, BackupRunReport } from './report.js'

/**
 * The callbacks a caller supplies to hear what the cycle is doing.
 *
 * @internal
 */
export interface BackupTurnAudience {
  /** Called with every run the cycle finishes. */
  onRun?: (report: BackupRunReport) => void
  /** Called at step resolution while a run proceeds. */
  onProgress?: (progress: BackupProgress) => void
  /** Called with every turn the cycle passes over. */
  onSkip?: (skip: BackupSkip) => void
  /** Called with every failure a turn raises. */
  onError?: (error: Error) => void
}

/**
 * Keeps the record of what the cycle's turns produced, and passes each one to
 * the caller who asked to hear about it.
 *
 * A turn often breaks inside a step that has already passed the failure to the
 * caller, and the code that started the turn then receives that same failure a
 * second time. This log therefore holds every failure a turn has announced, so
 * that the cycle can announce each of them exactly once.
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
   * Records a finished run and passes it to the caller.
   *
   * @param report - What that run produced.
   */
  ran(report: BackupRunReport): void {
    this.record.ran(report)
    reportQuietly(this.audience.onRun, report)
  }

  /**
   * Records how far the run under way has reached and passes that to the caller.
   *
   * @param progress - The counters that run has reached.
   */
  progressed(progress: BackupProgress): void {
    this.record.progressed(progress)
    reportQuietly(this.audience.onProgress, progress)
  }

  /** Marks a turn as under way, and forgets whatever failures the previous turn announced. */
  turnStarted(): void {
    this.announced.clear()
    this.record.turnStarted()
  }

  /** Marks the turn as finished. */
  turnFinished(): void {
    this.record.turnFinished()
  }

  /**
   * Records a failure as the turn's outcome and passes it to the caller.
   *
   * @param err - What the turn failed with.
   * @param chainId - The chain to record it against, where the cycle holds one.
   */
  failed(err: unknown, chainId?: string): void {
    this.announced.add(err)
    const failure = toBackupError(err)
    this.record.failed(failure, chainId)
    reportQuietly(this.audience.onError, failure)
  }

  /**
   * Answers whether this turn has already passed one failure to the caller.
   *
   * @param err - The failure to look for.
   * @returns Whether the caller has already heard about that failure.
   */
  hasAnnounced(err: unknown): boolean {
    return this.announced.has(err)
  }

  /**
   * Records a turn the cycle passed over and passes it to the caller, stating
   * how much log this node was still holding as it skipped.
   *
   * @param skip - Why the cycle passed the turn over.
   */
  async skipped(skip: BackupSkip): Promise<void> {
    const held = await uncapturedLogBytes(this.logPath)
    const passed = held === undefined ? skip : { ...skip, uncapturedLogBytes: held }
    this.record.skipped(passed)
    reportQuietly(this.audience.onSkip, passed)
  }

  /**
   * Reads everything recorded so far.
   *
   * @param chainId - The chain the cycle is extending, where it has one.
   * @returns What the cycle is doing, and what its recent turns produced.
   */
  read(chainId?: string): BackupCycleStatus {
    return this.record.read(chainId)
  }
}
