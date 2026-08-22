import { mkdir, rm } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import { type BackupChain, DEFAULT_CHAIN_NAME, readBackupChains } from './chain.js'
import { captureAndCheckpointTurn, stagedCapturePath } from './cycle-capture.js'
import { decideBackupTurn, previousRunStillActive, startsAFreshChain, unclaimableChainList } from './cycle-guard.js'
import {
  type BackupCycleRequest,
  DEFAULT_BACKUP_NAME_PREFIX,
  DEFAULT_CAPTURE_INTERVAL_MS,
  DEFAULT_FULL_COPY_INTERVAL_MS,
  defaultStagingDir,
} from './cycle-options.js'
import { BackupTurnLog } from './cycle-reporting.js'
import { BackupCycleTimer, SerialTurns } from './cycle-scheduling.js'
import {
  confirmChainStillListed,
  releaseChainPastLogLimit,
  type StandDownRequest,
  standDownFromChain,
} from './cycle-standdown.js'
import { type BackupCycleState, readCycleState, removeCycleState, writeCycleState } from './cycle-state.js'
import type { BackupCycleStatus } from './cycle-status.js'
import { beginReplacementChain, sendStagedCapture } from './cycle-transfer.js'
import type { BackupNodePreference, BackupSkip } from './preferred-node.js'
import type { BackupRunReport } from './report.js'
import { type BackupVerifyResult, verifyBackupRecord } from './verify.js'
import { logPathFor } from './wal-log.js'

/**
 * Captures a database's write-ahead log and then checkpoints it, in that order,
 * on the interval the operator sets.
 *
 * The order is the whole point. A checkpoint lets SQLite overwrite frames
 * nothing has captured yet, and it reports success either way. That is why a
 * capture that fails also stops the checkpoint behind it.
 *
 * A database opens its cycle through the `backups` option. Nobody builds one
 * of these by hand.
 *
 * @public
 */
export class BackupCycle {
  private readonly chainName: string
  private readonly namePrefix: string
  private readonly stagingDir: string
  private readonly logPath: string
  private readonly intervalMs: number
  private readonly fullCopyIntervalMs: number
  private readonly preferredNode: BackupNodePreference

  private readonly timer = new BackupCycleTimer()
  private state: BackupCycleState | null = null
  private queued: Promise<BackupRunReport | undefined> | null = null
  private readonly turns = new SerialTurns()
  private started = false
  private verified = false
  private busy = false
  private captured = false
  private sendRefused = false
  private stopped = false

  private readonly request: BackupCycleRequest
  private readonly turnLog: BackupTurnLog

  /** @internal */
  constructor(request: BackupCycleRequest) {
    this.logPath = logPathFor(request.sourcePath)
    this.turnLog = new BackupTurnLog(request, this.logPath)
    this.request = {
      ...request,
      onRun: report => this.turnLog.ran(report),
      onProgress: progress => this.turnLog.progressed(progress),
    }
    this.chainName = request.chainName ?? DEFAULT_CHAIN_NAME
    this.namePrefix = request.namePrefix ?? DEFAULT_BACKUP_NAME_PREFIX
    this.stagingDir = request.stagingDir ?? defaultStagingDir(request.sourcePath)
    this.intervalMs = request.intervalMs ?? DEFAULT_CAPTURE_INTERVAL_MS
    this.fullCopyIntervalMs = request.fullCopyIntervalMs ?? DEFAULT_FULL_COPY_INTERVAL_MS
    this.preferredNode = request.preferredNode ?? 'replica'
  }

  /**
   * Picks a chain up where the previous run left it, or starts a new one with a
   * full copy, and then repeats on the interval. A chain picked up takes an
   * ordinary turn, so the frames written while this node was down reach the
   * destination rather than waiting for the first interval.
   */
  async start(): Promise<void> {
    await mkdir(this.stagingDir, { recursive: true })
    this.state = (await readCycleState(this.stagingDir)) ?? null
    this.started = true
    const unclaimable = unclaimableChainList(this.request, this.chainName)
    if (unclaimable) this.report(unclaimable)
    await this.runTurn(async () => {
      try {
        if (!(await this.takesTheTurn())) return
        await this.turnOrStartOver()
      } catch (err) {
        this.report(err)
      }
    })
    if (!this.stopped) this.timer.arm(this.intervalMs, () => void this.tick())
  }

  /**
   * Takes one turn now. A turn sends any capture still waiting, reads the
   * frames written since the previous turn, and then checkpoints the log.
   *
   * One turn waits at a time. A call made while a turn is under way queues one
   * behind it, and every call after that joins the queued turn, which reads the
   * log once it begins and so covers their writes as well. Without that bound,
   * a caller asking repeatedly during a long full copy would build a queue of
   * turns with nothing left for any of them to capture.
   *
   * A turn on a node whose group backs up from somewhere else writes nothing
   * and reports a skip.
   *
   * @returns What the turn wrote, or undefined where it wrote nothing.
   */
  runOnce(): Promise<BackupRunReport | undefined> {
    const queued = this.queued
    if (queued) return queued

    const turn = this.runTurn(async () => {
      this.queued = null
      if (!(await this.takesTheTurn())) return undefined
      return this.turnOrStartOver()
    })
    this.queued = turn
    return turn
  }

  /**
   * Stops the cycle. It captures the log one final time first, so the writes
   * made since the previous turn reach the destination before the database
   * closes behind it.
   */
  async stop(): Promise<void> {
    if (this.stopped) return
    this.stopped = true
    this.timer.disarm()
    await this.runTurn(async () => {
      const state = this.state
      if (!state) return
      try {
        await this.turn()
        state.closedCleanly = state.pending === null
        await writeCycleState(this.stagingDir, state)
      } catch (err) {
        this.report(err)
      }
    })
  }

  /**
   * Lists what the destination holds.
   *
   * @returns Every chain, newest first, each with its full copy and its change pieces.
   */
  chains(): Promise<BackupChain[]> {
    return readBackupChains(this.request.destination, this.chainName)
  }

  /**
   * Reads what the cycle is doing at this moment, and what its recent turns
   * produced.
   *
   * A caller following a long full copy reads this without having to wait on
   * the turn, and it answers between turns as well.
   *
   * @returns Whether a turn is under way, how far it has got, and the last run, skip, and failure.
   */
  status(): BackupCycleStatus {
    return this.turnLog.read(this.state?.chainId)
  }

  /**
   * Reads one of this chain's backups back out of the destination and compares
   * it against the record the backup that wrote it left behind.
   *
   * @param name - Name the backup is stored under.
   * @returns The pieces read, the bytes they add up to, and the digest where the backup recorded one.
   */
  async verify(name: string): Promise<BackupVerifyResult> {
    return verifyBackupRecord(this.request.destination, await this.chains(), name)
  }

  /**
   * Runs one turn and passes whatever broke it to the operator. A deeper step
   * announces some failures itself and then raises them, so this announces a
   * failure only where no step has announced it already. The operator therefore
   * hears each failure exactly once. Sirannon empties a log the turn has
   * outgrown once all of that is done, and the report of writes reaching no
   * backup replaces whatever failure the turn recorded.
   */
  private runTurn<T>(op: () => Promise<T>): Promise<T> {
    return this.turns.run(async () => {
      this.busy = true
      this.captured = false
      this.sendRefused = false
      this.turnLog.turnStarted()
      try {
        return await op()
      } catch (err) {
        if (!this.turnLog.hasAnnounced(err)) this.report(err)
        throw err
      } finally {
        if (!this.captured) await this.releaseLogPastLimit()
        this.busy = false
        this.turnLog.turnFinished()
      }
    })
  }

  /** What the stand-down path needs of this cycle to let go of its chain. */
  private get standDownRequest(): StandDownRequest {
    return {
      request: this.request,
      logPath: this.logPath,
      holdsChain: () => this.state !== null,
      sendStagedCapture: () => this.sendWaitingCapture(),
      forgetChain: async () => {
        await this.discardState()
        await removeCycleState(this.stagingDir)
        this.verified = false
      },
      report: err => this.report(err),
    }
  }

  /** Empties a log this turn captured nothing from, past the operator's limit. */
  private async releaseLogPastLimit(): Promise<void> {
    if (this.stopped) return
    try {
      if (await releaseChainPastLogLimit(this.standDownRequest)) this.captured = true
    } catch (err) {
      this.report(err)
    }
  }

  /**
   * Asks whether this node takes the turn it is starting. A node the group
   * backs up from somewhere else stands down from its chain and empties its
   * log instead. A node that could not read its group holds everything where
   * it is, because the frames it has yet to capture are still in no backup.
   */
  private async takesTheTurn(): Promise<boolean> {
    const decision = await decideBackupTurn(this.request.replicationGroup, this.preferredNode)
    if (decision.runs) return true
    await this.reportSkip(decision.skip)
    if (decision.skip?.reason === 'not-preferred') {
      try {
        if (await standDownFromChain(this.standDownRequest)) this.captured = true
      } catch (err) {
        this.report(err)
      }
    }
    return false
  }

  /** Passes a skipped turn on, where the cycle has a reason to give for it. */
  private async reportSkip(skip: BackupSkip | undefined): Promise<void> {
    if (skip) await this.turnLog.skipped(skip)
  }

  private async tick(): Promise<void> {
    if (this.stopped) return
    if (this.busy) {
      await this.reportSkip(previousRunStillActive())
      return
    }
    await this.runOnce().catch(() => {})
  }

  private async verifyChain(): Promise<void> {
    this.verified = await confirmChainStillListed({
      request: this.request,
      chainName: this.chainName,
      state: this.state,
      verified: this.verified,
      discardState: () => this.discardState(),
      report: err => this.report(err),
    })
  }

  private async discardState(): Promise<void> {
    const pending = this.state?.pending
    if (pending) await rm(stagedCapturePath(this.stagingDir, pending.sequence), { force: true })
    this.state = null
  }

  /**
   * Runs one turn. A log that restarted before the capture reached it leaves a
   * chain nothing can extend, so this starts a fresh one. The caller still gets
   * the error: those writes are in no backup, and an operator has to hear that.
   * Where the replacement chain also fails to start, Sirannon records that
   * second failure against the chain the restart broke, because the cycle holds
   * no chain of its own by then and an operator reading the status has to know
   * which one they have lost.
   */
  private async turnOrStartOver(): Promise<BackupRunReport | undefined> {
    try {
      return await this.turn()
    } catch (err) {
      if (startsAFreshChain(err)) {
        const broken = this.state?.chainId
        this.reportAgainstChain(err, broken)
        await this.replaceChain().catch(chainErr => this.reportAgainstChain(chainErr, broken))
      }
      throw err
    }
  }

  private async turn(): Promise<BackupRunReport | undefined> {
    if (!this.started) throw new SirannonError('The backup cycle has not started', 'BACKUP_ERROR')

    await this.verifyChain()
    const state = this.state
    if (!state) return this.replaceChain()

    let last = await this.sendWaitingCapture()
    if (Date.now() - state.chainStartedAt >= this.fullCopyIntervalMs) {
      return (await this.replaceChain()) ?? last
    }

    await this.captureAndCheckpoint()
    last = (await this.sendWaitingCapture()) ?? last
    return last
  }

  /**
   * Passes a failure to the operator and records it as the turn's outcome.
   *
   * @param err - What the turn failed with.
   * @param chainId - The chain to record it against, which defaults to the one the cycle holds now.
   */
  private report(err: unknown): void {
    this.turnLog.failed(err, this.state?.chainId)
  }

  /** Records a failure against a chain the cycle may already have let go of. */
  private reportAgainstChain(err: unknown, chainId: string | undefined): void {
    this.turnLog.failed(err, chainId)
  }

  private async replaceChain(): Promise<BackupRunReport | undefined> {
    const previousChainId = this.state?.chainId
    await this.discardState()

    const begun = await beginReplacementChain(
      this.request,
      this.chainName,
      this.namePrefix,
      this.stagingDir,
      previousChainId,
    )

    this.verified = false
    this.state = begun.state
    this.captured = true
    this.request.onRun?.(begun.report)
    return begun.report
  }

  private async captureAndCheckpoint(): Promise<void> {
    const state = this.state
    if (!state) return

    await captureAndCheckpointTurn({
      request: this.request,
      state,
      logPath: this.logPath,
      stagingDir: this.stagingDir,
      namePrefix: this.namePrefix,
    })
    this.captured = true
  }

  /**
   * Sends the capture staged against the chain this cycle holds. A turn offers
   * a capture the destination has already refused no second time, because that
   * second offer waits out the destination deadline again and delays every turn
   * behind it.
   */
  private async sendWaitingCapture(): Promise<BackupRunReport | undefined> {
    if (this.sendRefused) return undefined
    try {
      return await sendStagedCapture(this.request, this.chainName, this.stagingDir, this.state)
    } catch (err) {
      this.sendRefused = true
      throw err
    }
  }
}
