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
 * Captures the write-ahead log of a database and then checkpoints it, on the
 * interval that the operator sets.
 *
 * The capture must come first, because a checkpoint lets SQLite overwrite
 * frames that Sirannon has yet to capture, and SQLite reports success either
 * way. A failed capture therefore also skips the checkpoint after it.
 *
 * Sirannon creates the cycle for a database from its `backups` option.
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
   * Resumes the chain that the state file records, or starts a new chain with a
   * full copy, and then repeats the turn on the interval. On a resumed chain,
   * Sirannon takes an ordinary turn straight away, so it sends the frames written
   * while this node was down to the destination before the first interval ends.
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
   * Takes one turn now. In a turn, Sirannon sends any staged capture, reads the
   * frames written since the previous turn, and then checkpoints the log.
   *
   * At most one turn waits in the queue. A call during a turn in progress
   * queues one turn after it, and every later call returns that queued turn,
   * which reads the log when it begins and so captures the writes of every one
   * of those callers. Without that limit, repeated calls during a long full
   * copy would build a queue of turns with nothing left to capture.
   *
   * On a node whose replication group takes its backups from another node, a
   * turn writes nothing and reports a skip.
   *
   * @returns The report of what the turn writes, or undefined where it writes nothing.
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
   * Stops the cycle after one final turn, so that Sirannon sends the writes
   * made since the previous turn to the destination before the database closes.
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
   * Returns every chain at the destination.
   *
   * @returns Every chain, newest first, each with its full copy and its change pieces.
   */
  chains(): Promise<BackupChain[]> {
    return readBackupChains(this.request.destination, this.chainName)
  }

  /**
   * Returns what the cycle is doing now and the outcome of its recent turns.
   *
   * It returns straight away, both during a long full copy and between turns.
   *
   * @returns Whether a turn is in progress, the progress of that turn, and the last run, skip, and failure.
   */
  status(): BackupCycleStatus {
    return this.turnLog.read(this.state?.chainId)
  }

  /**
   * Reads one backup from the destination and checks it against its record in
   * the chains at that destination.
   *
   * @param name - The name that Sirannon stores the backup under.
   * @returns The number of pieces read, their total size in bytes, and the fingerprint where the record holds one.
   */
  async verify(name: string): Promise<BackupVerifyResult> {
    return verifyBackupRecord(this.request.destination, await this.chains(), name)
  }

  /**
   * Runs one turn and reports to the operator any error that stops it. Some
   * deeper steps report a failure themselves before they throw it, so this
   * reports an error only where no step has reported it already, and the
   * operator receives each failure once. When the turn captures nothing and the
   * log has grown past the limit, Sirannon then empties the log, and the
   * resulting report of the writes that no backup holds replaces any failure
   * that the turn recorded.
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

  /** The callbacks and paths that the stand-down code uses to release the chain of this cycle. */
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

  /** Empties the log when this turn captures nothing from it and it has grown past the limit that the operator sets. */
  private async releaseLogPastLimit(): Promise<void> {
    if (this.stopped) return
    try {
      if (await releaseChainPastLogLimit(this.standDownRequest)) this.captured = true
    } catch (err) {
      this.report(err)
    }
  }

  /**
   * Returns whether this node takes the turn that it is starting. Where the
   * replication group takes its backups from another node, this node stands
   * down from its chain and empties its log. Where Sirannon cannot read the
   * group, this node keeps its chain and its log as they are, because no backup
   * holds the frames that it has yet to capture.
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

  /** Reports a skipped turn when the decision carries a reason for it. */
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
   * Runs one turn. When the turn fails with `BACKUP_LOG_REWOUND` or
   * `BACKUP_CHAIN_BROKEN`, no later piece can extend the chain, so this starts a
   * new chain. The caller still receives the error, because no backup holds the
   * lost writes. Where the new chain also fails to start, Sirannon records that
   * second failure against the broken chain, because the cycle holds no chain by
   * then and the operator needs the status to name the chain that they lost.
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
   * Reports a failure to the operator and records it as the outcome of the
   * turn, against the chain that the cycle holds now.
   *
   * @param err - The error that stops the turn.
   */
  private report(err: unknown): void {
    this.turnLog.failed(err, this.state?.chainId)
  }

  /** Records a failure against a chain that the cycle may already have released. */
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
   * Sends the capture staged against the chain that this cycle holds. Once the
   * destination refuses a capture, Sirannon skips the send for the rest of the
   * turn, because a second try would wait out the destination deadline again
   * and delay every turn queued after it.
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
