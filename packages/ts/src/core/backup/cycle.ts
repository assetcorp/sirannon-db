import { mkdir, rm } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import { type BackupChain, DEFAULT_CHAIN_NAME, readBackupChains, readChainHeads } from './chain.js'
import { checkpointLog } from './checkpoint.js'
import { captureLogFrames, stagedCapturePath } from './cycle-capture.js'
import {
  checkpointWithoutCapturing,
  decideBackupTurn,
  logGrownPastLimit,
  previousRunStillActive,
} from './cycle-guard.js'
import {
  type BackupCycleRequest,
  DEFAULT_BACKUP_NAME_PREFIX,
  DEFAULT_CAPTURE_INTERVAL_MS,
  DEFAULT_FULL_COPY_INTERVAL_MS,
  defaultStagingDir,
} from './cycle-options.js'
import { type BackupCycleState, readCycleState, removeCycleState, writeCycleState } from './cycle-state.js'
import { sendStagedCapture, startChain } from './cycle-transfer.js'
import { DEFAULT_DESTINATION_TIMEOUT_MS, destinationWithDeadline } from './destination-deadline.js'
import type { BackupNodePreference, BackupSkip } from './preferred-node.js'
import type { BackupRunReport } from './report.js'

const LOG_REWOUND = 'BACKUP_LOG_REWOUND'
const CHAIN_BROKEN = 'BACKUP_CHAIN_BROKEN'
const STARTS_A_FRESH_CHAIN = [LOG_REWOUND, CHAIN_BROKEN]

function toError(value: unknown): Error {
  if (value instanceof Error) return value
  return new SirannonError(typeof value === 'string' ? value : 'The backup cycle failed', 'BACKUP_ERROR')
}

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

  private timer: ReturnType<typeof setInterval> | null = null
  private state: BackupCycleState | null = null
  private inFlight: Promise<unknown> = Promise.resolve()
  private started = false
  private verified = false
  private busy = false
  private captured = false
  private stopped = false

  /** @internal */
  constructor(private readonly request: BackupCycleRequest) {
    this.chainName = request.chainName ?? DEFAULT_CHAIN_NAME
    this.namePrefix = request.namePrefix ?? DEFAULT_BACKUP_NAME_PREFIX
    this.stagingDir = request.stagingDir ?? defaultStagingDir(request.sourcePath)
    this.logPath = `${request.sourcePath}-wal`
    this.intervalMs = request.intervalMs ?? DEFAULT_CAPTURE_INTERVAL_MS
    this.fullCopyIntervalMs = request.fullCopyIntervalMs ?? DEFAULT_FULL_COPY_INTERVAL_MS
    this.preferredNode = request.preferredNode ?? 'replica'
  }

  /**
   * Picks a chain up where the previous run left it, or starts a new one with a
   * full copy, and then repeats on the interval.
   */
  async start(): Promise<void> {
    await mkdir(this.stagingDir, { recursive: true })
    this.state = (await readCycleState(this.stagingDir)) ?? null
    this.started = true
    await this.runTurn(async () => {
      try {
        if (!(await this.takesTheTurn())) return
        await this.verifyChain()
        if (this.state) await this.sendWaitingCapture()
        else await this.replaceChain()
      } catch (err) {
        this.report(err)
      }
    })
    this.arm()
  }

  /**
   * Runs one turn now. A turn sends any capture still waiting, reads the frames
   * written since the previous turn, and checkpoints the log.
   *
   * A turn on a node its group backs up from somewhere else writes nothing and
   * reports a skip instead.
   *
   * @returns What the turn wrote, or undefined where it wrote nothing.
   */
  runOnce(): Promise<BackupRunReport | undefined> {
    return this.runTurn(async () => {
      if (!(await this.takesTheTurn())) return undefined
      return this.turnOrStartOver()
    })
  }

  /**
   * Stops the cycle. It captures the log one final time first, so the writes
   * made since the previous turn reach the destination before the database
   * closes behind it.
   */
  async stop(): Promise<void> {
    if (this.stopped) return
    this.stopped = true
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = null
    }
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

  private runTurn<T>(op: () => Promise<T>): Promise<T> {
    return this.serialise(async () => {
      this.busy = true
      this.captured = false
      try {
        return await op()
      } finally {
        if (!this.captured) await this.releaseLogPastLimit()
        this.busy = false
      }
    })
  }

  /**
   * Empties a log the cycle has left behind, where the operator set a limit and
   * this turn captured nothing. The chain ends there, with the report naming
   * the writes that reach no backup.
   */
  private async releaseLogPastLimit(): Promise<void> {
    if (this.stopped) return
    try {
      const lost = await logGrownPastLimit(this.logPath, this.request.maxUncapturedLogBytes, this.request.databaseId)
      if (!lost) return
      this.report(lost)
      await this.standDown()
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
    this.reportSkip(decision.skip)
    if (decision.skip?.reason === 'not-preferred') {
      try {
        await this.standDown()
      } catch (err) {
        this.report(err)
      }
    }
    return false
  }

  /**
   * Lets go of the chain this node was building. Its captures reach the
   * destination first, because a piece already read off the log is a piece the
   * chain can still use. The turn that brings the group's backups back to this
   * node starts a fresh chain, since a chain of physical pieces from one node
   * continues on no other.
   */
  private async standDown(): Promise<void> {
    if (this.state) {
      try {
        await this.sendWaitingCapture()
      } catch (err) {
        this.report(err)
      }
      await this.discardState()
      await removeCycleState(this.stagingDir)
      this.verified = false
    }
    await checkpointWithoutCapturing(this.request)
    this.captured = true
  }

  private reportSkip(skip: BackupSkip | undefined): void {
    if (!skip || !this.request.onSkip) return
    try {
      this.request.onSkip(skip)
    } catch {}
  }

  private serialise<T>(op: () => Promise<T>): Promise<T> {
    const run = this.inFlight.then(op, op)
    this.inFlight = run.then(
      () => {},
      () => {},
    )
    return run
  }

  private arm(): void {
    if (this.stopped || this.intervalMs <= 0) return
    this.timer = setInterval(() => void this.tick(), this.intervalMs)
    this.timer.unref?.()
  }

  private async tick(): Promise<void> {
    if (this.stopped) return
    if (this.busy) {
      this.reportSkip(previousRunStillActive())
      return
    }
    try {
      await this.runOnce()
    } catch (err) {
      this.report(err)
    }
  }

  /**
   * Checks that the destination still holds the chain the state file names.
   * Where the check cannot reach the destination, the chain stays unverified and
   * the next turn runs it again before it appends anything. A record appended
   * under a chain the destination has lost would be in no listing, so no restore
   * could reach it.
   */
  private async verifyChain(): Promise<void> {
    if (this.verified || !this.state) return
    const heads = await readChainHeads(this.request.destination, this.chainName)
    if (heads.some(head => head.chainId === this.state?.chainId)) this.verified = true
    else await this.discardState()
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
   */
  private async turnOrStartOver(): Promise<BackupRunReport | undefined> {
    try {
      return await this.turn()
    } catch (err) {
      if (err instanceof SirannonError && STARTS_A_FRESH_CHAIN.includes(err.code)) {
        this.report(err)
        await this.replaceChain().catch(chainErr => this.report(chainErr))
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

  private report(err: unknown): void {
    if (!this.request.onError) return
    try {
      this.request.onError(toError(err))
    } catch {}
  }

  private async replaceChain(): Promise<BackupRunReport | undefined> {
    const previousChainId = this.state?.chainId
    await this.discardState()

    const started = await startChain(this.request, this.chainName, this.namePrefix, previousChainId)

    this.verified = true
    this.state = {
      chainName: this.chainName,
      chainId: started.chainId,
      chainStartedAt: started.startedAt,
      records: 1,
      cursor: null,
      pending: null,
      closedCleanly: false,
    }
    await writeCycleState(this.stagingDir, this.state)
    this.captured = true
    this.request.onRun?.(started.report)
    return started.report
  }

  private async captureAndCheckpoint(): Promise<void> {
    const state = this.state
    if (!state) return

    await this.request.runExclusive(async () => {
      const captured = await captureLogFrames({
        sourcePath: this.request.sourcePath,
        logPath: this.logPath,
        stagingDir: this.stagingDir,
        chainId: state.chainId,
        namePrefix: this.namePrefix,
        sequence: state.records,
        cursor: state.cursor,
        expectNewLog: state.closedCleanly,
      })

      if (captured) {
        state.pending = captured
        await writeCycleState(this.stagingDir, state)
      }

      const checkpointed = (await checkpointLog(this.request.acquireWriter())).emptied
      const cursor = captured?.cursor ?? state.cursor
      if (cursor) cursor.checkpointed = checkpointed
      state.closedCleanly = false
      await writeCycleState(this.stagingDir, state)
      this.captured = true
    })
  }

  private sendWaitingCapture(): Promise<BackupRunReport | undefined> {
    return sendStagedCapture(this.request, this.chainName, this.stagingDir, this.state)
  }
}

/**
 * Builds the cycle that captures a database's log and checkpoints it. It runs
 * nothing until someone starts it.
 *
 * @param request - The operator's settings, plus the database to run against.
 * @returns The cycle.
 */
export function createBackupCycle(request: BackupCycleRequest): BackupCycle {
  return new BackupCycle({
    ...request,
    destination: destinationWithDeadline(
      request.destination,
      request.destinationTimeoutMs ?? DEFAULT_DESTINATION_TIMEOUT_MS,
    ),
  })
}
