import { existsSync } from 'node:fs'
import { mkdir, rm } from 'node:fs/promises'
import { SirannonError } from '../errors.js'
import { type BackupChain, DEFAULT_CHAIN_NAME, readBackupChains, readChainHeads } from './chain.js'
import { checkpointLog } from './checkpoint.js'
import { captureLogFrames, stagedCapturePath } from './cycle-capture.js'
import {
  type BackupCycleRequest,
  DEFAULT_BACKUP_NAME_PREFIX,
  DEFAULT_CAPTURE_INTERVAL_MS,
  DEFAULT_FULL_COPY_INTERVAL_MS,
} from './cycle-options.js'
import { type BackupCycleState, readCycleState, writeCycleState } from './cycle-state.js'
import { startChain, transferCapture } from './cycle-transfer.js'
import type { BackupRunReport } from './report.js'

const LOG_REWOUND = 'BACKUP_LOG_REWOUND'
const CHAIN_BROKEN = 'BACKUP_CHAIN_BROKEN'
const STARTS_A_FRESH_CHAIN = [LOG_REWOUND, CHAIN_BROKEN]

function toError(value: unknown): Error {
  if (value instanceof Error) return value
  return new SirannonError(typeof value === 'string' ? value : 'The backup cycle failed', 'BACKUP_ERROR')
}

/**
 * Works out where a database stages its captures when the operator names no
 * directory. It goes beside the database file, so a capture that has yet to
 * reach the destination is still there after a restart.
 *
 * @param sourcePath - Path of the database file.
 * @returns Path of that directory.
 */
export function defaultStagingDir(sourcePath: string): string {
  return `${sourcePath}-backup`
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

  private timer: ReturnType<typeof setInterval> | null = null
  private state: BackupCycleState | null = null
  private inFlight: Promise<unknown> = Promise.resolve()
  private started = false
  private verified = false
  private ticking = false
  private stopped = false

  /** @internal */
  constructor(private readonly request: BackupCycleRequest) {
    this.chainName = request.chainName ?? DEFAULT_CHAIN_NAME
    this.namePrefix = request.namePrefix ?? DEFAULT_BACKUP_NAME_PREFIX
    this.stagingDir = request.stagingDir ?? defaultStagingDir(request.sourcePath)
    this.logPath = `${request.sourcePath}-wal`
    this.intervalMs = request.intervalMs ?? DEFAULT_CAPTURE_INTERVAL_MS
    this.fullCopyIntervalMs = request.fullCopyIntervalMs ?? DEFAULT_FULL_COPY_INTERVAL_MS
  }

  /**
   * Picks a chain up where the previous run left it, or starts a new one with a
   * full copy, and then repeats on the interval.
   */
  async start(): Promise<void> {
    await mkdir(this.stagingDir, { recursive: true })
    this.state = (await readCycleState(this.stagingDir)) ?? null
    this.started = true
    await this.serialise(async () => {
      try {
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
   * @returns What the turn wrote, or undefined where the log held nothing new.
   */
  runOnce(): Promise<BackupRunReport | undefined> {
    return this.serialise(() => this.turnOrStartOver())
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
    await this.serialise(async () => {
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
    if (this.ticking || this.stopped) return
    this.ticking = true
    try {
      await this.runOnce()
    } catch (err) {
      this.report(err)
    } finally {
      this.ticking = false
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

    const headIndex = (await readChainHeads(this.request.destination, this.chainName)).length
    const started = await startChain(this.request, this.chainName, this.namePrefix, headIndex, previousChainId)

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
    })
  }

  private async sendWaitingCapture(): Promise<BackupRunReport | undefined> {
    const state = this.state
    const pending = state?.pending
    if (!state || !pending) return undefined

    const stagedPath = stagedCapturePath(this.stagingDir, pending.sequence)
    if (!existsSync(stagedPath)) {
      throw new SirannonError(
        `The frames staged for change piece ${pending.sequence} of chain '${state.chainId}' are no longer in '${stagedPath}', so the writes they carried are in no backup. ` +
          'Leave the staging directory to Sirannon, and take a fresh full copy so a new chain starts from a known state.',
        CHAIN_BROKEN,
      )
    }
    const report = await transferCapture(
      this.request,
      this.chainName,
      state.chainId,
      pending,
      state.records,
      stagedPath,
    )
    state.records++
    state.cursor = pending.cursor
    state.pending = null
    await writeCycleState(this.stagingDir, state)
    await rm(stagedPath, { force: true })
    this.request.onRun?.(report)
    return report
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
  return new BackupCycle(request)
}
