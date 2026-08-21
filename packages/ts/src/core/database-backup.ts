import { type BackupCapabilities, describeBackupCapabilities } from './backup/capabilities.js'
import { type BackupChain, type BackupChainRecord, DEFAULT_CHAIN_NAME } from './backup/chain.js'
import {
  type BackupRestorePlan,
  type BackupSafeToDeleteOptions,
  backupPiecesSafeToDelete,
  planBackupRestore,
} from './backup/chain-queries.js'
import type { BackupCycle } from './backup/cycle.js'
import { type BackupCycleOptions, defaultStagingDir } from './backup/cycle-options.js'
import type { BackupCycleStatus } from './backup/cycle-status.js'
import type { BackupDestination } from './backup/destination.js'
import type { BackupRunReport, BackupToDestinationOptions } from './backup/report.js'
import { startCopyWithoutHoldingWriter } from './backup/start-guard.js'
import type { BackupVerifyResult } from './backup/verify.js'
import type { BackupEngine, DriverCapabilities, SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BackupScheduleOptions } from './types.js'

type RunExclusive = (op: () => Promise<void>) => Promise<void>

/** Where one database's backups are stored, and what a restore of that database reads.
 * @public
 */
export interface BackupChainLocation {
  /** The destination its pieces and its chain records go to. */
  destination: BackupDestination
  /** Name that destination lists its chains under. */
  chainName: string
  /** Directory the cycle stages each capture in before that capture goes out. */
  stagingDir: string
  /** Deadline the operator set on every call to that destination, where they set one. */
  destinationTimeoutMs?: number
}

/**
 * Refuses a database whose change log no cycle could ever read. The refusal
 * comes at open, before any connection exists, so nobody discovers it when the
 * first capture runs hours later.
 *
 * @param driver - Driver the database opens through.
 * @param id - Identifier it opens under.
 * @param path - File it opens.
 * @param walMode - Whether it opens in write-ahead logging mode.
 *
 * @internal
 */
export function assertChangeLogCaptureSupported(
  driver: SQLiteDriver,
  id: string,
  path: string,
  walMode: boolean,
): void {
  if (!driver.createBackupEngine) {
    throw new SirannonError(
      `Database '${id}' asks for backups and this driver provides no backup engine`,
      'BACKUP_UNSUPPORTED',
    )
  }
  if (!walMode || path === ':memory:') {
    throw new SirannonError(
      `Database '${id}' captures its change log from SQLite's write-ahead log, which needs a database file in write-ahead logging mode`,
      'BACKUP_UNSUPPORTED',
    )
  }
}

export class DatabaseBackupController {
  private readonly cancellers: (() => void)[] = []
  private cycle: BackupCycle | null = null
  private cycleOptions: BackupCycleOptions | null = null
  private cycleStarted: Promise<void> = Promise.resolve()

  constructor(
    private readonly runExclusive: RunExclusive,
    private readonly acquireWriter: () => SQLiteConnection,
    private readonly driverCapabilities: DriverCapabilities,
    private readonly databaseId: string,
    private readonly sourcePath: string,
    private readonly engine?: BackupEngine,
  ) {}

  private require(): BackupEngine {
    if (!this.engine) {
      throw new SirannonError(
        'Backups need a driver that can write files; this driver does not provide one',
        'BACKUP_UNSUPPORTED',
      )
    }
    return this.engine
  }

  private requireCycle(): BackupCycle {
    if (!this.cycle) {
      throw new SirannonError(
        `Database '${this.databaseId}' opened without the backups option, so it captures no change log and extends no chain`,
        'BACKUP_UNSUPPORTED',
      )
    }
    return this.cycle
  }

  capabilities(): BackupCapabilities {
    return describeBackupCapabilities(
      this.driverCapabilities,
      this.engine !== undefined,
      this.engine?.streamsToDestination() ?? false,
    )
  }

  backup(destPath: string): Promise<void> {
    const engine = this.require()
    return startCopyWithoutHoldingWriter(this.runExclusive, onFirstStep =>
      engine.backup(this.acquireWriter(), destPath, onFirstStep),
    )
  }

  backupTo(options: BackupToDestinationOptions): Promise<BackupRunReport> {
    const engine = this.require()
    return startCopyWithoutHoldingWriter(this.runExclusive, onFirstStep =>
      engine.copyToDestination(this.acquireWriter(), {
        ...options,
        databaseId: this.databaseId,
        sourcePath: this.sourcePath,
        onFirstStep,
      }),
    )
  }

  schedule(options: BackupScheduleOptions): void {
    const engine = this.require()
    this.cancellers.push(engine.schedule(this.acquireWriter(), options, this.runExclusive))
  }

  /**
   * Builds the cycle and starts it in the background. Its first turn copies the
   * whole database, and an open cannot wait on that. A failure reaches the
   * caller through the cycle's own error callback.
   */
  startCycle(options: BackupCycleOptions): void {
    const engine = this.require()
    this.cycleOptions = options
    this.cycle = engine.createCycle({
      ...options,
      databaseId: this.databaseId,
      sourcePath: this.sourcePath,
      runExclusive: this.runExclusive,
      acquireWriter: this.acquireWriter,
      fullCopy: copyOptions => this.backupTo(copyOptions),
    })
    this.cycleStarted = this.cycle.start().catch(err => {
      if (!options.onError) return
      try {
        options.onError(err instanceof Error ? err : new SirannonError(String(err), 'BACKUP_ERROR'))
      } catch {}
    })
  }

  captureChanges(): Promise<BackupRunReport | undefined> {
    const cycle = this.requireCycle()
    return this.cycleStarted.then(() => cycle.runOnce())
  }

  chains(): Promise<BackupChain[]> {
    return this.requireCycle().chains()
  }

  status(): BackupCycleStatus {
    return this.requireCycle().status()
  }

  location(): BackupChainLocation {
    const options = this.cycleOptions
    if (!options) {
      throw new SirannonError(
        `Database '${this.databaseId}' opened without the backups option, so it captures no change log and extends no chain`,
        'BACKUP_UNSUPPORTED',
      )
    }
    return {
      destination: options.destination,
      chainName: options.chainName ?? DEFAULT_CHAIN_NAME,
      stagingDir: options.stagingDir ?? defaultStagingDir(this.sourcePath),
      ...(options.destinationTimeoutMs === undefined ? {} : { destinationTimeoutMs: options.destinationTimeoutMs }),
    }
  }

  verify(name: string): Promise<BackupVerifyResult> {
    return this.requireCycle().verify(name)
  }

  async restorePlan(moment: number): Promise<BackupRestorePlan> {
    return planBackupRestore(await this.chains(), moment)
  }

  async piecesSafeToDelete(options?: BackupSafeToDeleteOptions): Promise<BackupChainRecord[]> {
    return backupPiecesSafeToDelete(await this.chains(), options)
  }

  /** Stops the cycle, capturing the log one final time so nothing written since the previous turn is lost. */
  async stopCycle(): Promise<void> {
    if (!this.cycle) return
    const cycle = this.cycle
    await this.cycleStarted
    await cycle.stop()
  }

  cancelAll(): void {
    for (const cancel of this.cancellers) {
      try {
        cancel()
      } catch {}
    }
    this.cancellers.length = 0
  }
}
