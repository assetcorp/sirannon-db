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
import type { BackupFileReport, BackupRunReport, BackupToDestinationOptions } from './backup/report.js'
import { startCopyWithoutHoldingWriter } from './backup/start-guard.js'
import type { BackupVerifyResult } from './backup/verify.js'
import { invokeCallerCallback } from './caller-callbacks.js'
import type { BackupEngine, DriverCapabilities, SQLiteConnection, SQLiteDriver } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BackupScheduleOptions } from './types.js'

type RunExclusive = (op: () => Promise<void>) => Promise<void>

/** The destination and chain name under which Sirannon stores one database's backups, with the directory where it stages each capture.
 * @public
 */
export interface BackupChainLocation {
  /** The destination that Sirannon sends the backup pieces and chain records to. */
  destination: BackupDestination
  /** The name that Sirannon lists this database's chains under at the destination. */
  chainName: string
  /** The directory where the backup cycle writes each capture before it sends that capture to the destination. */
  stagingDir: string
  /** The deadline, in milliseconds, that the operator set on each call to the destination, when the operator set one. */
  destinationTimeoutMs?: number
}

/**
 * Throws `BACKUP_UNSUPPORTED` when the backup cycle cannot capture this
 * database's change log, because the driver has no backup engine, the database
 * is in memory, or write-ahead logging is off. Sirannon calls this before it
 * opens any connection, so that the operator sees the error at open and not at
 * the first capture.
 *
 * @param driver - The driver that opens the database.
 * @param id - The database identifier, which the error message names.
 * @param path - The path of the database file, or `':memory:'`.
 * @param walMode - Whether the database uses write-ahead logging.
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

  async backup(destPath: string): Promise<BackupFileReport> {
    const engine = this.require()
    const copy = await startCopyWithoutHoldingWriter(this.runExclusive, onFirstStep =>
      engine.backup(this.acquireWriter(), destPath, onFirstStep),
    )
    return { ...copy, databaseId: this.databaseId, sourcePath: this.sourcePath }
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
    this.cancellers.push(
      engine.schedule(this.acquireWriter(), {
        ...options,
        databaseId: this.databaseId,
        sourcePath: this.sourcePath,
        runExclusive: this.runExclusive,
      }),
    )
  }

  /**
   * Builds the backup cycle and starts it without awaiting the first turn, which
   * copies the whole database, so that the open can return first. When the start
   * fails, this method passes the error to the `onError` callback in the cycle
   * options.
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
      const onError = options.onError
      if (!onError) return
      invokeCallerCallback(() => onError(err instanceof Error ? err : new SirannonError(String(err), 'BACKUP_ERROR')))
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

  /** Stops the backup cycle after one last capture, which sends the writes made since the previous turn to the destination. */
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
