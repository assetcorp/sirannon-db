import { type BackupCapabilities, describeBackupCapabilities } from './backup/capabilities.js'
import type { BackupRunReport, BackupToDestinationOptions } from './backup/report.js'
import { startCopyWithoutHoldingWriter } from './backup/start-guard.js'
import type { BackupEngine, DriverCapabilities, SQLiteConnection } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BackupScheduleOptions } from './types.js'

type RunExclusive = (op: () => Promise<void>) => Promise<void>

export class DatabaseBackupController {
  private readonly cancellers: (() => void)[] = []

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

  capabilities(): BackupCapabilities {
    return describeBackupCapabilities(this.driverCapabilities, this.engine !== undefined)
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

  cancelAll(): void {
    for (const cancel of this.cancellers) {
      try {
        cancel()
      } catch {}
    }
    this.cancellers.length = 0
  }
}
