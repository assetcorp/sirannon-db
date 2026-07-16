import type { BackupEngine, SQLiteConnection } from './driver/types.js'
import { SirannonError } from './errors.js'
import type { BackupScheduleOptions } from './types.js'

type RunExclusive = (op: () => Promise<void>) => Promise<void>

export class DatabaseBackupController {
  private readonly cancellers: (() => void)[] = []

  constructor(
    private readonly runExclusive: RunExclusive,
    private readonly acquireWriter: () => SQLiteConnection,
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

  backup(destPath: string): Promise<void> {
    const engine = this.require()
    return this.runExclusive(() => engine.backup(this.acquireWriter(), destPath))
  }

  schedule(options: BackupScheduleOptions): void {
    const engine = this.require()
    this.cancellers.push(engine.schedule(this.acquireWriter(), options, this.runExclusive))
  }

  cancelAll(): void {
    for (const cancel of this.cancellers) {
      try {
        cancel()
      } catch {
        /* best-effort */
      }
    }
    this.cancellers.length = 0
  }
}
