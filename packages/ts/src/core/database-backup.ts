import { BackupManager } from './backup/backup.js'
import { BackupScheduler } from './backup/scheduler.js'
import type { SQLiteConnection } from './driver/types.js'
import type { BackupScheduleOptions } from './types.js'

type RunExclusive = (op: () => Promise<void>) => Promise<void>

export class DatabaseBackupController {
  private readonly manager = new BackupManager()
  private readonly scheduler = new BackupScheduler(this.manager)
  private readonly cancellers: (() => void)[] = []

  constructor(
    private readonly runExclusive: RunExclusive,
    private readonly acquireWriter: () => SQLiteConnection,
  ) {}

  backup(destPath: string): Promise<void> {
    return this.runExclusive(() => this.manager.backup(this.acquireWriter(), destPath))
  }

  schedule(options: BackupScheduleOptions): void {
    this.cancellers.push(this.scheduler.schedule(this.acquireWriter(), options, this.runExclusive))
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
