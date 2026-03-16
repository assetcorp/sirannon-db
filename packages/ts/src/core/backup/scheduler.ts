import { existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { Cron } from 'croner'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError } from '../errors.js'
import type { BackupScheduleOptions } from '../types.js'
import { BackupManager } from './backup.js'

const DEFAULT_MAX_FILES = 5

export class BackupScheduler {
  private readonly manager: BackupManager

  constructor(manager?: BackupManager) {
    this.manager = manager ?? new BackupManager()
  }

  schedule(conn: SQLiteConnection, options: BackupScheduleOptions): () => void {
    const { cron: cronExpr, destDir, maxFiles = DEFAULT_MAX_FILES, onError } = options
    const resolvedDir = resolve(destDir)

    if (!existsSync(resolvedDir)) {
      try {
        mkdirSync(resolvedDir, { recursive: true })
      } catch (err) {
        throw new BackupError(
          `Failed to create backup directory '${destDir}': ${err instanceof Error ? err.message : String(err)}`,
        )
      }
    }

    let job: Cron
    try {
      job = new Cron(
        cronExpr,
        {
          unref: true,
          catch: (err: unknown) => {
            if (onError) {
              const error =
                err instanceof Error ? err : new BackupError(typeof err === 'string' ? err : 'Scheduled backup failed')
              onError(error)
            }
          },
        },
        async () => {
          const filename = this.manager.generateFilename()
          const destPath = join(resolvedDir, filename)
          await this.manager.backup(conn, destPath)
          this.manager.rotate(resolvedDir, maxFiles)
        },
      )
    } catch (err) {
      throw new BackupError(
        `Invalid cron expression '${cronExpr}': ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    return () => {
      job.stop()
    }
  }
}
