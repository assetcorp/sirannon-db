import { Cron } from 'croner'
import { existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import type Database from 'better-sqlite3'
import { BackupError } from '../errors.js'
import type { BackupScheduleOptions } from '../types.js'
import { BackupManager } from './backup.js'

type SqliteDb = InstanceType<typeof Database>

const DEFAULT_MAX_FILES = 5

export class BackupScheduler {
  private readonly manager: BackupManager

  constructor(manager?: BackupManager) {
    this.manager = manager ?? new BackupManager()
  }

  /**
   * Schedules periodic backups on a cron expression.
   *
   * Each tick creates a timestamped backup file inside {@link options.destDir}
   * and rotates old files so no more than {@link options.maxFiles} (default 5)
   * are retained.
   *
   * Provide {@link options.onError} to receive notification when a scheduled
   * backup fails. Without it, errors are silently discarded to prevent
   * unhandled exceptions from crashing the process.
   *
   * The underlying timer is unreferenced so it won't keep the Node.js
   * process alive on its own (consistent with the CDC polling timer).
   *
   * Returns a cancel function that stops the scheduled job immediately.
   */
  schedule(db: SqliteDb, options: BackupScheduleOptions): () => void {
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
                err instanceof Error
                  ? err
                  : new BackupError(typeof err === 'string' ? err : 'Scheduled backup failed')
              onError(error)
            }
          },
        },
        () => {
          const filename = this.manager.generateFilename()
          const destPath = join(resolvedDir, filename)
          this.manager.backup(db, destPath)
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
