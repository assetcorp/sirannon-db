import { existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError } from '../errors.js'
import type { BackupScheduleOptions } from '../types.js'
import { BackupManager } from './backup.js'
import { assertValidTimeZone, type CronExpression, type CronParts, parseCron, wallClockParts } from './cron.js'

const DEFAULT_MAX_FILES = 5
const MINUTE_RESOLUTION_MS = 60_000
const SECOND_RESOLUTION_MS = 1_000
const ONE_HOUR_MS = 3_600_000

function toError(value: unknown): Error {
  if (value instanceof Error) {
    return value
  }
  return new BackupError(typeof value === 'string' ? value : 'Scheduled backup failed')
}

function slotKey(parts: CronParts, hasSeconds: boolean): string {
  const base = `${parts.year}-${parts.month}-${parts.dayOfMonth}-${parts.hour}-${parts.minute}`
  return hasSeconds ? `${base}-${parts.second}` : base
}

export class BackupScheduler {
  private readonly manager: BackupManager

  constructor(manager?: BackupManager) {
    this.manager = manager ?? new BackupManager()
  }

  schedule(conn: SQLiteConnection, options: BackupScheduleOptions): () => void {
    const { cron: cronExpr, destDir, maxFiles = DEFAULT_MAX_FILES, onError, timezone } = options

    let cron: CronExpression
    try {
      cron = parseCron(cronExpr)
    } catch (err) {
      throw new BackupError(
        `Invalid cron expression '${cronExpr}': ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    if (timezone !== undefined) {
      try {
        assertValidTimeZone(timezone)
      } catch (err) {
        throw new BackupError(`Invalid timezone '${timezone}': ${err instanceof Error ? err.message : String(err)}`)
      }
    }

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

    return this.run(conn, cron, resolvedDir, maxFiles, timezone, onError)
  }

  private run(
    conn: SQLiteConnection,
    cron: CronExpression,
    resolvedDir: string,
    maxFiles: number,
    timezone: string | undefined,
    onError?: (error: Error) => void,
  ): () => void {
    const tickMs = cron.hasSeconds ? SECOND_RESOLUTION_MS : MINUTE_RESOLUTION_MS
    let timer: ReturnType<typeof setTimeout> | null = null
    let stopped = false
    let running = false
    let lastFiredSlot = ''

    const runBackup = async (): Promise<void> => {
      try {
        const destPath = join(resolvedDir, this.manager.generateFilename())
        await this.manager.backup(conn, destPath)
        this.manager.rotate(resolvedDir, maxFiles)
      } catch (err) {
        if (onError) {
          try {
            onError(toError(err))
          } catch {
            // A throwing error handler must not escalate into an unhandled rejection.
          }
        }
      } finally {
        running = false
      }
    }

    const isRepeatedWallClock = (now: Date, parts: CronParts): boolean => {
      const previous = wallClockParts(new Date(now.getTime() - ONE_HOUR_MS), timezone)
      return slotKey(previous, cron.hasSeconds) === slotKey(parts, cron.hasSeconds)
    }

    const scheduleNext = (): void => {
      if (stopped) {
        return
      }
      const delay = tickMs - (Date.now() % tickMs)
      timer = setTimeout(onTick, delay)
      timer.unref?.()
    }

    const onTick = (): void => {
      if (stopped) {
        return
      }
      const now = new Date()
      const parts = wallClockParts(now, timezone)
      if (!running && cron.matches(parts)) {
        const slot = slotKey(parts, cron.hasSeconds)
        if (slot !== lastFiredSlot && !isRepeatedWallClock(now, parts)) {
          lastFiredSlot = slot
          running = true
          void runBackup()
        }
      }
      scheduleNext()
    }

    scheduleNext()

    return () => {
      stopped = true
      if (timer) {
        clearTimeout(timer)
        timer = null
      }
    }
  }
}
