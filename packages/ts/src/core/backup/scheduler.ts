import { existsSync, mkdirSync } from 'node:fs'
import { join, resolve } from 'node:path'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError, SirannonError } from '../errors.js'
import type { BackupScheduleOptions } from '../types.js'
import { BackupManager } from './backup.js'
import { assertValidTimeZone, type CronExpression, type CronParts, parseCron, wallClockParts } from './cron.js'
import { assertDeadline, withinDeadline } from './destination-deadline.js'
import { startCopyWithoutHoldingWriter } from './start-guard.js'

const DEFAULT_MAX_FILES = 5
const MINUTE_RESOLUTION_MS = 60_000
const SECOND_RESOLUTION_MS = 1_000
const DST_LOOKBACK_MS = 3 * 60 * 60 * 1000
const DEFAULT_ON_BACKUP_TIMEOUT_MS = 600_000

/**
 * What one repeating backup needs beyond the caller's own options, so that
 * every report it produces names the database the copy came from.
 *
 * @public
 */
export interface BackupScheduleRequest extends BackupScheduleOptions {
  /** Database the copies are taken from. */
  databaseId: string
  /** File they are taken from. */
  sourcePath: string
  /** Runs each copy with nothing else holding the writer. */
  runExclusive?: RunExclusive
}

interface ResolvedSchedule extends Omit<BackupScheduleRequest, 'cron'> {
  cron: CronExpression
  resolvedDir: string
  maxFiles: number
}

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

function zoneOffsetMs(date: Date, timeZone: string | undefined): number {
  const parts = wallClockParts(date, timeZone)
  const wallAsUtc = Date.UTC(parts.year, parts.month - 1, parts.dayOfMonth, parts.hour, parts.minute, parts.second)
  return wallAsUtc - Math.floor(date.getTime() / 1000) * 1000
}

function isRepeatedWallClock(
  now: Date,
  nowParts: CronParts,
  timeZone: string | undefined,
  hasSeconds: boolean,
): boolean {
  const offsetNow = zoneOffsetMs(now, timeZone)
  const offsetBefore = zoneOffsetMs(new Date(now.getTime() - DST_LOOKBACK_MS), timeZone)
  if (offsetNow >= offsetBefore) {
    return false
  }
  const shift = offsetBefore - offsetNow
  const firstOccurrence = wallClockParts(new Date(now.getTime() - shift), timeZone)
  return slotKey(firstOccurrence, hasSeconds) === slotKey(nowParts, hasSeconds)
}

export interface TickState {
  readonly lastFiredSlot: string
  readonly lastFiredEpoch: number
}

export const INITIAL_TICK_STATE: TickState = {
  lastFiredSlot: '',
  lastFiredEpoch: Number.NEGATIVE_INFINITY,
}

export function evaluateTick(
  now: Date,
  cron: CronExpression,
  timeZone: string | undefined,
  state: TickState,
): { shouldFire: boolean; nextState: TickState } {
  const nowEpoch = now.getTime()
  if (nowEpoch <= state.lastFiredEpoch) {
    return { shouldFire: false, nextState: state }
  }

  const parts = wallClockParts(now, timeZone)
  if (!cron.matches(parts)) {
    return { shouldFire: false, nextState: state }
  }

  const slot = slotKey(parts, cron.hasSeconds)
  if (slot === state.lastFiredSlot) {
    return { shouldFire: false, nextState: state }
  }
  if (isRepeatedWallClock(now, parts, timeZone, cron.hasSeconds)) {
    return { shouldFire: false, nextState: state }
  }

  return { shouldFire: true, nextState: { lastFiredSlot: slot, lastFiredEpoch: nowEpoch } }
}

/** Runs a backup while the caller's writer lock is held, so it never shares the writer connection with another write. */
export type RunExclusive = (op: () => Promise<void>) => Promise<void>

function runDirect(op: () => Promise<void>): Promise<void> {
  return op()
}

/**
 * @public
 *
 * Repeats a database backup on a cron schedule and keeps a bounded number of files.
 */
export class BackupScheduler {
  private readonly manager: BackupManager

  constructor(manager?: BackupManager) {
    this.manager = manager ?? new BackupManager()
  }

  /**
   * Starts repeating backups and returns a function that stops them.
   *
   * @param conn - Connection to the database being copied.
   * @param request - Cron expression, destination directory, retention, time zone, callbacks, and the database the copies come from.
   * @returns A function that stops the schedule.
   */
  schedule(conn: SQLiteConnection, request: BackupScheduleRequest): () => void {
    const { cron: cronExpr, destDir, maxFiles = DEFAULT_MAX_FILES, timezone } = request

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

    assertDeadline(request.onBackupTimeoutMs ?? DEFAULT_ON_BACKUP_TIMEOUT_MS, 'onBackupTimeoutMs')

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

    return this.run(conn, { ...request, cron, resolvedDir, maxFiles, timezone })
  }

  private run(conn: SQLiteConnection, run: ResolvedSchedule): () => void {
    const { cron, resolvedDir, maxFiles, timezone, onBackup, onError } = run
    const runExclusive = run.runExclusive ?? runDirect
    const tickMs = cron.hasSeconds ? SECOND_RESOLUTION_MS : MINUTE_RESOLUTION_MS
    let timer: ReturnType<typeof setTimeout> | null = null
    let stopped = false
    let running = false
    let tickState = INITIAL_TICK_STATE

    const runBackup = async (): Promise<void> => {
      try {
        const destPath = join(resolvedDir, this.manager.generateFilename())
        const copy = await startCopyWithoutHoldingWriter(runExclusive, onFirstStep =>
          this.manager.backup(conn, destPath, onFirstStep),
        )
        this.manager.rotate(resolvedDir, maxFiles)
        if (onBackup) {
          const report = { ...copy, databaseId: run.databaseId, sourcePath: run.sourcePath }
          const timeoutMs = run.onBackupTimeoutMs ?? DEFAULT_ON_BACKUP_TIMEOUT_MS
          const handed = Promise.resolve(onBackup(report))
          await (timeoutMs > 0
            ? withinDeadline(
                handed,
                timeoutMs,
                () =>
                  new SirannonError(
                    `The onBackup callback for database '${run.databaseId}' did not return within ${timeoutMs}ms, so the schedule went on without it. The copy at '${report.destPath}' is on disk and whatever that callback was doing with it has not finished.`,
                    'BACKUP_ERROR',
                  ),
              )
            : handed)
        }
      } catch (err) {
        if (onError) {
          try {
            onError(toError(err))
          } catch {}
        }
      } finally {
        running = false
      }
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
      if (!running) {
        const { shouldFire, nextState } = evaluateTick(new Date(), cron, timezone, tickState)
        if (shouldFire) {
          tickState = nextState
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
