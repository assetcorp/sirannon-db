import { existsSync, mkdirSync } from 'node:fs'
import { basename, extname, join, resolve } from 'node:path'
import { invokeCallerCallback } from '../caller-callbacks.js'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError, SirannonError } from '../errors.js'
import { BackupManager } from './backup.js'
import { assertValidTimeZone, type CronExpression, type CronParts, parseCron, wallClockParts } from './cron.js'
import { assertDeadline, withinDeadline } from './destination-deadline.js'
import { type BackupFileCopy, type BackupFileReport, readMainDatabasePath } from './report.js'
import type { BackupScheduleRequest, RunExclusive } from './schedule-request.js'
import { startCopyWithoutHoldingWriter } from './start-guard.js'

const DEFAULT_MAX_FILES = 5
const MINUTE_RESOLUTION_MS = 60_000
const SECOND_RESOLUTION_MS = 1_000
const DST_LOOKBACK_MS = 3 * 60 * 60 * 1000
const DEFAULT_ON_BACKUP_TIMEOUT_MS = 600_000

interface BackupSource {
  databaseId: string
  sourcePath: string
}

interface ResolvedSchedule extends Omit<BackupScheduleRequest, 'cron' | 'onBackupTimeoutMs'> {
  cron: CronExpression
  resolvedDir: string
  maxFiles: number
  onBackupTimeoutMs: number
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

function runDirect(op: () => Promise<void>): Promise<void> {
  return op()
}

/**
 * Repeats a database backup on a cron schedule and keeps a bounded number of files.
 *
 * @public
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
   * @param request - Cron expression, destination directory, retention, and time zone, along with the callbacks and the database the copies come from.
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

    const onBackupTimeoutMs = request.onBackupTimeoutMs ?? DEFAULT_ON_BACKUP_TIMEOUT_MS
    assertDeadline(onBackupTimeoutMs, 'onBackupTimeoutMs')

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

    return this.run(conn, { ...request, cron, resolvedDir, maxFiles, timezone, onBackupTimeoutMs })
  }

  /**
   * Works out the database and the file every report names. A caller may supply
   * either of them, and Sirannon reads the file SQLite has open where the caller
   * named none. Sirannon puts that question to the connection that writes, so it
   * asks with nothing else holding the writer.
   *
   * @param conn - Connection the copies come from.
   * @param run - The schedule, which holds whichever of the two the caller named.
   * @param runExclusive - Runs the question with nothing else holding the writer.
   * @returns The database identifier and the path of the file the copies come from.
   * @throws A `BACKUP_ERROR` where the caller named no source file and SQLite has none open.
   */
  private async namesFor(
    conn: SQLiteConnection,
    run: ResolvedSchedule,
    runExclusive: RunExclusive,
  ): Promise<BackupSource> {
    if (run.databaseId !== undefined && run.sourcePath !== undefined) {
      return { databaseId: run.databaseId, sourcePath: run.sourcePath }
    }
    let sourcePath = run.sourcePath ?? ''
    if (sourcePath === '') {
      await runExclusive(async () => {
        sourcePath = await readMainDatabasePath(conn)
      })
    }
    if (sourcePath === '') {
      throw new BackupError(
        'This schedule names no source file, and SQLite reports no file open on the connection it copies, so no report could name one. Give the schedule a sourcePath.',
      )
    }
    return { databaseId: run.databaseId ?? basename(sourcePath, extname(sourcePath)), sourcePath }
  }

  /**
   * Passes one finished copy to the caller's completion callback, and stops
   * waiting on that callback once its deadline passes.
   *
   * @param report - What the copy produced.
   * @param run - The schedule, which holds the callback and its deadline.
   */
  private async handToCaller(report: BackupFileReport, run: ResolvedSchedule): Promise<void> {
    const { onBackup, onBackupTimeoutMs } = run
    if (!onBackup) return
    const handed = Promise.resolve(onBackup(report))
    if (onBackupTimeoutMs === 0) {
      await handed
      return
    }
    await withinDeadline(
      handed,
      onBackupTimeoutMs,
      () =>
        new SirannonError(
          `The onBackup callback for database '${report.databaseId}' did not return within ${onBackupTimeoutMs}ms, so the schedule went on without it. The copy at '${report.destPath}' is on disk and whatever that callback was doing with it has not finished.`,
          'BACKUP_ERROR',
        ),
    )
  }

  private run(conn: SQLiteConnection, run: ResolvedSchedule): () => void {
    const { cron, resolvedDir, maxFiles, timezone, onError } = run
    const runExclusive = run.runExclusive ?? runDirect
    const tickMs = cron.hasSeconds ? SECOND_RESOLUTION_MS : MINUTE_RESOLUTION_MS
    let timer: ReturnType<typeof setTimeout> | null = null
    let stopped = false
    let running = false
    let tickState = INITIAL_TICK_STATE

    const announce = (err: unknown): void => {
      if (!onError) return
      invokeCallerCallback(() => onError(toError(err)))
    }

    let source: BackupSource | null = null

    const runBackup = async (): Promise<void> => {
      let copy: BackupFileCopy | null = null
      try {
        const names = source ?? (await this.namesFor(conn, run, runExclusive))
        source = names
        const destPath = join(resolvedDir, this.manager.generateFilename())
        copy = await startCopyWithoutHoldingWriter(runExclusive, onFirstStep =>
          this.manager.backup(conn, destPath, onFirstStep),
        )
        await this.handToCaller({ ...copy, ...names }, run)
      } catch (err) {
        announce(err)
      }
      if (copy) {
        try {
          this.manager.rotate(resolvedDir, maxFiles)
        } catch (err) {
          announce(err)
        }
      }
      running = false
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
