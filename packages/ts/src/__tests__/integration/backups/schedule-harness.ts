import { vi } from 'vitest'
import type { BackupFileReport } from '../../../core/backup/report.js'
import type { Database } from '../../../core/database.js'
import type { BackupScheduleOptions } from '../../../core/types.js'

const QUIET_TURNS_PER_STEP = 100
const COPY_SETTLE_LIMIT_MS = 10_000
const SECOND_MS = 1_000
const MINUTE_MS = 60_000

export function startScheduleClock(startIso?: string): void {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'] })
  if (startIso) vi.setSystemTime(new Date(startIso))
}

function fileRequestPending(): boolean {
  return process.getActiveResourcesInfo().some(resource => resource.startsWith('FSReq'))
}

export async function letCopiesFinish(): Promise<void> {
  const deadline = performance.now() + COPY_SETTLE_LIMIT_MS
  let quietTurns = 0
  while (quietTurns < QUIET_TURNS_PER_STEP && performance.now() < deadline) {
    await new Promise(settle => setImmediate(settle))
    quietTurns = fileRequestPending() ? 0 : quietTurns + 1
  }
}

async function advanceInSteps(totalMs: number, stepMs: number): Promise<void> {
  for (let elapsed = 0; elapsed < totalMs; elapsed += stepMs) {
    await vi.advanceTimersByTimeAsync(Math.min(stepMs, totalMs - elapsed))
    await letCopiesFinish()
  }
}

export function advanceSeconds(seconds: number): Promise<void> {
  return advanceInSteps(seconds * SECOND_MS, SECOND_MS)
}

export function advanceMinutes(minutes: number): Promise<void> {
  return advanceInSteps(minutes * MINUTE_MS, MINUTE_MS)
}

export interface Schedule {
  reports: BackupFileReport[]
  errors: (Error & { code?: string })[]
}

export function schedule(db: Database, options: Partial<BackupScheduleOptions> & { destDir: string }): Schedule {
  const reports: BackupFileReport[] = []
  const errors: (Error & { code?: string })[] = []
  db.scheduleBackup({
    cron: '* * * * * *',
    maxFiles: 100,
    onBackup: report => {
      reports.push(report)
    },
    onError: err => errors.push(err),
    ...options,
  })
  return { reports, errors }
}

export function wallClock(epochMs: number, timeZone: string): string {
  return new Intl.DateTimeFormat('en-GB', { timeZone, hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).format(
    new Date(epochMs),
  )
}
