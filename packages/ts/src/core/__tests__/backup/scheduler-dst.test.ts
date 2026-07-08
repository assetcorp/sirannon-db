import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { type CronParts, wallClockParts } from '../../backup/cron.js'
import { BackupScheduler } from '../../backup/scheduler.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { useTempDir } from './shared.js'

const temp = useTempDir()
const STUB_CONN = {} as unknown as SQLiteConnection

let runCounter = 0

interface Fire {
  epoch: number
  parts: CronParts
}

interface Scenario {
  cron: string
  timezone?: string
  startUtc: string
  advanceMs: number
}

async function collectFires(scenario: Scenario): Promise<Fire[]> {
  const fires: Fire[] = []
  const manager = new BackupManager()
  manager.backup = async () => {
    fires.push({ epoch: Date.now(), parts: wallClockParts(new Date(), scenario.timezone) })
  }
  manager.rotate = () => {}
  const scheduler = new BackupScheduler(manager)

  vi.useFakeTimers()
  vi.setSystemTime(new Date(scenario.startUtc))
  try {
    runCounter += 1
    const cancel = scheduler.schedule(STUB_CONN, {
      cron: scenario.cron,
      destDir: join(temp.path, `run-${runCounter}`),
      timezone: scenario.timezone,
    })
    await vi.advanceTimersByTimeAsync(scenario.advanceMs)
    cancel()
  } finally {
    vi.useRealTimers()
  }
  return fires
}

function labelOf(parts: CronParts): string {
  return `${String(parts.hour).padStart(2, '0')}:${String(parts.minute).padStart(2, '0')}`
}

function labels(fires: Fire[]): string[] {
  return fires.map(fire => labelOf(fire.parts))
}

describe('BackupScheduler across daylight-saving transitions', () => {
  it('fires once per label through a one-hour fall-back (northern hemisphere)', async () => {
    const fires = await collectFires({
      cron: '0,30 1 * * *',
      timezone: 'America/New_York',
      startUtc: '2023-11-05T04:59:00Z',
      advanceMs: 96 * 60 * 1000,
    })
    expect(labels(fires).sort()).toEqual(['01:00', '01:30'])
    expect(new Set(labels(fires)).size).toBe(fires.length)
  })

  it('skips a job inside a one-hour spring-forward gap (northern hemisphere)', async () => {
    const fires = await collectFires({
      cron: '0,30 2 * * *',
      timezone: 'America/New_York',
      startUtc: '2023-03-12T06:29:00Z',
      advanceMs: 66 * 60 * 1000,
    })
    expect(fires).toHaveLength(0)
  })

  it('fires once per label through a 30-minute fall-back (Lord Howe Island)', async () => {
    const fires = await collectFires({
      cron: '30,45 1 * * *',
      timezone: 'Australia/Lord_Howe',
      startUtc: '2023-04-01T14:29:00Z',
      advanceMs: 51 * 60 * 1000,
    })
    expect(labels(fires).sort()).toEqual(['01:30', '01:45'])
    expect(new Set(labels(fires)).size).toBe(fires.length)
  })

  it('skips a job inside a 30-minute spring-forward gap (Lord Howe Island)', async () => {
    const fires = await collectFires({
      cron: '0,15 2 * * *',
      timezone: 'Australia/Lord_Howe',
      startUtc: '2023-09-30T15:00:00Z',
      advanceMs: 90 * 60 * 1000,
    })
    expect(fires).toHaveLength(0)
  })

  it('fires once per label through a southern-hemisphere fall-back (Sydney)', async () => {
    const fires = await collectFires({
      cron: '0,30 2 * * *',
      timezone: 'Australia/Sydney',
      startUtc: '2023-04-01T14:59:00Z',
      advanceMs: 96 * 60 * 1000,
    })
    expect(labels(fires).sort()).toEqual(['02:00', '02:30'])
    expect(new Set(labels(fires)).size).toBe(fires.length)
  })

  it('skips a job inside a southern-hemisphere spring-forward gap (Sydney)', async () => {
    const fires = await collectFires({
      cron: '0,30 2 * * *',
      timezone: 'Australia/Sydney',
      startUtc: '2023-09-30T15:59:00Z',
      advanceMs: 66 * 60 * 1000,
    })
    expect(fires).toHaveLength(0)
  })
})

describe('BackupScheduler timezone evaluation', () => {
  it('fires at the local wall-clock time of a half-hour-offset zone (Kolkata)', async () => {
    const fires = await collectFires({
      cron: '0 9 * * *',
      timezone: 'Asia/Kolkata',
      startUtc: '2023-06-01T03:29:00Z',
      advanceMs: 3 * 60 * 1000,
    })
    expect(labels(fires)).toEqual(['09:00'])
  })

  it('fires at the configured UTC time', async () => {
    const fires = await collectFires({
      cron: '0 12 * * *',
      timezone: 'UTC',
      startUtc: '2023-06-01T11:59:00Z',
      advanceMs: 3 * 60 * 1000,
    })
    expect(labels(fires)).toEqual(['12:00'])
  })

  it('honours the zone rather than a UTC interpretation of the same expression', async () => {
    const fires = await collectFires({
      cron: '0 12 * * *',
      timezone: 'America/New_York',
      startUtc: '2023-07-03T15:59:00Z',
      advanceMs: 3 * 60 * 1000,
    })
    expect(labels(fires)).toEqual(['12:00'])
    expect(fires[0].epoch).toBe(new Date('2023-07-03T16:00:00Z').getTime())
  })
})
