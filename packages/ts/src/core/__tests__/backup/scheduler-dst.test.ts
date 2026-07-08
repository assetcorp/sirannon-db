import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { BackupScheduler } from '../../backup/scheduler.js'
import { createTestDb, useTempDir } from './shared.js'

const temp = useTempDir()

function countingScheduler(): { scheduler: BackupScheduler; getCount: () => number } {
  const manager = new BackupManager()
  let count = 0
  manager.backup = async () => {
    count += 1
  }
  manager.rotate = () => {}
  return { scheduler: new BackupScheduler(manager), getCount: () => count }
}

describe('BackupScheduler daylight-saving behaviour', () => {
  it('fires once at the first occurrence during a fall-back overlap', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2023-11-05T04:59:00Z'))
    try {
      const conn = await createTestDb(temp.path)
      const { scheduler, getCount } = countingScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '*/30 1 * * *',
        destDir: `${temp.path}/fall-back`,
        timezone: 'America/New_York',
      })

      await vi.advanceTimersByTimeAsync(5_700_000)
      cancel()

      expect(getCount()).toBe(2)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('skips a job scheduled inside the spring-forward gap', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2023-03-12T06:30:00Z'))
    try {
      const conn = await createTestDb(temp.path)
      const { scheduler, getCount } = countingScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '*/30 2 * * *',
        destDir: `${temp.path}/spring-forward`,
        timezone: 'America/New_York',
      })

      await vi.advanceTimersByTimeAsync(3_900_000)
      cancel()

      expect(getCount()).toBe(0)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('fires at the local wall-clock time of the configured timezone', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2023-07-03T15:59:00Z'))
    try {
      const conn = await createTestDb(temp.path)
      const { scheduler, getCount } = countingScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '0 12 * * *',
        destDir: `${temp.path}/tz-noon`,
        timezone: 'America/New_York',
      })

      await vi.advanceTimersByTimeAsync(180_000)
      cancel()

      expect(getCount()).toBe(1)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })
})
