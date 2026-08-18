import { readdirSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { BackupScheduler } from '../../backup/scheduler.js'
import { BackupError } from '../../errors.js'
import { testDriver } from '../helpers/test-driver.js'
import { countingManager, createTestDb, settleUntil, tempDirPerTest, useCronTimers } from './shared.js'

const temp = tempDirPerTest()

describe('BackupScheduler', () => {
  it('fires a backup on cron schedule', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'scheduled')
      const counting = countingManager()
      const scheduler = new BackupScheduler(counting.manager)

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => counting.completed() >= 1)
      cancel()

      const files = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(files.length).toBeGreaterThanOrEqual(1)

      const backupPath = join(backupDir, files[0])
      const backupConn = await testDriver.open(backupPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users')
      const rows = (await stmt.all()) as { name: string }[]
      expect(rows).toHaveLength(2)
      await backupConn.close()
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('rotates files according to maxFiles', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'rotated')
      const counting = countingManager()
      const scheduler = new BackupScheduler(counting.manager)

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 2,
      })

      await vi.advanceTimersByTimeAsync(4500)
      await settleUntil(() => counting.completed() >= 3)
      cancel()

      const files = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(files.length).toBeGreaterThanOrEqual(1)
      expect(files.length).toBeLessThanOrEqual(2)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('cancel function stops future backups', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'cancelled')
      const counting = countingManager()
      const scheduler = new BackupScheduler(counting.manager)

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => counting.completed() >= 1)
      cancel()

      const countAfterCancel = readdirSync(backupDir).filter(f => f.endsWith('.db')).length
      expect(countAfterCancel).toBeGreaterThanOrEqual(1)

      await vi.advanceTimersByTimeAsync(3000)
      await settleUntil(() => false, 50)
      const countLater = readdirSync(backupDir).filter(f => f.endsWith('.db')).length
      expect(countLater).toBe(countAfterCancel)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('defaults maxFiles to 5 when not specified', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'defaults')

      let observedMaxFiles: number | undefined
      const customManager = new BackupManager()
      const originalRotate = customManager.rotate.bind(customManager)
      customManager.rotate = (dir: string, maxFiles: number) => {
        observedMaxFiles = maxFiles
        return originalRotate(dir, maxFiles)
      }

      const scheduler = new BackupScheduler(customManager)
      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => observedMaxFiles !== undefined)
      cancel()

      expect(observedMaxFiles).toBe(5)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('calls onError when a scheduled backup fails', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      await conn.close()

      const backupDir = join(temp.path, 'error-reporting')
      const errors: Error[] = []
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 5,
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => errors.length >= 1)
      cancel()

      expect(errors.length).toBeGreaterThanOrEqual(1)
      expect(errors[0]).toBeInstanceOf(BackupError)
      expect((errors[0] as BackupError).code).toBe('BACKUP_ERROR')
    } finally {
      vi.useRealTimers()
    }
  })

  it('silently discards errors when onError is not provided', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      await conn.close()

      const backupDir = join(temp.path, 'silent-errors')
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 5,
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => false, 50)
      cancel()

      const files = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(files).toHaveLength(0)
    } finally {
      vi.useRealTimers()
    }
  })
})
