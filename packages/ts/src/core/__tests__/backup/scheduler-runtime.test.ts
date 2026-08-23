import { readdirSync, statSync } from 'node:fs'
import { basename, join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import type { BackupFileReport } from '../../backup/report.js'
import { BackupScheduler } from '../../backup/scheduler.js'
import type { SQLiteConnection } from '../../driver/types.js'
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
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
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

  it('reports every copy it finishes, naming the file and what that copy moved', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'reported')
      const sourcePath = join(temp.path, 'source.db')
      const reports: BackupFileReport[] = []
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        databaseId: 'main',
        sourcePath,
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
        onBackup: report => {
          reports.push(report)
        },
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => reports.length >= 1)
      cancel()

      const report = reports[0]
      expect(report?.databaseId).toBe('main')
      expect(report?.sourcePath).toBe(sourcePath)
      expect(readdirSync(backupDir)).toContain(basename(report?.destPath ?? ''))
      expect(report?.pageCount).toBeGreaterThan(0)
      expect(report?.pageSize).toBeGreaterThan(0)
      expect(report?.byteLength).toBe(statSync(report?.destPath ?? '').size)
      expect(report?.restarts).toBe(0)
      expect(report?.runId).toMatch(/^[0-9a-f]{16}$/)
      expect(report?.finishedAt).toBeGreaterThanOrEqual(report?.startedAt ?? 0)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('leaves a schedule that names no completion callback running as it always did', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'no-callback')
      const counting = countingManager()
      const scheduler = new BackupScheduler(counting.manager)

      const cancel = scheduler.schedule(conn, {
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => counting.completed() >= 1)
      cancel()

      expect(readdirSync(backupDir).filter(f => f.endsWith('.db')).length).toBeGreaterThanOrEqual(1)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('keeps copying after a completion callback throws', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'throwing-callback')
      const errors: Error[] = []
      let calls = 0
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
        onBackup: () => {
          calls++
          throw new Error('the caller could not store the file')
        },
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => calls >= 1)
      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => calls >= 2)
      cancel()

      expect(calls).toBeGreaterThanOrEqual(2)
      expect(errors.map(err => err.message)).toContain('the caller could not store the file')
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('reports a completion callback whose promise rejects, and never leaves that rejection loose', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'rejecting-callback')
      const errors: Error[] = []
      const loose: unknown[] = []
      const catchLoose = (reason: unknown) => loose.push(reason)
      const scheduler = new BackupScheduler()
      process.on('unhandledRejection', catchLoose)

      try {
        const cancel = scheduler.schedule(conn, {
          databaseId: 'main',
          sourcePath: join(temp.path, 'source.db'),
          cron: '* * * * * *',
          destDir: backupDir,
          maxFiles: 10,
          onBackup: async () => {
            throw new Error('the object store refused the upload')
          },
          onError: err => errors.push(err),
        })

        await vi.advanceTimersByTimeAsync(1500)
        await settleUntil(() => errors.length >= 1)
        cancel()
      } finally {
        process.off('unhandledRejection', catchLoose)
      }

      expect(errors.map(err => err.message)).toContain('the object store refused the upload')
      expect(loose).toEqual([])
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('gives up on a completion callback that never settles, and keeps copying', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'hanging-callback')
      const errors: Error[] = []
      let calls = 0
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
        onBackupTimeoutMs: 50,
        onBackup: () => {
          calls++
          return new Promise<void>(() => {})
        },
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => errors.length >= 1)
      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => calls >= 2)
      cancel()

      expect(calls).toBeGreaterThanOrEqual(2)
      expect(errors[0]?.message).toContain('did not return within 50ms')
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('names the open file on its reports where the caller named no database at all', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'derived-names')
      const reports: BackupFileReport[] = []
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
        onBackup: report => {
          reports.push(report)
        },
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => reports.length >= 1)
      cancel()

      expect(reports[0]?.databaseId).toBe('source')
      expect(reports[0]?.sourcePath.endsWith('source.db')).toBe(true)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('refuses to report a copy where nothing names the file it came from', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const errors: Error[] = []
      const reports: BackupFileReport[] = []
      const fileless = {
        ...conn,
        prepare: async (sql: string) => {
          if (!sql.includes('database_list')) return conn.prepare(sql)
          return { all: async () => [{ name: 'main', file: '' }], get: async () => undefined, run: async () => ({}) }
        },
      } as unknown as SQLiteConnection
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(fileless, {
        cron: '* * * * * *',
        destDir: join(temp.path, 'nameless'),
        maxFiles: 10,
        onBackup: report => {
          reports.push(report)
        },
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => errors.length >= 1)
      cancel()

      expect(reports).toEqual([])
      expect(errors[0]?.message).toContain('names no source file')
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('waits on a completion callback for as long as it takes where the deadline is zero', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'unbounded-callback')
      const errors: Error[] = []
      let releaseCallback: (() => void) | undefined
      let finished = 0
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
        onBackupTimeoutMs: 0,
        onBackup: () =>
          new Promise<void>(resolve => {
            releaseCallback = () => {
              finished++
              resolve()
            }
          }),
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => releaseCallback !== undefined)
      await vi.advanceTimersByTimeAsync(700_000)

      expect(errors).toEqual([])
      expect(finished).toBe(0)
      releaseCallback?.()
      await settleUntil(() => finished === 1)
      cancel()
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('reports the copy it finished even where clearing the older files fails', async () => {
    useCronTimers()
    try {
      const conn = await createTestDb(temp.path)
      const backupDir = join(temp.path, 'rotation-failure')
      const reports: BackupFileReport[] = []
      const errors: Error[] = []
      const manager = new BackupManager()
      manager.rotate = () => {
        throw new BackupError('the backup directory could not be listed')
      }
      const scheduler = new BackupScheduler(manager)

      const cancel = scheduler.schedule(conn, {
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
        onBackup: report => {
          reports.push(report)
        },
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      await settleUntil(() => reports.length >= 1)
      cancel()

      expect(reports).toHaveLength(1)
      expect(errors.map(err => err.message)).toContain('the backup directory could not be listed')
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
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
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
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
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
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
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
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
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
        databaseId: 'main',
        sourcePath: join(temp.path, 'source.db'),
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
