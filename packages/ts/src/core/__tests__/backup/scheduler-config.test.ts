import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../../backup/backup.js'
import { BackupScheduler } from '../../backup/scheduler.js'
import { BackupError } from '../../errors.js'
import { createTestDb, useTempDir } from './shared.js'

const temp = useTempDir()

describe('BackupScheduler', () => {
  it('throws BackupError with BACKUP_ERROR code for an invalid cron expression', async () => {
    const conn = await createTestDb(temp.path)
    const scheduler = new BackupScheduler()

    try {
      scheduler.schedule(conn, {
        cron: 'not a cron',
        destDir: join(temp.path, 'invalid'),
        maxFiles: 5,
      })
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(BackupError)
      expect((err as BackupError).code).toBe('BACKUP_ERROR')
      expect((err as BackupError).message).toContain("Invalid cron expression 'not a cron'")
    }
    await conn.close()
  })

  it('creates destination directory if it does not exist', async () => {
    const conn = await createTestDb(temp.path)
    const backupDir = join(temp.path, 'new', 'nested', 'dir')
    const scheduler = new BackupScheduler()

    const cancel = scheduler.schedule(conn, {
      cron: '0 0 1 1 *',
      destDir: backupDir,
      maxFiles: 5,
    })

    expect(existsSync(backupDir)).toBe(true)
    cancel()
    await conn.close()
  })

  it('accepts a custom BackupManager via constructor', () => {
    const customManager = new BackupManager()
    const scheduler = new BackupScheduler(customManager)
    expect(scheduler).toBeInstanceOf(BackupScheduler)
  })

  it('throws when scheduler destination directory cannot be created', async () => {
    const conn = await createTestDb(temp.path)
    const blocked = join(temp.path, 'blocked')
    writeFileSync(blocked, 'not-a-directory')
    const scheduler = new BackupScheduler()

    expect(() =>
      scheduler.schedule(conn, {
        cron: '0 0 1 1 *',
        destDir: join(blocked, 'nested'),
        maxFiles: 5,
      }),
    ).toThrow(BackupError)
    await conn.close()
  })

  it('schedules without creating directory when destination already exists', async () => {
    const conn = await createTestDb(temp.path)
    const backupDir = join(temp.path, 'existing-dir')
    mkdirSync(backupDir, { recursive: true })
    const scheduler = new BackupScheduler()

    const cancel = scheduler.schedule(conn, {
      cron: '0 0 1 1 *',
      destDir: backupDir,
      maxFiles: 5,
    })
    cancel()
    await conn.close()
  })

  it('formats non-Error mkdir failures when preparing scheduler destination', async () => {
    vi.resetModules()
    const fsActual = await vi.importActual<typeof import('node:fs')>('node:fs')

    try {
      vi.doMock('node:fs', () => ({
        ...fsActual,
        existsSync: () => false,
        mkdirSync: () => {
          throw 'mkdir failed'
        },
      }))

      const { BackupScheduler: MockedBackupScheduler } = await import('../../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb(temp.path)

      expect(() =>
        scheduler.schedule(conn, {
          cron: '0 0 1 1 *',
          destDir: join(temp.path, 'new-dir'),
        }),
      ).toThrow('mkdir failed')
      await conn.close()
    } finally {
      vi.doUnmock('node:fs')
      vi.resetModules()
    }
  })

  it('wraps a string thrown by a scheduled backup into a BackupError for onError', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb(temp.path)
      const manager = new BackupManager()
      manager.backup = async () => {
        throw 'string backup failure'
      }
      const scheduler = new BackupScheduler(manager)
      const errors: Error[] = []

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(temp.path, 'cron-string'),
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      cancel()

      expect(errors.length).toBeGreaterThanOrEqual(1)
      expect(errors[0]).toBeInstanceOf(BackupError)
      expect(errors[0].message).toContain('string backup failure')
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('wraps a non-string thrown by a scheduled backup into a generic BackupError', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb(temp.path)
      const manager = new BackupManager()
      manager.backup = async () => {
        throw 12345
      }
      const scheduler = new BackupScheduler(manager)
      const errors: Error[] = []

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(temp.path, 'cron-number'),
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      cancel()

      expect(errors.length).toBeGreaterThanOrEqual(1)
      expect(errors[0]).toBeInstanceOf(BackupError)
      expect(errors[0].message).toContain('Scheduled backup failed')
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('contains a throwing onError handler without crashing the scheduler', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb(temp.path)
      const manager = new BackupManager()
      manager.backup = async () => {
        throw new BackupError('backup failed')
      }
      const scheduler = new BackupScheduler(manager)
      let handlerCalls = 0

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(temp.path, 'throwing-handler'),
        onError: () => {
          handlerCalls += 1
          throw new Error('handler exploded')
        },
      })

      await vi.advanceTimersByTimeAsync(1500)
      cancel()

      expect(handlerCalls).toBeGreaterThanOrEqual(1)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })
})
