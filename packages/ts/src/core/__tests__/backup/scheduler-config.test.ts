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

  it('wraps string cron callback failures into BackupError for onError', async () => {
    vi.resetModules()

    try {
      vi.doMock('croner', () => ({
        Cron: class {
          constructor(_expr: string, options: { catch?: (err: unknown) => void }) {
            options.catch?.('string cron failure')
          }
          stop() {}
        },
      }))

      const { BackupError: MockedBackupError } = await import('../../errors.js')
      const { BackupScheduler: MockedBackupScheduler } = await import('../../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb(temp.path)
      const errors: Error[] = []

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(temp.path, 'cron-string'),
        onError: err => errors.push(err),
      })
      cancel()

      expect(errors).toHaveLength(1)
      expect(errors[0]).toBeInstanceOf(MockedBackupError)
      expect(errors[0].message).toContain('string cron failure')
      await conn.close()
    } finally {
      vi.doUnmock('croner')
      vi.resetModules()
    }
  })

  it('wraps non-string cron callback failures into a generic BackupError', async () => {
    vi.resetModules()

    try {
      vi.doMock('croner', () => ({
        Cron: class {
          constructor(_expr: string, options: { catch?: (err: unknown) => void }) {
            options.catch?.(12345)
          }
          stop() {}
        },
      }))

      const { BackupError: MockedBackupError } = await import('../../errors.js')
      const { BackupScheduler: MockedBackupScheduler } = await import('../../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb(temp.path)
      const errors: Error[] = []

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(temp.path, 'cron-number'),
        onError: err => errors.push(err),
      })
      cancel()

      expect(errors).toHaveLength(1)
      expect(errors[0]).toBeInstanceOf(MockedBackupError)
      expect(errors[0].message).toContain('Scheduled backup failed')
      await conn.close()
    } finally {
      vi.doUnmock('croner')
      vi.resetModules()
    }
  })

  it('formats non-Error cron constructor failures', async () => {
    vi.resetModules()

    try {
      vi.doMock('croner', () => ({
        Cron: class {
          constructor() {
            throw 'invalid cron syntax'
          }
          stop() {}
        },
      }))

      const { BackupScheduler: MockedBackupScheduler } = await import('../../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb(temp.path)

      expect(() =>
        scheduler.schedule(conn, {
          cron: 'bad cron',
          destDir: join(temp.path, 'invalid-cron'),
        }),
      ).toThrow("Invalid cron expression 'bad cron': invalid cron syntax")
      await conn.close()
    } finally {
      vi.doUnmock('croner')
      vi.resetModules()
    }
  })
})
