import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { BackupManager } from '../backup/backup.js'
import { BackupScheduler } from '../backup/scheduler.js'
import type { SQLiteConnection } from '../driver/types.js'
import { BackupError } from '../errors.js'
import { testDriver } from './helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-backup-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

async function createTestDb(): Promise<SQLiteConnection> {
  const dbPath = join(tempDir, 'source.db')
  const conn = await testDriver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')
  await conn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  await conn.exec("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  await conn.exec("INSERT INTO users (name, age) VALUES ('Bob', 25)")
  return conn
}

describe('BackupManager', () => {
  const manager = new BackupManager()

  describe('backup', () => {
    it('creates a valid SQLite backup file', async () => {
      const conn = await createTestDb()
      const destPath = join(tempDir, 'backup.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)

      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users ORDER BY id')
      const rows = (await stmt.all()) as {
        id: number
        name: string
        age: number
      }[]
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')
      await backupConn.close()
      await conn.close()
    })

    it('preserves all rows and schema in the backup', async () => {
      const conn = await createTestDb()
      await conn.exec('CREATE TABLE products (sku TEXT PRIMARY KEY, price REAL)')
      await conn.exec("INSERT INTO products (sku, price) VALUES ('WIDGET-01', 9.99)")

      const destPath = join(tempDir, 'full-backup.db')
      await manager.backup(conn, destPath)

      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const usersStmt = await backupConn.prepare('SELECT count(*) as cnt FROM users')
      const users = (await usersStmt.get()) as { cnt: number }
      const productsStmt = await backupConn.prepare('SELECT * FROM products')
      const products = (await productsStmt.all()) as { sku: string; price: number }[]
      expect(users.cnt).toBe(2)
      expect(products).toHaveLength(1)
      expect(products[0].sku).toBe('WIDGET-01')
      await backupConn.close()
      await conn.close()
    })

    it('creates parent directories when they do not exist', async () => {
      const conn = await createTestDb()
      const nested = join(tempDir, 'a', 'b', 'c')
      const destPath = join(nested, 'backup.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)
      await conn.close()
    })

    it('throws BackupError with BACKUP_ERROR code when destination already exists', async () => {
      const conn = await createTestDb()
      const destPath = join(tempDir, 'existing.db')
      writeFileSync(destPath, '')

      try {
        await manager.backup(conn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
        expect((err as BackupError).message).toContain('already exists')
      }
      await conn.close()
    })

    it('throws BackupError with BACKUP_ERROR code when database is closed', async () => {
      const conn = await createTestDb()
      await conn.close()
      const destPath = join(tempDir, 'closed-backup.db')

      try {
        await manager.backup(conn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
      }
    })

    it('throws BackupError when backing up to the source database path', async () => {
      const conn = await createTestDb()
      const sourcePath = join(tempDir, 'source.db')

      try {
        await manager.backup(conn, sourcePath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
        expect((err as BackupError).message).toContain('already exists')
      }
      await conn.close()
    })

    it('handles paths with spaces', async () => {
      const conn = await createTestDb()
      const spacedDir = join(tempDir, 'dir with spaces')
      mkdirSync(spacedDir, { recursive: true })
      const destPath = join(spacedDir, 'backup file.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)
      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(2)
      await backupConn.close()
      await conn.close()
    })

    it('handles paths with single quotes', async () => {
      const conn = await createTestDb()
      const quotedDir = join(tempDir, "it's a dir")
      mkdirSync(quotedDir, { recursive: true })
      const destPath = join(quotedDir, 'backup.db')

      await manager.backup(conn, destPath)

      expect(existsSync(destPath)).toBe(true)
      const backupConn = await testDriver.open(destPath, { readonly: true, walMode: false })
      const stmt = await backupConn.prepare('SELECT * FROM users')
      const rows = await stmt.all()
      expect(rows).toHaveLength(2)
      await backupConn.close()
      await conn.close()
    })

    it('cleans up partial files on failure', async () => {
      const conn = await createTestDb()
      await conn.close()
      const destPath = join(tempDir, 'partial.db')

      await expect(manager.backup(conn, destPath)).rejects.toThrow(BackupError)
      expect(existsSync(destPath)).toBe(false)
    })

    it('rejects backup paths with control characters', async () => {
      const conn = await createTestDb()
      const badPath = `${join(tempDir, 'bad')}\u0001.db`

      await expect(manager.backup(conn, badPath)).rejects.toThrow(BackupError)
      await conn.close()
    })

    it('throws when backup directory creation fails', async () => {
      const conn = await createTestDb()
      const blocked = join(tempDir, 'blocked')
      writeFileSync(blocked, 'not-a-directory')
      const destPath = join(blocked, 'nested', 'backup.db')

      await expect(manager.backup(conn, destPath)).rejects.toThrow(BackupError)
      await conn.close()
    })

    it('formats non-Error values thrown during backup execution', async () => {
      const fakeConn = {
        async exec() {
          throw 'string exec failure'
        },
      } as unknown as SQLiteConnection
      const destPath = join(tempDir, 'non-error-exec.db')

      try {
        await manager.backup(fakeConn, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).message).toContain('string exec failure')
      }
    })

    it('formats non-Error values thrown while creating destination directory', async () => {
      vi.resetModules()
      const fsActual = await vi.importActual<typeof import('node:fs')>('node:fs')

      try {
        vi.doMock('node:fs', () => ({
          ...fsActual,
          existsSync: fsActual.existsSync,
          lstatSync: fsActual.lstatSync,
          readdirSync: fsActual.readdirSync,
          rmSync: fsActual.rmSync,
          mkdirSync: () => {
            throw 'string mkdir failure'
          },
        }))

        const { BackupManager: MockedBackupManager } = await import('../backup/backup.js')
        const mockedManager = new MockedBackupManager()
        const conn = await createTestDb()
        const destPath = join(tempDir, 'nested', 'backup.db')

        try {
          await mockedManager.backup(conn, destPath)
          expect.unreachable('should have thrown')
        } catch (err) {
          expect((err as Error).message).toContain('string mkdir failure')
        } finally {
          await conn.close()
        }
      } finally {
        vi.doUnmock('node:fs')
        vi.resetModules()
      }
    })
  })

  describe('generateFilename', () => {
    it('returns a filename with backup prefix and .db extension', () => {
      const filename = manager.generateFilename()
      expect(filename).toMatch(/^backup-.*\.db$/)
    })

    it('includes an ISO-style timestamp with hyphens replacing colons and dots', () => {
      const filename = manager.generateFilename()
      expect(filename).toMatch(/^backup-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z\.db$/)
    })

    it('generates unique filenames across calls separated by time', async () => {
      const first = manager.generateFilename()
      await new Promise(r => setTimeout(r, 5))
      const second = manager.generateFilename()
      expect(first).not.toBe(second)
    })
  })

  describe('rotate', () => {
    it('deletes oldest files beyond maxFiles', () => {
      const backupDir = join(tempDir, 'backups')
      mkdirSync(backupDir)

      const now = Date.now()
      for (let i = 0; i < 5; i++) {
        const filePath = join(backupDir, `backup-file-${i}.db`)
        writeFileSync(filePath, `data-${i}`)
        const time = new Date(now - (4 - i) * 2000)
        utimesSync(filePath, time, time)
      }

      manager.rotate(backupDir, 3)

      const remaining = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(remaining).toHaveLength(3)
    })

    it('keeps the most recent files and removes the oldest', () => {
      const backupDir = join(tempDir, 'backups')
      mkdirSync(backupDir)

      const now = Date.now()
      const files = [
        { name: 'backup-old.db', age: 3000 },
        { name: 'backup-mid.db', age: 2000 },
        { name: 'backup-new.db', age: 1000 },
      ]
      for (const f of files) {
        const filePath = join(backupDir, f.name)
        writeFileSync(filePath, 'data')
        const time = new Date(now - f.age)
        utimesSync(filePath, time, time)
      }

      manager.rotate(backupDir, 2)

      const remaining = readdirSync(backupDir)
      expect(remaining).toContain('backup-new.db')
      expect(remaining).toContain('backup-mid.db')
      expect(remaining).not.toContain('backup-old.db')
    })

    it('does nothing when file count is at or below maxFiles', () => {
      const backupDir = join(tempDir, 'backups')
      mkdirSync(backupDir)
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')
      writeFileSync(join(backupDir, 'backup-b.db'), 'data')

      manager.rotate(backupDir, 5)

      const remaining = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(remaining).toHaveLength(2)
    })

    it('ignores files that do not match the backup naming convention', () => {
      const backupDir = join(tempDir, 'backups')
      mkdirSync(backupDir)

      const now = Date.now()
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')
      utimesSync(join(backupDir, 'backup-a.db'), new Date(now - 2000), new Date(now - 2000))
      writeFileSync(join(backupDir, 'backup-b.db'), 'data')
      utimesSync(join(backupDir, 'backup-b.db'), new Date(now), new Date(now))
      writeFileSync(join(backupDir, 'other-file.db'), 'data')
      writeFileSync(join(backupDir, 'notes.txt'), 'data')

      manager.rotate(backupDir, 1)

      const remaining = readdirSync(backupDir)
      expect(remaining).toContain('other-file.db')
      expect(remaining).toContain('notes.txt')
      expect(remaining.filter(f => f.startsWith('backup-'))).toHaveLength(1)
      expect(remaining).toContain('backup-b.db')
    })

    it('handles a non-existent directory without throwing', () => {
      expect(() => manager.rotate(join(tempDir, 'nonexistent'), 3)).not.toThrow()
    })

    it('is a no-op when maxFiles is zero', () => {
      const backupDir = join(tempDir, 'backups')
      mkdirSync(backupDir)
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')

      manager.rotate(backupDir, 0)

      expect(readdirSync(backupDir)).toHaveLength(1)
    })

    it('is a no-op when maxFiles is negative', () => {
      const backupDir = join(tempDir, 'backups')
      mkdirSync(backupDir)
      writeFileSync(join(backupDir, 'backup-a.db'), 'data')

      manager.rotate(backupDir, -1)

      expect(readdirSync(backupDir)).toHaveLength(1)
    })

    it('throws when backup directory cannot be read', () => {
      const filePath = join(tempDir, 'not-a-directory')
      writeFileSync(filePath, 'x')

      expect(() => manager.rotate(filePath, 2)).toThrow(BackupError)
    })

    it('formats non-Error values thrown while listing backup files', async () => {
      vi.resetModules()
      const fsActual = await vi.importActual<typeof import('node:fs')>('node:fs')

      try {
        vi.doMock('node:fs', () => ({
          ...fsActual,
          existsSync: () => true,
          readdirSync: () => {
            throw 'string readdir failure'
          },
        }))

        const { BackupError: MockedBackupError } = await import('../errors.js')
        const { BackupManager: MockedBackupManager } = await import('../backup/backup.js')
        const mockedManager = new MockedBackupManager()

        try {
          mockedManager.rotate(tempDir, 1)
          expect.unreachable('should have thrown')
        } catch (err) {
          expect(err).toBeInstanceOf(MockedBackupError)
          expect((err as InstanceType<typeof MockedBackupError>).message).toContain('string readdir failure')
        }
      } finally {
        vi.doUnmock('node:fs')
        vi.resetModules()
      }
    })
  })
})

describe('BackupScheduler', () => {
  it('fires a backup on cron schedule', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb()
      const backupDir = join(tempDir, 'scheduled')
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
      })

      await vi.advanceTimersByTimeAsync(1500)
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
    vi.useFakeTimers()
    try {
      const conn = await createTestDb()
      const backupDir = join(tempDir, 'rotated')
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 2,
      })

      await vi.advanceTimersByTimeAsync(4500)
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
    vi.useFakeTimers()
    try {
      const conn = await createTestDb()
      const backupDir = join(tempDir, 'cancelled')
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 10,
      })

      await vi.advanceTimersByTimeAsync(1500)
      cancel()

      const countAfterCancel = readdirSync(backupDir).filter(f => f.endsWith('.db')).length
      expect(countAfterCancel).toBeGreaterThanOrEqual(1)

      await vi.advanceTimersByTimeAsync(3000)
      const countLater = readdirSync(backupDir).filter(f => f.endsWith('.db')).length
      expect(countLater).toBe(countAfterCancel)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('throws BackupError with BACKUP_ERROR code for an invalid cron expression', async () => {
    const conn = await createTestDb()
    const scheduler = new BackupScheduler()

    try {
      scheduler.schedule(conn, {
        cron: 'not a cron',
        destDir: join(tempDir, 'invalid'),
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
    const conn = await createTestDb()
    const backupDir = join(tempDir, 'new', 'nested', 'dir')
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

  it('defaults maxFiles to 5 when not specified', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb()
      const backupDir = join(tempDir, 'defaults')

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
      cancel()

      expect(observedMaxFiles).toBe(5)
      await conn.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('calls onError when a scheduled backup fails', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb()
      await conn.close()

      const backupDir = join(tempDir, 'error-reporting')
      const errors: Error[] = []
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 5,
        onError: err => errors.push(err),
      })

      await vi.advanceTimersByTimeAsync(1500)
      cancel()

      expect(errors.length).toBeGreaterThanOrEqual(1)
      expect(errors[0]).toBeInstanceOf(BackupError)
      expect((errors[0] as BackupError).code).toBe('BACKUP_ERROR')
    } finally {
      vi.useRealTimers()
    }
  })

  it('silently discards errors when onError is not provided', async () => {
    vi.useFakeTimers()
    try {
      const conn = await createTestDb()
      await conn.close()

      const backupDir = join(tempDir, 'silent-errors')
      const scheduler = new BackupScheduler()

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: backupDir,
        maxFiles: 5,
      })

      await vi.advanceTimersByTimeAsync(1500)
      cancel()

      const files = readdirSync(backupDir).filter(f => f.endsWith('.db'))
      expect(files).toHaveLength(0)
    } finally {
      vi.useRealTimers()
    }
  })

  it('accepts a custom BackupManager via constructor', () => {
    const customManager = new BackupManager()
    const scheduler = new BackupScheduler(customManager)
    expect(scheduler).toBeInstanceOf(BackupScheduler)
  })

  it('throws when scheduler destination directory cannot be created', async () => {
    const conn = await createTestDb()
    const blocked = join(tempDir, 'blocked')
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
    const conn = await createTestDb()
    const backupDir = join(tempDir, 'existing-dir')
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

      const { BackupScheduler: MockedBackupScheduler } = await import('../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb()

      expect(() =>
        scheduler.schedule(conn, {
          cron: '0 0 1 1 *',
          destDir: join(tempDir, 'new-dir'),
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

      const { BackupError: MockedBackupError } = await import('../errors.js')
      const { BackupScheduler: MockedBackupScheduler } = await import('../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb()
      const errors: Error[] = []

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(tempDir, 'cron-string'),
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

      const { BackupError: MockedBackupError } = await import('../errors.js')
      const { BackupScheduler: MockedBackupScheduler } = await import('../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb()
      const errors: Error[] = []

      const cancel = scheduler.schedule(conn, {
        cron: '* * * * * *',
        destDir: join(tempDir, 'cron-number'),
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

      const { BackupScheduler: MockedBackupScheduler } = await import('../backup/scheduler.js')
      const scheduler = new MockedBackupScheduler()
      const conn = await createTestDb()

      expect(() =>
        scheduler.schedule(conn, {
          cron: 'bad cron',
          destDir: join(tempDir, 'invalid-cron'),
        }),
      ).toThrow("Invalid cron expression 'bad cron': invalid cron syntax")
      await conn.close()
    } finally {
      vi.doUnmock('croner')
      vi.resetModules()
    }
  })
})
