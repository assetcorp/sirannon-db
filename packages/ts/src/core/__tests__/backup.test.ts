import BetterSqlite3 from 'better-sqlite3'
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { BackupManager } from '../backup/backup.js'
import { BackupScheduler } from '../backup/scheduler.js'
import { BackupError } from '../errors.js'

type SqliteDb = InstanceType<typeof BetterSqlite3>

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-backup-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

function createTestDb(): SqliteDb {
  const dbPath = join(tempDir, 'source.db')
  const db = new BetterSqlite3(dbPath)
  db.pragma('journal_mode = WAL')
  db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  db.exec("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  db.exec("INSERT INTO users (name, age) VALUES ('Bob', 25)")
  return db
}

describe('BackupManager', () => {
  const manager = new BackupManager()

  describe('backup', () => {
    it('creates a valid SQLite backup file', () => {
      const db = createTestDb()
      const destPath = join(tempDir, 'backup.db')

      manager.backup(db, destPath)

      expect(existsSync(destPath)).toBe(true)

      const backupDb = new BetterSqlite3(destPath, { readonly: true })
      const rows = backupDb.prepare('SELECT * FROM users ORDER BY id').all() as {
        id: number
        name: string
        age: number
      }[]
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')
      backupDb.close()
      db.close()
    })

    it('preserves all rows and schema in the backup', () => {
      const db = createTestDb()
      db.exec('CREATE TABLE products (sku TEXT PRIMARY KEY, price REAL)')
      db.exec("INSERT INTO products (sku, price) VALUES ('WIDGET-01', 9.99)")

      const destPath = join(tempDir, 'full-backup.db')
      manager.backup(db, destPath)

      const backupDb = new BetterSqlite3(destPath, { readonly: true })
      const users = backupDb.prepare('SELECT count(*) as cnt FROM users').get() as { cnt: number }
      const products = backupDb.prepare('SELECT * FROM products').all() as { sku: string; price: number }[]
      expect(users.cnt).toBe(2)
      expect(products).toHaveLength(1)
      expect(products[0].sku).toBe('WIDGET-01')
      backupDb.close()
      db.close()
    })

    it('creates parent directories when they do not exist', () => {
      const db = createTestDb()
      const nested = join(tempDir, 'a', 'b', 'c')
      const destPath = join(nested, 'backup.db')

      manager.backup(db, destPath)

      expect(existsSync(destPath)).toBe(true)
      db.close()
    })

    it('throws BackupError with BACKUP_ERROR code when destination already exists', () => {
      const db = createTestDb()
      const destPath = join(tempDir, 'existing.db')
      writeFileSync(destPath, '')

      try {
        manager.backup(db, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
        expect((err as BackupError).message).toContain('already exists')
      }
      db.close()
    })

    it('throws BackupError with BACKUP_ERROR code when database is closed', () => {
      const db = createTestDb()
      db.close()
      const destPath = join(tempDir, 'closed-backup.db')

      try {
        manager.backup(db, destPath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
      }
    })

    it('throws BackupError when backing up to the source database path', () => {
      const db = createTestDb()
      const sourcePath = join(tempDir, 'source.db')

      try {
        manager.backup(db, sourcePath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(BackupError)
        expect((err as BackupError).code).toBe('BACKUP_ERROR')
        expect((err as BackupError).message).toContain('already exists')
      }
      db.close()
    })

    it('handles paths with spaces', () => {
      const db = createTestDb()
      const spacedDir = join(tempDir, 'dir with spaces')
      mkdirSync(spacedDir, { recursive: true })
      const destPath = join(spacedDir, 'backup file.db')

      manager.backup(db, destPath)

      expect(existsSync(destPath)).toBe(true)
      const backupDb = new BetterSqlite3(destPath, { readonly: true })
      const rows = backupDb.prepare('SELECT * FROM users').all()
      expect(rows).toHaveLength(2)
      backupDb.close()
      db.close()
    })

    it('handles paths with single quotes', () => {
      const db = createTestDb()
      const quotedDir = join(tempDir, "it's a dir")
      mkdirSync(quotedDir, { recursive: true })
      const destPath = join(quotedDir, 'backup.db')

      manager.backup(db, destPath)

      expect(existsSync(destPath)).toBe(true)
      const backupDb = new BetterSqlite3(destPath, { readonly: true })
      const rows = backupDb.prepare('SELECT * FROM users').all()
      expect(rows).toHaveLength(2)
      backupDb.close()
      db.close()
    })

    it('cleans up partial files on failure', () => {
      const db = createTestDb()
      db.close()
      const destPath = join(tempDir, 'partial.db')

      expect(() => manager.backup(db, destPath)).toThrow(BackupError)
      expect(existsSync(destPath)).toBe(false)
    })
  })

  describe('generateFilename', () => {
    it('returns a filename with backup prefix and .db extension', () => {
      const filename = manager.generateFilename()
      expect(filename).toMatch(/^backup-.*\.db$/)
    })

    it('includes an ISO-style timestamp with hyphens replacing colons and dots', () => {
      const filename = manager.generateFilename()
      // Expected format: backup-YYYY-MM-DDTHH-MM-SS-mmmZ.db
      expect(filename).toMatch(/^backup-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z\.db$/)
    })

    it('generates unique filenames across calls separated by time', async () => {
      const first = manager.generateFilename()
      await new Promise((r) => setTimeout(r, 5))
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
        // Stagger modification times so ordering is deterministic
        const time = new Date(now - (4 - i) * 2000)
        utimesSync(filePath, time, time)
      }

      manager.rotate(backupDir, 3)

      const remaining = readdirSync(backupDir).filter((f) => f.endsWith('.db'))
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

      const remaining = readdirSync(backupDir).filter((f) => f.endsWith('.db'))
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
      expect(remaining.filter((f) => f.startsWith('backup-'))).toHaveLength(1)
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
  })
})

describe('BackupScheduler', () => {
  it('fires a backup on cron schedule', async () => {
    const db = createTestDb()
    const backupDir = join(tempDir, 'scheduled')
    const scheduler = new BackupScheduler()

    const cancel = scheduler.schedule(db, {
      cron: '* * * * * *',
      destDir: backupDir,
      maxFiles: 10,
    })

    await new Promise((r) => setTimeout(r, 2500))
    cancel()

    const files = readdirSync(backupDir).filter((f) => f.endsWith('.db'))
    expect(files.length).toBeGreaterThanOrEqual(1)

    // Verify the backup file contains valid data
    const backupPath = join(backupDir, files[0])
    const backupDb = new BetterSqlite3(backupPath, { readonly: true })
    const rows = backupDb.prepare('SELECT * FROM users').all() as { name: string }[]
    expect(rows).toHaveLength(2)
    backupDb.close()
    db.close()
  })

  it('rotates files according to maxFiles', async () => {
    const db = createTestDb()
    const backupDir = join(tempDir, 'rotated')
    const scheduler = new BackupScheduler()

    const cancel = scheduler.schedule(db, {
      cron: '* * * * * *',
      destDir: backupDir,
      maxFiles: 2,
    })

    // Wait for several ticks so rotation has a chance to prune
    await new Promise((r) => setTimeout(r, 4500))
    cancel()

    const files = readdirSync(backupDir).filter((f) => f.endsWith('.db'))
    // At least one backup must have been created, and at most maxFiles retained
    expect(files.length).toBeGreaterThanOrEqual(1)
    expect(files.length).toBeLessThanOrEqual(2)
    db.close()
  })

  it('cancel function stops future backups', async () => {
    const db = createTestDb()
    const backupDir = join(tempDir, 'cancelled')
    const scheduler = new BackupScheduler()

    const cancel = scheduler.schedule(db, {
      cron: '* * * * * *',
      destDir: backupDir,
      maxFiles: 10,
    })

    // Let at least one backup fire, then cancel
    await new Promise((r) => setTimeout(r, 2500))
    cancel()

    const countAfterCancel = readdirSync(backupDir).filter((f) => f.endsWith('.db')).length
    expect(countAfterCancel).toBeGreaterThanOrEqual(1)

    // Wait and confirm no new backups appear
    await new Promise((r) => setTimeout(r, 2500))
    const countLater = readdirSync(backupDir).filter((f) => f.endsWith('.db')).length
    expect(countLater).toBe(countAfterCancel)
    db.close()
  })

  it('throws BackupError with BACKUP_ERROR code for an invalid cron expression', () => {
    const db = createTestDb()
    const scheduler = new BackupScheduler()

    try {
      scheduler.schedule(db, {
        cron: 'not a cron',
        destDir: join(tempDir, 'invalid'),
        maxFiles: 5,
      })
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(BackupError)
      expect((err as BackupError).code).toBe('BACKUP_ERROR')
    }
    db.close()
  })

  it('creates destination directory if it does not exist', () => {
    const db = createTestDb()
    const backupDir = join(tempDir, 'new', 'nested', 'dir')
    const scheduler = new BackupScheduler()

    // Once-a-year schedule; we only verify directory creation, not firing
    const cancel = scheduler.schedule(db, {
      cron: '0 0 1 1 *',
      destDir: backupDir,
      maxFiles: 5,
    })

    expect(existsSync(backupDir)).toBe(true)
    cancel()
    db.close()
  })

  it('defaults maxFiles to 5 when not specified', async () => {
    const db = createTestDb()
    const backupDir = join(tempDir, 'defaults')

    let observedMaxFiles: number | undefined
    const customManager = new BackupManager()
    const originalRotate = customManager.rotate.bind(customManager)
    customManager.rotate = (dir: string, maxFiles: number) => {
      observedMaxFiles = maxFiles
      return originalRotate(dir, maxFiles)
    }

    const scheduler = new BackupScheduler(customManager)
    const cancel = scheduler.schedule(db, {
      cron: '* * * * * *',
      destDir: backupDir,
    })

    await new Promise((r) => setTimeout(r, 2500))
    cancel()

    expect(observedMaxFiles).toBe(5)
    db.close()
  })

  it('calls onError when a scheduled backup fails', async () => {
    const db = createTestDb()
    db.close() // close the database so every backup attempt fails

    const backupDir = join(tempDir, 'error-reporting')
    const errors: Error[] = []
    const scheduler = new BackupScheduler()

    const cancel = scheduler.schedule(db, {
      cron: '* * * * * *',
      destDir: backupDir,
      maxFiles: 5,
      onError: (err) => errors.push(err),
    })

    await new Promise((r) => setTimeout(r, 2500))
    cancel()

    expect(errors.length).toBeGreaterThanOrEqual(1)
    expect(errors[0]).toBeInstanceOf(BackupError)
    expect((errors[0] as BackupError).code).toBe('BACKUP_ERROR')
  })

  it('silently discards errors when onError is not provided', async () => {
    const db = createTestDb()
    db.close()

    const backupDir = join(tempDir, 'silent-errors')
    const scheduler = new BackupScheduler()

    // Should not throw or crash, even though every backup attempt fails
    const cancel = scheduler.schedule(db, {
      cron: '* * * * * *',
      destDir: backupDir,
      maxFiles: 5,
    })

    await new Promise((r) => setTimeout(r, 2500))
    cancel()

    // No backup files should exist since the database was closed
    const files = readdirSync(backupDir).filter((f) => f.endsWith('.db'))
    expect(files).toHaveLength(0)
  })

  it('accepts a custom BackupManager via constructor', () => {
    const customManager = new BackupManager()
    const scheduler = new BackupScheduler(customManager)
    expect(scheduler).toBeInstanceOf(BackupScheduler)
  })
})
