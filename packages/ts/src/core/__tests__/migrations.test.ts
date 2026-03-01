import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../database.js'
import { MigrationError } from '../errors.js'
import { MigrationRunner } from '../migrations/runner.js'
import type { AppliedMigration } from '../migrations/types.js'

let tempDir: string
let migrationsDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-migrations-'))
  migrationsDir = join(tempDir, 'migrations')
  mkdirSync(migrationsDir)
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

function createTestDb(): Database {
  const dbPath = join(tempDir, 'test.db')
  return new Database('test', dbPath)
}

function writeMigration(filename: string, sql: string): void {
  writeFileSync(join(migrationsDir, filename), sql)
}

describe('MigrationRunner', () => {
  describe('applying migrations', () => {
    it('applies migrations in ascending version order', () => {
      const db = createTestDb()

      writeMigration('002_add_email.sql', 'ALTER TABLE users ADD COLUMN email TEXT')
      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(2)
      expect(result.applied[0].version).toBe(1)
      expect(result.applied[0].name).toBe('create_users')
      expect(result.applied[1].version).toBe(2)
      expect(result.applied[1].name).toBe('add_email')
      expect(result.skipped).toBe(0)

      const rows = db.query<{ name: string; email: string | null }>("SELECT * FROM users WHERE name = 'test'")
      expect(rows).toEqual([])

      db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
      const inserted = db.queryOne<{ name: string; email: string }>('SELECT name, email FROM users WHERE id = 1')
      expect(inserted?.name).toBe('Alice')
      expect(inserted?.email).toBe('alice@example.com')

      db.close()
    })

    it('records applied migrations in the tracking table', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('002_add_posts.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')

      db.migrate(migrationsDir)

      const tracked = db.query<AppliedMigration>(
        'SELECT version, name, applied_at FROM _sirannon_migrations ORDER BY version',
      )
      expect(tracked).toHaveLength(2)
      expect(tracked[0].version).toBe(1)
      expect(tracked[0].name).toBe('create_users')
      expect(tracked[0].applied_at).toBeGreaterThan(0)
      expect(tracked[1].version).toBe(2)
      expect(tracked[1].name).toBe('add_posts')

      db.close()
    })

    it('handles non-zero-padded version numbers', () => {
      const db = createTestDb()

      writeMigration('1_first.sql', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)')
      writeMigration('10_tenth.sql', 'CREATE TABLE t10 (id INTEGER PRIMARY KEY)')
      writeMigration('2_second.sql', 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)')

      const result = db.migrate(migrationsDir)

      expect(result.applied.map(m => m.version)).toEqual([1, 2, 10])
      db.close()
    })

    it('handles multi-statement migration files', () => {
      const db = createTestDb()

      writeMigration(
        '001_seed.sql',
        `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice');
INSERT INTO users (name) VALUES ('Bob');`,
      )

      db.migrate(migrationsDir)

      const rows = db.query<{ name: string }>('SELECT name FROM users ORDER BY id')
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')

      db.close()
    })
  })

  describe('skipping applied migrations', () => {
    it('skips already-applied migrations on second run', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const first = db.migrate(migrationsDir)
      expect(first.applied).toHaveLength(1)
      expect(first.skipped).toBe(0)

      const second = db.migrate(migrationsDir)
      expect(second.applied).toHaveLength(0)
      expect(second.skipped).toBe(1)

      db.close()
    })

    it('applies only new migrations when some are already applied', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.migrate(migrationsDir)

      writeMigration('002_add_posts.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(1)
      expect(result.applied[0].version).toBe(2)
      expect(result.skipped).toBe(1)

      db.close()
    })
  })

  describe('transaction rollback on failure', () => {
    it('rolls back all migrations when one fails', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('002_bad_sql.sql', 'THIS IS NOT VALID SQL')

      expect(() => db.migrate(migrationsDir)).toThrow(MigrationError)

      const tables = db.query<{ name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
      )
      expect(tables).toHaveLength(0)

      const tracking = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations')
      expect(tracking).toHaveLength(0)

      db.close()
    })

    it('includes the failing version number in the error', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('003_broken.sql', 'DROP TABLE nonexistent_table')

      try {
        db.migrate(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).version).toBe(3)
        expect((err as MigrationError).code).toBe('MIGRATION_ERROR')
      }

      db.close()
    })
  })

  describe('empty migrations directory', () => {
    it('returns empty result for an empty directory', () => {
      const db = createTestDb()
      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(0)
      expect(result.skipped).toBe(0)

      db.close()
    })
  })

  describe('error handling', () => {
    it('throws MigrationError for nonexistent directory', () => {
      const db = createTestDb()
      const badPath = join(tempDir, 'nonexistent')

      expect(() => db.migrate(badPath)).toThrow(MigrationError)

      db.close()
    })

    it('throws MigrationError for duplicate version numbers', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
      writeMigration('001_create_posts.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)')

      try {
        db.migrate(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).version).toBe(1)
        expect((err as MigrationError).message).toContain('Duplicate')
      }

      db.close()
    })

    it('throws MigrationError for empty migration file', () => {
      const db = createTestDb()

      writeMigration('001_empty.sql', '')

      try {
        db.migrate(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).message).toContain('empty')
      }

      db.close()
    })

    it('throws MigrationError for whitespace-only migration file', () => {
      const db = createTestDb()

      writeMigration('001_blank.sql', '   \n\t  ')

      try {
        db.migrate(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).message).toContain('empty')
      }

      db.close()
    })

    it('ignores non-matching files in the directory', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
      writeMigration('README.md', '# Migrations')
      writeMigration('.gitkeep', '')
      writeMigration('backup.sql.bak', 'SELECT 1')

      const result = db.migrate(migrationsDir)
      expect(result.applied).toHaveLength(1)
      expect(result.applied[0].version).toBe(1)

      db.close()
    })

    it('throws MigrationError when path is a file instead of a directory', () => {
      const db = createTestDb()
      const filePath = join(tempDir, 'not-a-dir.txt')
      writeFileSync(filePath, 'hello')

      try {
        db.migrate(filePath)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).message).toContain('not a directory')
      }

      db.close()
    })

    it('ignores subdirectories matching the migration filename pattern', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
      mkdirSync(join(migrationsDir, '002_sneaky.sql'))

      const result = db.migrate(migrationsDir)
      expect(result.applied).toHaveLength(1)
      expect(result.applied[0].version).toBe(1)

      db.close()
    })

    it('rejects migration on a read-only database', () => {
      const dbPath = join(tempDir, 'readonly.db')
      const setup = new Database('setup', dbPath)
      setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY)')
      setup.close()

      const readonlyDb = new Database('test', dbPath, { readOnly: true })

      writeMigration('001_add_column.sql', 'ALTER TABLE users ADD COLUMN name TEXT')

      expect(() => readonlyDb.migrate(migrationsDir)).toThrow()

      readonlyDb.close()
    })

    it('rejects migration on a closed database', () => {
      const db = createTestDb()
      db.close()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')

      expect(() => db.migrate(migrationsDir)).toThrow()
    })
  })

  describe('idempotency', () => {
    it('produces identical results across repeated calls', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('002_create_posts.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')

      const first = db.migrate(migrationsDir)
      const second = db.migrate(migrationsDir)
      const third = db.migrate(migrationsDir)

      expect(first.applied).toHaveLength(2)
      expect(second.applied).toHaveLength(0)
      expect(second.skipped).toBe(2)
      expect(third.applied).toHaveLength(0)
      expect(third.skipped).toBe(2)

      db.close()
    })

    it('allows incremental migrations across multiple calls', () => {
      const db = createTestDb()

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.migrate(migrationsDir)

      writeMigration('002_create_posts.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      db.migrate(migrationsDir)

      writeMigration('003_create_comments.sql', 'CREATE TABLE comments (id INTEGER PRIMARY KEY, body TEXT)')
      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(1)
      expect(result.applied[0].version).toBe(3)
      expect(result.skipped).toBe(2)

      const tracked = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations ORDER BY version')
      expect(tracked.map(r => r.version)).toEqual([1, 2, 3])

      db.close()
    })
  })

  describe('MigrationRunner.run (direct)', () => {
    it('works with a raw better-sqlite3 connection', () => {
      const dbPath = join(tempDir, 'raw.db')
      const raw = new BetterSqlite3(dbPath)

      writeMigration('001_create_users.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const result = MigrationRunner.run(raw, migrationsDir)
      expect(result.applied).toHaveLength(1)

      const rows = raw.prepare('SELECT name FROM users').all()
      expect(rows).toEqual([])

      raw.close()
    })
  })
})
