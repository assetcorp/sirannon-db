import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../database.js'
import { MigrationError } from '../errors.js'
import { MigrationRunner } from '../migrations/runner.js'
import type { AppliedMigration, Migration } from '../migrations/types.js'

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
  describe('forward migrations (file-based)', () => {
    it('applies migrations in ascending version order', () => {
      const db = createTestDb()

      writeMigration('002_add_email.up.sql', 'ALTER TABLE users ADD COLUMN email TEXT')
      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(2)
      expect(result.applied[0].version).toBe(1)
      expect(result.applied[0].name).toBe('create_users')
      expect(result.applied[1].version).toBe(2)
      expect(result.applied[1].name).toBe('add_email')
      expect(result.skipped).toBe(0)

      db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
      const inserted = db.queryOne<{ name: string; email: string }>('SELECT name, email FROM users WHERE id = 1')
      expect(inserted?.name).toBe('Alice')
      expect(inserted?.email).toBe('alice@example.com')

      db.close()
    })

    it('records applied migrations in the tracking table', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('002_add_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')

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

    it('handles non-zero-padded and timestamp-based versions', () => {
      const db = createTestDb()

      writeMigration('1_first.up.sql', 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)')
      writeMigration('10_tenth.up.sql', 'CREATE TABLE t10 (id INTEGER PRIMARY KEY)')
      writeMigration('2_second.up.sql', 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)')

      const result = db.migrate(migrationsDir)

      expect(result.applied.map(m => m.version)).toEqual([1, 2, 10])
      db.close()
    })

    it('handles multi-statement migration files', () => {
      const db = createTestDb()

      writeMigration(
        '001_seed.up.sql',
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

    it('skips already-applied migrations on second run', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
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

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.migrate(migrationsDir)

      writeMigration('002_add_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(1)
      expect(result.applied[0].version).toBe(2)
      expect(result.skipped).toBe(1)

      db.close()
    })

    it('rolls back all migrations in the transaction when one fails', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('002_bad_sql.up.sql', 'THIS IS NOT VALID SQL')

      expect(() => db.migrate(migrationsDir)).toThrow(MigrationError)

      const tables = db.query<{ name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
      )
      expect(tables).toHaveLength(0)

      const tracking = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations')
      expect(tracking).toHaveLength(0)

      db.close()
    })

    it('returns empty result for an empty directory', () => {
      const db = createTestDb()
      const result = db.migrate(migrationsDir)

      expect(result.applied).toHaveLength(0)
      expect(result.skipped).toBe(0)

      db.close()
    })

    it('produces identical results across repeated calls', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('002_create_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')

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
  })

  describe('rollback (file-based)', () => {
    it('undoes the last migration when no version is specified', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('001_create_users.down.sql', 'DROP TABLE users')
      writeMigration('002_create_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      writeMigration('002_create_posts.down.sql', 'DROP TABLE posts')

      db.migrate(migrationsDir)
      const result = db.rollback(migrationsDir)

      expect(result.rolledBack).toHaveLength(1)
      expect(result.rolledBack[0].version).toBe(2)
      expect(result.rolledBack[0].name).toBe('create_posts')

      const tables = db.query<{ name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'posts'",
      )
      expect(tables).toHaveLength(0)

      const tracking = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations ORDER BY version')
      expect(tracking).toHaveLength(1)
      expect(tracking[0].version).toBe(1)

      db.close()
    })

    it('undoes all versions greater than the target version', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('001_create_users.down.sql', 'DROP TABLE users')
      writeMigration('002_create_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      writeMigration('002_create_posts.down.sql', 'DROP TABLE posts')
      writeMigration('003_create_comments.up.sql', 'CREATE TABLE comments (id INTEGER PRIMARY KEY, body TEXT)')
      writeMigration('003_create_comments.down.sql', 'DROP TABLE comments')

      db.migrate(migrationsDir)
      const result = db.rollback(migrationsDir, 1)

      expect(result.rolledBack).toHaveLength(2)
      expect(result.rolledBack[0].version).toBe(3)
      expect(result.rolledBack[1].version).toBe(2)

      const tracking = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations ORDER BY version')
      expect(tracking).toHaveLength(1)
      expect(tracking[0].version).toBe(1)

      db.close()
    })

    it('undoes everything when version is 0', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('001_create_users.down.sql', 'DROP TABLE users')
      writeMigration('002_create_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      writeMigration('002_create_posts.down.sql', 'DROP TABLE posts')

      db.migrate(migrationsDir)
      const result = db.rollback(migrationsDir, 0)

      expect(result.rolledBack).toHaveLength(2)

      const tracking = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations')
      expect(tracking).toHaveLength(0)

      db.close()
    })

    it('throws MIGRATION_NO_DOWN when .down.sql is missing', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      db.migrate(migrationsDir)

      try {
        db.rollback(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_NO_DOWN')
      }

      db.close()
    })

    it('rolls back entire batch atomically when a rollback fails', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('001_create_users.down.sql', 'DROP TABLE users')
      writeMigration('002_create_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)')
      writeMigration('002_create_posts.down.sql', 'THIS IS NOT VALID SQL')

      db.migrate(migrationsDir)

      expect(() => db.rollback(migrationsDir, 0)).toThrow(MigrationError)

      const tracking = db.query<{ version: number }>('SELECT version FROM _sirannon_migrations ORDER BY version')
      expect(tracking).toHaveLength(2)

      db.close()
    })

    it('returns empty result when no migrations are applied', () => {
      const db = createTestDb()
      const result = db.rollback(migrationsDir)
      expect(result.rolledBack).toHaveLength(0)
      db.close()
    })

    it('returns empty result when target version is higher than all applied', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('001_create_users.down.sql', 'DROP TABLE users')

      db.migrate(migrationsDir)
      const result = db.rollback(migrationsDir, 100)

      expect(result.rolledBack).toHaveLength(0)
      db.close()
    })
  })

  describe('programmatic migrations (array input)', () => {
    it('applies SQL string up migrations', () => {
      const db = createTestDb()

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
        { version: 2, name: 'add_email', up: 'ALTER TABLE users ADD COLUMN email TEXT' },
      ]

      const result = db.migrate(migrations)

      expect(result.applied).toHaveLength(2)
      expect(result.applied[0].version).toBe(1)

      db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
      const row = db.queryOne<{ email: string }>('SELECT email FROM users WHERE id = 1')
      expect(row?.email).toBe('alice@test.com')

      db.close()
    })

    it('applies function up migrations', () => {
      const db = createTestDb()

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'create_users',
          up: (tx) => {
            tx.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
            tx.execute("INSERT INTO users (name) VALUES (?)", ['Alice'])
          },
        },
      ]

      const result = db.migrate(migrations)
      expect(result.applied).toHaveLength(1)

      const rows = db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')

      db.close()
    })

    it('applies mixed SQL and function migrations in one batch', () => {
      const db = createTestDb()

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
        {
          version: 2,
          name: 'seed_users',
          up: (tx) => {
            tx.execute("INSERT INTO users (name) VALUES (?)", ['Alice'])
            tx.execute("INSERT INTO users (name) VALUES (?)", ['Bob'])
          },
        },
      ]

      db.migrate(migrations)
      const rows = db.query<{ name: string }>('SELECT name FROM users ORDER BY id')
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')

      db.close()
    })

    it('rolls back using function down migrations', () => {
      const db = createTestDb()

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'create_users',
          up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down: (tx) => {
            tx.execute('DROP TABLE users')
          },
        },
      ]

      db.migrate(migrations)
      const result = db.rollback(migrations)

      expect(result.rolledBack).toHaveLength(1)

      const tables = db.query<{ name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
      )
      expect(tables).toHaveLength(0)

      db.close()
    })

    it('throws MIGRATION_NO_DOWN when down is missing on a migration in rollback path', () => {
      const db = createTestDb()

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      db.migrate(migrations)

      try {
        db.rollback(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_NO_DOWN')
      }

      db.close()
    })
  })

  describe('validation', () => {
    it('throws for version 0', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: 0, name: 'bad', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for negative version', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: -1, name: 'bad', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for NaN version', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: NaN, name: 'bad', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for Infinity version', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: Infinity, name: 'bad', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for non-integer version', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: 1.5, name: 'bad', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for version exceeding MAX_SAFE_INTEGER', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: Number.MAX_SAFE_INTEGER + 1, name: 'bad', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for duplicate versions', () => {
      const db = createTestDb()
      const migrations: Migration[] = [
        { version: 1, name: 'first', up: 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)' },
        { version: 1, name: 'second', up: 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)' },
      ]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_DUPLICATE_VERSION')
      }

      db.close()
    })

    it('throws for empty up string', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: 1, name: 'empty', up: '   ' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for name with non-word characters', () => {
      const db = createTestDb()
      const migrations: Migration[] = [{ version: 1, name: 'bad-name', up: 'SELECT 1' }]

      try {
        db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })
  })

  describe('path security', () => {
    it('throws for path with null byte', () => {
      const db = createTestDb()

      try {
        db.migrate('/some/path\0/migrations')
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      db.close()
    })

    it('throws for path with directory traversal', () => {
      const db = createTestDb()

      try {
        db.migrate('/some/path/../../../etc')
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

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

    it('ignores non-matching files and subdirectories', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
      writeMigration('README.md', '# Migrations')
      writeMigration('.gitkeep', '')
      writeMigration('backup.sql.bak', 'SELECT 1')
      mkdirSync(join(migrationsDir, '002_sneaky.up.sql'))

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

      writeMigration('001_add_column.up.sql', 'ALTER TABLE users ADD COLUMN name TEXT')

      expect(() => readonlyDb.migrate(migrationsDir)).toThrow()

      readonlyDb.close()
    })

    it('rejects migration on a closed database', () => {
      const db = createTestDb()
      db.close()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')

      expect(() => db.migrate(migrationsDir)).toThrow()
    })

    it('includes the failing version number in the error', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('003_broken.up.sql', 'DROP TABLE nonexistent_table')

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

    it('throws MigrationError for empty migration file', () => {
      const db = createTestDb()

      writeMigration('001_empty.up.sql', '')

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

      writeMigration('001_blank.up.sql', '   \n\t  ')

      try {
        db.migrate(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).message).toContain('empty')
      }

      db.close()
    })

    it('throws for duplicate versions with conflicting names in files', () => {
      const db = createTestDb()

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
      writeMigration('001_create_posts.up.sql', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)')

      try {
        db.migrate(migrationsDir)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).version).toBe(1)
        expect((err as MigrationError).code).toBe('MIGRATION_DUPLICATE_VERSION')
      }

      db.close()
    })
  })

  describe('MigrationRunner.run() and MigrationRunner.rollback() (direct)', () => {
    it('run works with a raw better-sqlite3 connection', () => {
      const dbPath = join(tempDir, 'raw.db')
      const raw = new BetterSqlite3(dbPath)

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const result = MigrationRunner.run(raw, migrationsDir)
      expect(result.applied).toHaveLength(1)

      const rows = raw.prepare('SELECT name FROM users').all()
      expect(rows).toEqual([])

      raw.close()
    })

    it('rollback works with a raw better-sqlite3 connection', () => {
      const dbPath = join(tempDir, 'raw-rollback.db')
      const raw = new BetterSqlite3(dbPath)

      writeMigration('001_create_users.up.sql', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      writeMigration('001_create_users.down.sql', 'DROP TABLE users')

      MigrationRunner.run(raw, migrationsDir)
      const result = MigrationRunner.rollback(raw, migrationsDir)

      expect(result.rolledBack).toHaveLength(1)
      expect(result.rolledBack[0].version).toBe(1)

      const tables = raw
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'")
        .all()
      expect(tables).toHaveLength(0)

      raw.close()
    })

    it('run works with programmatic migrations directly', () => {
      const dbPath = join(tempDir, 'raw-prog.db')
      const raw = new BetterSqlite3(dbPath)

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      const result = MigrationRunner.run(raw, migrations)
      expect(result.applied).toHaveLength(1)

      raw.close()
    })
  })
})
