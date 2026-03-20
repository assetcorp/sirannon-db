import { mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../database.js'
import { MigrationError } from '../errors.js'
import { MigrationRunner } from '../migrations/runner.js'
import type { Migration } from '../migrations/types.js'
import { testDriver } from './helpers/test-driver.js'

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

async function createTestDb(): Promise<Database> {
  const dbPath = join(tempDir, 'test.db')
  return Database.create('test', dbPath, testDriver)
}

describe('MigrationRunner', () => {
  describe('programmatic migrations (array input)', () => {
    it('applies SQL string up migrations', async () => {
      const db = await createTestDb()

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
        { version: 2, name: 'add_email', up: 'ALTER TABLE users ADD COLUMN email TEXT' },
      ]

      const result = await db.migrate(migrations)

      expect(result.applied).toHaveLength(2)
      expect(result.applied[0].version).toBe(1)

      await db.execute("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
      const row = await db.queryOne<{ email: string }>('SELECT email FROM users WHERE id = 1')
      expect(row?.email).toBe('alice@test.com')

      await db.close()
    })

    it('applies function up migrations', async () => {
      const db = await createTestDb()

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'create_users',
          up: async tx => {
            await tx.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
            await tx.execute('INSERT INTO users (name) VALUES (?)', ['Alice'])
          },
        },
      ]

      const result = await db.migrate(migrations)
      expect(result.applied).toHaveLength(1)

      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('Alice')

      await db.close()
    })

    it('applies mixed SQL and function migrations in one batch', async () => {
      const db = await createTestDb()

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
        {
          version: 2,
          name: 'seed_users',
          up: async tx => {
            await tx.execute('INSERT INTO users (name) VALUES (?)', ['Alice'])
            await tx.execute('INSERT INTO users (name) VALUES (?)', ['Bob'])
          },
        },
      ]

      await db.migrate(migrations)
      const rows = await db.query<{ name: string }>('SELECT name FROM users ORDER BY id')
      expect(rows).toHaveLength(2)
      expect(rows[0].name).toBe('Alice')
      expect(rows[1].name).toBe('Bob')

      await db.close()
    })

    it('skips already-applied migrations on second run', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      const first = await db.migrate(migrations)
      expect(first.applied).toHaveLength(1)
      expect(first.skipped).toBe(0)

      const second = await db.migrate(migrations)
      expect(second.applied).toHaveLength(0)
      expect(second.skipped).toBe(1)

      await db.close()
    })

    it('rolls back using function down migrations', async () => {
      const db = await createTestDb()

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'create_users',
          up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down: async tx => {
            await tx.execute('DROP TABLE users')
          },
        },
      ]

      await db.migrate(migrations)
      const result = await db.rollback(migrations)

      expect(result.rolledBack).toHaveLength(1)

      const tables = await db.query<{ name: string }>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'",
      )
      expect(tables).toHaveLength(0)

      await db.close()
    })

    it('throws MIGRATION_NO_DOWN when down is missing on a migration in rollback path', async () => {
      const db = await createTestDb()

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      await db.migrate(migrations)

      try {
        await db.rollback(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_NO_DOWN')
      }

      await db.close()
    })

    it('rethrows MigrationError from programmatic up migration', async () => {
      const db = await createTestDb()
      const original = new MigrationError('denied', 1, 'MIGRATION_ERROR')
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'denied',
          up: () => {
            throw original
          },
        },
      ]

      await expect(db.migrate(migrations)).rejects.toThrow(original)
      await db.close()
    })

    it('wraps non-Error throws from programmatic up migration', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'string_throw',
          up: () => {
            throw 'up string failure'
          },
        },
      ]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).message).toContain('up string failure')
      }

      await db.close()
    })

    it('rethrows MigrationError from programmatic down migration', async () => {
      const db = await createTestDb()
      const original = new MigrationError('down denied', 1, 'MIGRATION_ROLLBACK_ERROR')
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'down_denied',
          up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down: () => {
            throw original
          },
        },
      ]

      await db.migrate(migrations)
      await expect(db.rollback(migrations)).rejects.toThrow(original)
      await db.close()
    })

    it('wraps non-Error throws from programmatic down migration', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'down_string',
          up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down: () => {
            throw 'down string failure'
          },
        },
      ]

      await db.migrate(migrations)

      try {
        await db.rollback(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).message).toContain('down string failure')
      }

      await db.close()
    })
  })

  describe('validation', () => {
    it('throws for version 0', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: 0, name: 'bad', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for negative version', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: -1, name: 'bad', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for NaN version', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: NaN, name: 'bad', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for Infinity version', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: Infinity, name: 'bad', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for non-integer version', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: 1.5, name: 'bad', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for version exceeding MAX_SAFE_INTEGER', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: Number.MAX_SAFE_INTEGER + 1, name: 'bad', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws rollback validation error for non-finite target version', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
      ]
      await db.migrate(migrations)

      try {
        await db.rollback(migrations, Number.NaN as unknown as number)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for duplicate versions', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [
        { version: 1, name: 'first', up: 'CREATE TABLE t1 (id INTEGER PRIMARY KEY)' },
        { version: 1, name: 'second', up: 'CREATE TABLE t2 (id INTEGER PRIMARY KEY)' },
      ]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_DUPLICATE_VERSION')
      }

      await db.close()
    })

    it('throws for empty up string', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: 1, name: 'empty', up: '   ' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for empty down string', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [
        {
          version: 1,
          name: 'create_users',
          up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down: '   ',
        },
      ]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for name with non-word characters', async () => {
      const db = await createTestDb()
      const migrations: Migration[] = [{ version: 1, name: 'bad-name', up: 'SELECT 1' }]

      try {
        await db.migrate(migrations)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })

    it('throws for negative rollback target version', async () => {
      const db = await createTestDb()

      try {
        await db.rollback([], -1)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(MigrationError)
        expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
      }

      await db.close()
    })
  })

  describe('MigrationRunner.run() and MigrationRunner.rollback() (direct)', () => {
    it('run works with a raw connection', async () => {
      const conn = await testDriver.open(join(tempDir, 'raw.db'))

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      const result = await MigrationRunner.run(conn, migrations)
      expect(result.applied).toHaveLength(1)

      const stmt = await conn.prepare('SELECT name FROM users')
      const rows = await stmt.all()
      expect(rows).toEqual([])

      await conn.close()
    })

    it('rollback works with a raw connection', async () => {
      const conn = await testDriver.open(join(tempDir, 'raw-rollback.db'))

      const migrations: Migration[] = [
        {
          version: 1,
          name: 'create_users',
          up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down: 'DROP TABLE users',
        },
      ]

      await MigrationRunner.run(conn, migrations)
      const result = await MigrationRunner.rollback(conn, migrations)

      expect(result.rolledBack).toHaveLength(1)
      expect(result.rolledBack[0].version).toBe(1)

      const stmt = await conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'")
      const tables = await stmt.all()
      expect(tables).toHaveLength(0)

      await conn.close()
    })

    it('run works with programmatic migrations directly', async () => {
      const conn = await testDriver.open(join(tempDir, 'raw-prog.db'))

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      const result = await MigrationRunner.run(conn, migrations)
      expect(result.applied).toHaveLength(1)

      await conn.close()
    })
  })
})
