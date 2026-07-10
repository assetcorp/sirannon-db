import { describe, expect, it } from 'vitest'
import { MigrationError } from '../../errors.js'
import type { Migration } from '../../migrations/types.js'
import { createTestDb, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

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

      const tables = await db.query<{ name: string }>("SELECT name FROM pragma_table_list WHERE name = 'users'")
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
})
