import { describe, expect, it } from 'vitest'
import { MigrationError } from '../../errors.js'
import type { Migration } from '../../migrations/types.js'
import { createTestDb, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

describe('MigrationRunner', () => {
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
})
