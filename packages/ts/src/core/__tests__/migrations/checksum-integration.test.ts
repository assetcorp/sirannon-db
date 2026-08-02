import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { MigrationError } from '../../errors.js'
import { MIGRATIONS_TABLE } from '../../internal-tables.js'
import type { Migration } from '../../migrations/types.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, ctx, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

describe('migration checksum verification', () => {
  it('re-migrating an unchanged set does not throw', async () => {
    const db = await createTestDb()
    const migrations: Migration[] = [
      { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
    ]

    await db.migrate(migrations)
    const second = await db.migrate(migrations)

    expect(second.applied).toEqual([])
    expect(second.skipped).toBe(1)

    await db.close()
  })

  it('rejects a migration whose up SQL changed after it was applied', async () => {
    const db = await createTestDb()
    await db.migrate([{ version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' }])

    try {
      await db.migrate([
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ])
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
      expect((err as MigrationError).code).toBe('MIGRATION_CHECKSUM_MISMATCH')
      expect((err as MigrationError).version).toBe(1)
    }

    await db.close()
  })

  it('ignores whitespace-only differences in an applied migration', async () => {
    const db = await createTestDb()
    await db.migrate([{ version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' }])

    const second = await db.migrate([
      { version: 1, name: 'create_users', up: '  CREATE TABLE users (id INTEGER PRIMARY KEY)\n' },
    ])

    expect(second.applied).toEqual([])
    expect(second.skipped).toBe(1)

    await db.close()
  })

  it('does not checksum-check a migration that was removed from the input', async () => {
    const db = await createTestDb()
    await db.migrate([
      { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
      { version: 2, name: 'add_posts', up: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)' },
    ])

    const second = await db.migrate([
      { version: 2, name: 'add_posts', up: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)' },
    ])

    expect(second.applied).toEqual([])
    expect(second.skipped).toBe(1)

    await db.close()
  })

  it('does not verify function-based migrations', async () => {
    const db = await createTestDb()
    const migration: Migration = {
      version: 1,
      name: 'create_users',
      up: async tx => {
        await tx.execute('CREATE TABLE users (id INTEGER PRIMARY KEY)')
      },
    }

    await db.migrate([migration])
    const second = await db.migrate([migration])

    expect(second.applied).toEqual([])
    expect(second.skipped).toBe(1)

    await db.close()
  })

  it('backfills a null checksum on upgrade and enforces it on the next migrate', async () => {
    const dbPath = join(ctx.tempDir, 'backfill.db')

    const setup = await testDriver.open(dbPath)
    await setup.exec(
      `CREATE TABLE ${MIGRATIONS_TABLE} (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at REAL NOT NULL DEFAULT (unixepoch('subsec')))`,
    )
    await setup.exec('CREATE TABLE users (id INTEGER PRIMARY KEY)')
    const insert = await setup.prepare(`INSERT INTO ${MIGRATIONS_TABLE} (version, name) VALUES (?, ?)`)
    await insert.run(1, 'create_users')
    await setup.close()

    const db = await Database.create('backfill', dbPath, testDriver)

    const first = await db.migrate([
      { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
    ])
    expect(first.applied).toEqual([])
    expect(first.skipped).toBe(1)

    try {
      await db.migrate([
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ])
      expect.unreachable('should have thrown after backfill')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
      expect((err as MigrationError).code).toBe('MIGRATION_CHECKSUM_MISMATCH')
    }

    await db.close()
  })
})
