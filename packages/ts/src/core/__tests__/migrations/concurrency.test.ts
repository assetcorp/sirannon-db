import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { Database } from '../../database.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { MigrationError } from '../../errors.js'
import { MIGRATIONS_TABLE } from '../../internal-tables.js'
import { isConcurrentMigrationConflict } from '../../migrations/concurrency.js'
import { MigrationRunner } from '../../migrations/runner.js'
import type { Migration } from '../../migrations/types.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, ctx, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

const migrations: Migration[] = [
  { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
]

function lockedError(code?: string): Error {
  const err = new Error('database is locked')
  if (code !== undefined) {
    Object.assign(err, { code })
  }
  return err
}

describe('isConcurrentMigrationConflict', () => {
  it('matches busy codes, locked messages, and history-table unique violations', () => {
    expect(isConcurrentMigrationConflict(lockedError('SQLITE_BUSY'))).toBe(true)
    expect(isConcurrentMigrationConflict(lockedError('SQLITE_BUSY_SNAPSHOT'))).toBe(true)
    expect(isConcurrentMigrationConflict(lockedError())).toBe(true)
    expect(isConcurrentMigrationConflict(new Error('SQLITE_BUSY: database is locked'))).toBe(true)
    expect(isConcurrentMigrationConflict(new Error(`UNIQUE constraint failed: ${MIGRATIONS_TABLE}.version`))).toBe(true)
  })

  it('does not match unrelated errors', () => {
    expect(isConcurrentMigrationConflict(new Error('no such table: users'))).toBe(false)
    expect(isConcurrentMigrationConflict(new Error('UNIQUE constraint failed: users.email'))).toBe(false)
    expect(isConcurrentMigrationConflict(new MigrationError('failed', 1))).toBe(false)
    expect(isConcurrentMigrationConflict('database is locked')).toBe(false)
  })
})

describe('concurrent migrators', () => {
  it('retries once after a transient lock and succeeds', async () => {
    const conn = await testDriver.open(join(ctx.tempDir, 'retry.db'))
    let failuresLeft = 1
    const flaky: SQLiteConnection = {
      exec: sql => conn.exec(sql),
      prepare: sql => conn.prepare(sql),
      close: () => conn.close(),
      transaction: fn => {
        if (failuresLeft > 0) {
          failuresLeft -= 1
          return Promise.reject(lockedError('SQLITE_BUSY'))
        }
        return conn.transaction(fn)
      },
    }

    const result = await MigrationRunner.run(flaky, migrations)
    expect(result.applied.map(m => m.version)).toEqual([1])
    expect(failuresLeft).toBe(0)

    await conn.close()
  })

  it('fails with MIGRATION_CONCURRENT while another connection holds the write lock', async () => {
    const dbPath = join(ctx.tempDir, 'contended.db')

    const holder = await testDriver.open(dbPath)
    await holder.exec('CREATE TABLE placeholder (id INTEGER PRIMARY KEY)')
    await holder.exec('BEGIN IMMEDIATE')
    await holder.exec('INSERT INTO placeholder (id) VALUES (1)')

    const impatientDriver = betterSqlite3({ busyTimeout: 25 })
    const db = await Database.create('contended', dbPath, impatientDriver)

    try {
      await db.migrate(migrations)
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
      expect((err as MigrationError).code).toBe('MIGRATION_CONCURRENT')
    } finally {
      await holder.exec('ROLLBACK')
      await holder.close()
      await db.close()
    }
  })

  it("a losing migrator sees the winner's rows and applies nothing twice", async () => {
    const db = await createTestDb()
    await db.migrate(migrations)

    const second = await db.migrate(migrations)
    expect(second.applied).toEqual([])
    expect(second.skipped).toBe(1)

    await db.close()
  })
})
