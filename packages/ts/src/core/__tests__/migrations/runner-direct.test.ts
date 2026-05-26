import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { MigrationRunner } from '../../migrations/runner.js'
import type { Migration } from '../../migrations/types.js'
import { testDriver } from '../helpers/test-driver.js'
import { ctx, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

describe('MigrationRunner', () => {
  describe('MigrationRunner.run() and MigrationRunner.rollback() (direct)', () => {
    it('run works with a raw connection', async () => {
      const conn = await testDriver.open(join(ctx.tempDir, 'raw.db'))

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
      const conn = await testDriver.open(join(ctx.tempDir, 'raw-rollback.db'))

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
      const conn = await testDriver.open(join(ctx.tempDir, 'raw-prog.db'))

      const migrations: Migration[] = [
        { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)' },
      ]

      const result = await MigrationRunner.run(conn, migrations)
      expect(result.applied).toHaveLength(1)

      await conn.close()
    })
  })
})
