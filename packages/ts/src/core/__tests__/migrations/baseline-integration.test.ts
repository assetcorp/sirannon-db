import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { MigrationError } from '../../errors.js'
import { MIGRATIONS_TABLE } from '../../internal-tables.js'
import type { Migration } from '../../migrations/types.js'
import { testDriver } from '../helpers/test-driver.js'
import { createTestDb, ctx, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

const oldHistory: Migration[] = [
  { version: 1, name: 'create_users', up: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
  { version: 2, name: 'create_posts', up: 'CREATE TABLE posts (id INTEGER PRIMARY KEY)' },
  { version: 3, name: 'create_tags', up: 'CREATE TABLE tags (id INTEGER PRIMARY KEY)' },
]

const baselineMigration: Migration = {
  version: 4,
  name: 'baseline_schema',
  up: [
    'CREATE TABLE users (id INTEGER PRIMARY KEY)',
    'CREATE TABLE posts (id INTEGER PRIMARY KEY)',
    'CREATE TABLE tags (id INTEGER PRIMARY KEY)',
  ].join(';\n'),
  down: 'DROP TABLE users; DROP TABLE posts; DROP TABLE tags',
  baseline: { through: 3 },
}

const postBaseline: Migration = {
  version: 5,
  name: 'create_comments',
  up: 'CREATE TABLE comments (id INTEGER PRIMARY KEY)',
  down: 'DROP TABLE comments',
}

const fullSet: Migration[] = [...oldHistory, baselineMigration, postBaseline]

async function readDbState(dbPath: string): Promise<{ userVersion: number; versions: number[]; tables: Set<string> }> {
  const conn = await testDriver.open(dbPath)
  const versionStmt = await conn.prepare('PRAGMA user_version')
  const versionRow = (await versionStmt.get()) as { user_version: number }
  const rowsStmt = await conn.prepare(`SELECT version FROM ${MIGRATIONS_TABLE} ORDER BY version`)
  const rows = (await rowsStmt.all()) as { version: number }[]
  const tablesStmt = await conn.prepare(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '_sirannon_%' AND name NOT LIKE 'sqlite_%'",
  )
  const tables = (await tablesStmt.all()) as { name: string }[]
  await conn.close()
  return {
    userVersion: Number(versionRow.user_version),
    versions: rows.map(r => r.version),
    tables: new Set(tables.map(t => t.name)),
  }
}

describe('baseline migrations end to end', () => {
  it('creates a fresh database from the baseline alone', async () => {
    const db = await createTestDb()
    const result = await db.migrate(fullSet)
    await db.close()

    expect(result.applied.map(m => m.version)).toEqual([4, 5])

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([4, 5])
    expect(state.userVersion).toBe(5)
    expect(state.tables).toEqual(new Set(['users', 'posts', 'tags', 'comments']))
  })

  it('never executes the baseline on a database with existing history', async () => {
    const db = await createTestDb()
    await db.migrate(oldHistory)
    const result = await db.migrate(fullSet)
    await db.close()

    expect(result.applied.map(m => m.version)).toEqual([5])

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([1, 2, 3, 5])
    expect(state.userVersion).toBe(5)
  })

  it('bridges a mid-history database with the original files before continuing', async () => {
    const db = await createTestDb()
    await db.migrate([oldHistory[0]])
    const result = await db.migrate(fullSet)
    await db.close()

    expect(result.applied.map(m => m.version)).toEqual([2, 3, 5])

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([1, 2, 3, 5])
    expect(state.userVersion).toBe(5)
  })

  it('fails loudly when superseded migrations are missing for a mid-history database', async () => {
    const db = await createTestDb()
    await db.migrate([oldHistory[0]])

    try {
      await db.migrate([baselineMigration, postBaseline])
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
      expect((err as MigrationError).code).toBe('MIGRATION_BASELINE_GAP')
      expect((err as MigrationError).version).toBe(4)
    }

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([1])

    await db.close()
  })

  it('rolls back a baseline-created database through the baseline down', async () => {
    const db = await createTestDb()
    await db.migrate(fullSet)
    const result = await db.rollback([baselineMigration, postBaseline], 0)
    await db.close()

    expect(result.rolledBack.map(m => m.version)).toEqual([5, 4])

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([])
    expect(state.userVersion).toBe(0)
    expect(state.tables).toEqual(new Set())
  })
})

describe('user_version mirroring', () => {
  it('mirrors the highest applied version after each migrate and rollback', async () => {
    const db = await createTestDb()
    await db.migrate(oldHistory)
    await db.rollback(
      oldHistory.map(m => ({ ...m, down: `DROP TABLE ${m.name.slice('create_'.length)}` })),
      1,
    )
    await db.close()

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([1])
    expect(state.userVersion).toBe(1)
  })

  it('backfills user_version on a legacy database with nothing pending', async () => {
    const dbPath = join(ctx.tempDir, 'legacy.db')
    const setup = await testDriver.open(dbPath)
    await setup.exec(
      `CREATE TABLE ${MIGRATIONS_TABLE} (version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at REAL NOT NULL DEFAULT (unixepoch('subsec')), checksum TEXT)`,
    )
    await setup.exec('CREATE TABLE users (id INTEGER PRIMARY KEY)')
    const insert = await setup.prepare(`INSERT INTO ${MIGRATIONS_TABLE} (version, name) VALUES (?, ?)`)
    await insert.run(1, 'create_users')
    await setup.close()

    const db = await Database.create('legacy', dbPath, testDriver)
    const result = await db.migrate([oldHistory[0]])
    await db.close()

    expect(result.applied).toEqual([])

    const state = await readDbState(dbPath)
    expect(state.userVersion).toBe(1)
  })

  it('leaves user_version untouched when a migration fails', async () => {
    const db = await createTestDb()
    await db.migrate([oldHistory[0]])

    try {
      await db.migrate([oldHistory[0], { version: 2, name: 'broken', up: 'CREATE SYNTAX ERROR' }])
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
    }

    await db.close()

    const state = await readDbState(join(ctx.tempDir, 'test.db'))
    expect(state.versions).toEqual([1])
    expect(state.userVersion).toBe(1)
  })

  it('rejects migration versions above the user_version ceiling', async () => {
    const db = await createTestDb()

    try {
      await db.migrate([{ version: 2_147_483_648, name: 'too_big', up: 'CREATE TABLE t (id)' }])
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
      expect((err as MigrationError).code).toBe('MIGRATION_VALIDATION_ERROR')
    }

    await db.close()
  })
})
