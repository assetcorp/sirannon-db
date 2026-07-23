import { writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { loadMigrations } from '../../../utils/file-migrations/index.js'
import { MigrationError } from '../../errors.js'
import type { Migration } from '../../migrations/types.js'
import { createTestDb, ctx, registerMigrationsFixtures } from './_helpers.js'

registerMigrationsFixtures()

function writeMigration(version: number, name: string, up: string, down?: string): void {
  writeFileSync(join(ctx.migrationsDir, `${version}_${name}.up.sql`), up)
  if (down !== undefined) {
    writeFileSync(join(ctx.migrationsDir, `${version}_${name}.down.sql`), down)
  }
}

async function tableExists(db: Awaited<ReturnType<typeof createTestDb>>, table: string): Promise<boolean> {
  const rows = await db.query<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", [
    table,
  ])
  return rows.length > 0
}

async function reapplicableVersions(
  db: Awaited<ReturnType<typeof createTestDb>>,
  migrations: Migration[],
): Promise<{ applied: number[]; skipped: number }> {
  const result = await db.migrate(migrations)
  return { applied: result.applied.map(a => a.version), skipped: result.skipped }
}

describe('file-based rollback', () => {
  it('rolls back the last applied migration using its .down.sql', async () => {
    const db = await createTestDb()
    writeMigration(1, 'create_users', 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)', 'DROP TABLE users')
    writeMigration(2, 'add_posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)', 'DROP TABLE posts')

    const migrations = loadMigrations(ctx.migrationsDir)
    await db.migrate(migrations)

    const result = await db.rollback(migrations)

    expect(result.rolledBack.map(r => r.version)).toEqual([2])
    expect(await tableExists(db, 'posts')).toBe(false)
    expect(await tableExists(db, 'users')).toBe(true)
    expect(await reapplicableVersions(db, migrations)).toEqual({ applied: [2], skipped: 1 })

    await db.close()
  })

  it('runs a multi-statement .down.sql fully', async () => {
    const db = await createTestDb()
    writeMigration(
      1,
      'create_schema',
      'CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE posts (id INTEGER PRIMARY KEY);',
      'DROP TABLE posts;\nDROP TABLE users;',
    )

    const migrations = loadMigrations(ctx.migrationsDir)
    await db.migrate(migrations)
    expect(await tableExists(db, 'users')).toBe(true)
    expect(await tableExists(db, 'posts')).toBe(true)

    await db.rollback(migrations, 0)

    expect(await tableExists(db, 'users')).toBe(false)
    expect(await tableExists(db, 'posts')).toBe(false)
    expect(await reapplicableVersions(db, migrations)).toEqual({ applied: [1], skipped: 0 })

    await db.close()
  })

  it('rolls back to a target version', async () => {
    const db = await createTestDb()
    writeMigration(1, 'create_users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)', 'DROP TABLE users')
    writeMigration(2, 'add_posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)', 'DROP TABLE posts')
    writeMigration(3, 'add_tags', 'CREATE TABLE tags (id INTEGER PRIMARY KEY)', 'DROP TABLE tags')

    const migrations = loadMigrations(ctx.migrationsDir)
    await db.migrate(migrations)

    const result = await db.rollback(migrations, 1)

    expect(result.rolledBack.map(r => r.version)).toEqual([3, 2])
    expect(await tableExists(db, 'tags')).toBe(false)
    expect(await tableExists(db, 'posts')).toBe(false)
    expect(await tableExists(db, 'users')).toBe(true)
    expect(await reapplicableVersions(db, migrations)).toEqual({ applied: [2, 3], skipped: 1 })

    await db.close()
  })

  it('rolls back everything with version 0', async () => {
    const db = await createTestDb()
    writeMigration(1, 'create_users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)', 'DROP TABLE users')
    writeMigration(2, 'add_posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)', 'DROP TABLE posts')

    const migrations = loadMigrations(ctx.migrationsDir)
    await db.migrate(migrations)

    const result = await db.rollback(migrations, 0)

    expect(result.rolledBack.map(r => r.version)).toEqual([2, 1])
    expect(await tableExists(db, 'posts')).toBe(false)
    expect(await tableExists(db, 'users')).toBe(false)
    expect(await reapplicableVersions(db, migrations)).toEqual({ applied: [1, 2], skipped: 0 })

    await db.close()
  })

  it('throws MIGRATION_NO_DOWN only for the targeted version that has no .down.sql and leaves state intact', async () => {
    const db = await createTestDb()
    writeMigration(1, 'create_users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)', 'DROP TABLE users')
    writeMigration(2, 'add_posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)')

    const migrations = loadMigrations(ctx.migrationsDir)
    await db.migrate(migrations)

    try {
      await db.rollback(migrations)
      expect.unreachable('should have thrown for the missing down file')
    } catch (err) {
      expect(err).toBeInstanceOf(MigrationError)
      expect((err as MigrationError).code).toBe('MIGRATION_NO_DOWN')
      expect((err as MigrationError).version).toBe(2)
    }

    expect(await tableExists(db, 'posts')).toBe(true)
    expect(await tableExists(db, 'users')).toBe(true)
    expect(await reapplicableVersions(db, migrations)).toEqual({ applied: [], skipped: 2 })

    await db.close()
  })

  it('rolls back a version with a down file even when a lower version lacks one', async () => {
    const db = await createTestDb()
    writeMigration(1, 'create_users', 'CREATE TABLE users (id INTEGER PRIMARY KEY)')
    writeMigration(2, 'add_posts', 'CREATE TABLE posts (id INTEGER PRIMARY KEY)', 'DROP TABLE posts')

    const migrations = loadMigrations(ctx.migrationsDir)
    await db.migrate(migrations)

    const result = await db.rollback(migrations)

    expect(result.rolledBack.map(r => r.version)).toEqual([2])
    expect(await tableExists(db, 'posts')).toBe(false)
    expect(await tableExists(db, 'users')).toBe(true)
    expect(await reapplicableVersions(db, migrations)).toEqual({ applied: [2], skipped: 1 })

    await db.close()
  })
})
