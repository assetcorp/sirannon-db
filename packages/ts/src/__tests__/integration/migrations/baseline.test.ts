import { afterEach, describe, expect, it } from 'vitest'
import type { Migration } from '../../../core/migrations/types.js'
import { cleanUp, historyTableExists, openDatabase, readFileState, withRawConnection } from './harness.js'

afterEach(cleanUp)

function table(version: number, name: string): Migration {
  return {
    version,
    name: `create_${name}`,
    up: `CREATE TABLE ${name} (id INTEGER PRIMARY KEY)`,
    down: `DROP TABLE ${name}`,
  }
}

const users = table(1, 'users')
const posts = table(2, 'posts')
const tags = table(3, 'tags')
const OLD_HISTORY = [users, posts, tags]

const squashed: Migration = {
  version: 4,
  name: 'squashed_schema',
  up: [
    'CREATE TABLE users (id INTEGER PRIMARY KEY)',
    'CREATE TABLE posts (id INTEGER PRIMARY KEY)',
    'CREATE TABLE tags (id INTEGER PRIMARY KEY)',
    'CREATE TABLE squash_marker (id INTEGER PRIMARY KEY)',
  ].join(';\n'),
  down: 'DROP TABLE users; DROP TABLE posts; DROP TABLE tags; DROP TABLE squash_marker',
  baseline: { through: 3 },
}

const comments = table(5, 'comments')
const FULL_SET = [comments, squashed, tags, users, posts]

describe('a baseline that squashes earlier history', () => {
  it('builds a fresh database from the baseline and the versions above it, without the superseded versions', async () => {
    const { db, path } = await openDatabase()

    const result = await db.migrate(FULL_SET)

    expect(result).toEqual({
      applied: [
        { version: 4, name: 'squashed_schema' },
        { version: 5, name: 'create_comments' },
      ],
      skipped: 3,
    })
    expect(readFileState(path)).toMatchObject({
      userVersion: 5,
      tables: ['comments', 'posts', 'squash_marker', 'tags', 'users'],
    })
    expect(readFileState(path).history.map(row => row.version)).toEqual([4, 5])
    expect(await db.migrate(FULL_SET)).toEqual({ applied: [], skipped: 5 })
  })

  it('never applies the baseline to a database whose history already reaches the squashed versions', async () => {
    const { db, path } = await openDatabase()
    await db.migrate(OLD_HISTORY)

    expect((await db.migrate(FULL_SET)).applied.map(entry => entry.version)).toEqual([5])

    expect(readFileState(path)).toMatchObject({ userVersion: 5, tables: ['comments', 'posts', 'tags', 'users'] })
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2, 3, 5])
  })

  it('bridges a database part-way through the squashed history with the original migrations, then continues', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([users])

    expect((await db.migrate(FULL_SET)).applied.map(entry => entry.version)).toEqual([2, 3, 5])

    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2, 3, 5])
    expect(readFileState(path)).toMatchObject({ userVersion: 5, tables: ['comments', 'posts', 'tags', 'users'] })
  })

  it('refuses with MIGRATION_BASELINE_GAP when the bridging migrations are missing, before it applies anything', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([users])
    const before = readFileState(path)

    for (const set of [
      [squashed, comments],
      [users, squashed, comments],
      [users, posts, squashed, comments],
    ]) {
      await expect(db.migrate(set)).rejects.toMatchObject({ code: 'MIGRATION_BASELINE_GAP', version: 4 })
      expect(readFileState(path)).toEqual(before)
    }
  })

  it('applies only the versions above the baseline once the history has passed the squashed range', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([table(7, 'legacy_reports')])
    const resquashed: Migration = { ...squashed, version: 10, baseline: { through: 5 } }

    const result = await db.migrate([users, posts, comments, resquashed, table(11, 'audits')])

    expect(result.applied.map(entry => entry.version)).toEqual([11])
    expect(readFileState(path)).toMatchObject({ userVersion: 11, tables: ['audits', 'legacy_reports'] })
  })

  it('uses the highest baseline on a fresh database and on a database built from an older baseline', async () => {
    const newer: Migration = {
      version: 9,
      name: 'squashed_again',
      up: 'CREATE TABLE users (id INTEGER PRIMARY KEY); CREATE TABLE posts (id INTEGER PRIMARY KEY); CREATE TABLE second_marker (id INTEGER PRIMARY KEY)',
      baseline: { through: 8 },
    }
    const set = [squashed, comments, table(8, 'drafts'), newer, table(10, 'likes')]
    const fresh = await openDatabase('fresh')
    const older = await openDatabase('older')
    await older.db.migrate([squashed, comments, table(8, 'drafts')])

    expect((await fresh.db.migrate(set)).applied.map(entry => entry.version)).toEqual([9, 10])
    expect((await older.db.migrate(set)).applied.map(entry => entry.version)).toEqual([10])

    expect(readFileState(fresh.path)).toMatchObject({
      userVersion: 10,
      tables: ['likes', 'posts', 'second_marker', 'users'],
    })
    expect(readFileState(older.path).history.map(row => row.version)).toEqual([4, 5, 8, 10])
  })

  it('refuses a through version that is no integer from 1 to below the baseline, and a version inside the squashed range', async () => {
    const { db, path } = await openDatabase()
    const throughs: unknown[] = [0, -1, 1.5, Number.NaN, '3', undefined, null, 4, 5]

    for (const through of throughs) {
      await expect(db.migrate([{ ...squashed, baseline: { through } as Migration['baseline'] }])).rejects.toMatchObject(
        {
          code: 'MIGRATION_VALIDATION_ERROR',
          version: 4,
        },
      )
    }
    for (const baseline of [null, 3, 'through 3']) {
      await expect(db.migrate([{ ...squashed, baseline } as unknown as Migration])).rejects.toMatchObject({
        code: 'MIGRATION_VALIDATION_ERROR',
      })
    }
    await expect(db.migrate([table(4, 'drafts'), { ...squashed, version: 10 }])).rejects.toMatchObject({
      code: 'MIGRATION_VALIDATION_ERROR',
      version: 4,
    })
    expect(historyTableExists(path)).toBe(false)
  })

  it('rolls a database built from the baseline back to empty through the baseline down', async () => {
    const { db, path } = await openDatabase()
    await db.migrate(FULL_SET)

    expect((await db.rollback(FULL_SET, 0)).rolledBack.map(entry => entry.version)).toEqual([5, 4])

    expect(readFileState(path)).toEqual({ userVersion: 0, history: [], tables: [] })
  })
})

describe('the user_version mirror', () => {
  it('restores user_version to the highest recorded version after another writer changes it', async () => {
    const { db, path } = await openDatabase()
    await db.migrate(OLD_HISTORY)

    for (const tampered of [99, 0, -5]) {
      withRawConnection(path, raw => raw.pragma(`user_version = ${tampered}`))
      expect(await db.migrate(OLD_HISTORY)).toEqual({ applied: [], skipped: 3 })
      expect(readFileState(path).userVersion).toBe(3)
    }
  })

  it('applies new versions past a recorded version too large for user_version and leaves user_version alone', async () => {
    const { db, path } = await openDatabase()
    await db.migrate([users])
    withRawConnection(path, raw => {
      raw.prepare('INSERT INTO _sirannon_migrations (version, name) VALUES (?, ?)').run(3_000_000_000, 'legacy_import')
      raw.pragma('user_version = 1')
    })

    expect((await db.migrate([users, posts])).applied.map(entry => entry.version)).toEqual([2])

    expect(readFileState(path)).toMatchObject({ userVersion: 1, tables: ['posts', 'users'] })
    expect(readFileState(path).history.map(row => row.version)).toEqual([1, 2, 3_000_000_000])
  })
})
