import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'

async function createTestDb(): Promise<SQLiteConnection> {
  const conn = await testDriver.open(':memory:')
  await conn.exec('PRAGMA journal_mode = WAL')
  await conn.exec(`
    CREATE TABLE users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT
    )
  `)
  return conn
}

async function insertUser(conn: SQLiteConnection, name: string, email: string | null = null): Promise<void> {
  const stmt = await conn.prepare('INSERT INTO users (name, email) VALUES (?, ?)')
  await stmt.run(name, email)
}

async function changeCount(conn: SQLiteConnection): Promise<number> {
  const stmt = await conn.prepare('SELECT COUNT(*) as cnt FROM _sirannon_changes')
  const row = (await stmt.get()) as { cnt: number }
  return row.cnt
}

async function makeRowsStale(conn: SQLiteConnection): Promise<void> {
  const twoSecondsAgo = Date.now() / 1000 - 2
  const stmt = await conn.prepare('UPDATE _sirannon_changes SET changed_at = ?')
  await stmt.run(twoSecondsAgo)
}

describe('ChangeTracker prune boundary', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker({ retention: 1000 })
    await tracker.watch(conn, 'users')
  })

  afterEach(async () => {
    await conn.close()
  })

  it('does not delete rows above the prune boundary even when old enough', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Carol')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(1n)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(1)
    expect(await changeCount(conn)).toBe(2)
  })

  it('deletes rows at or below the boundary when old enough', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Carol')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(2n)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(2)
    expect(await changeCount(conn)).toBe(1)
  })

  it('deletes all stale rows when boundary covers them all', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(100n)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(2)
    expect(await changeCount(conn)).toBe(0)
  })

  it('preserves fresh rows even when boundary is high', async () => {
    await insertUser(conn, 'Alice')

    tracker.setPruneBoundary(100n)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(0)
    expect(await changeCount(conn)).toBe(1)
  })

  it('works like before when no boundary is set (CDC-only mode)', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')

    await makeRowsStale(conn)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(2)
    expect(await changeCount(conn)).toBe(0)
  })

  it('uses the more restrictive of lastSeq and pruneBoundary', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Carol')
    await insertUser(conn, 'Dave')

    const events = await tracker.poll(conn)
    expect(events).toHaveLength(4)

    await insertUser(conn, 'Eve')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(2n)
    const deleted = await tracker.cleanup(conn)

    expect(deleted).toBe(2)
    expect(await changeCount(conn)).toBe(3)
  })

  it('resumes time-only cleanup after clearPruneBoundary', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(1n)
    const firstDelete = await tracker.cleanup(conn)
    expect(firstDelete).toBe(1)
    expect(await changeCount(conn)).toBe(1)

    tracker.clearPruneBoundary()
    const secondDelete = await tracker.cleanup(conn)
    expect(secondDelete).toBe(1)
    expect(await changeCount(conn)).toBe(0)
  })

  it('uses pruneBoundary alone when poll has not been called', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Carol')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(2n)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(2)
    expect(await changeCount(conn)).toBe(1)
  })

  it('picks lastSeq when lastSeq is lower than pruneBoundary', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Carol')
    await insertUser(conn, 'Dave')
    await insertUser(conn, 'Eve')

    const firstBatch = await tracker.poll(conn)
    expect(firstBatch).toHaveLength(5)

    await insertUser(conn, 'Frank')
    await insertUser(conn, 'Grace')

    await makeRowsStale(conn)

    tracker.setPruneBoundary(100n)

    const deleted = await tracker.cleanup(conn)

    expect(deleted).toBe(5)
    expect(await changeCount(conn)).toBe(2)
  })

  it('picks pruneBoundary when pruneBoundary is lower than lastSeq', async () => {
    await insertUser(conn, 'Alice')
    await insertUser(conn, 'Bob')
    await insertUser(conn, 'Carol')
    await insertUser(conn, 'Dave')
    await insertUser(conn, 'Eve')

    const events = await tracker.poll(conn)
    expect(events).toHaveLength(5)

    await makeRowsStale(conn)

    tracker.setPruneBoundary(3n)

    const deleted = await tracker.cleanup(conn)
    expect(deleted).toBe(3)
    expect(await changeCount(conn)).toBe(2)
  })
})
