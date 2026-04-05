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

describe('ChangeTracker with replication', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker({ replication: true })
  })

  afterEach(async () => {
    await conn.close()
  })

  it('creates a changes table with replication columns', async () => {
    await tracker.watch(conn, 'users')

    const stmt = await conn.prepare("PRAGMA table_info('_sirannon_changes')")
    const columns = (await stmt.all()) as Array<{ name: string }>
    const colNames = columns.map(c => c.name)

    expect(colNames).toContain('node_id')
    expect(colNames).toContain('tx_id')
    expect(colNames).toContain('hlc')
  })

  it('creates indexes on node_id and hlc', async () => {
    await tracker.watch(conn, 'users')

    const stmt = await conn.prepare(
      "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx__sirannon_changes_%'",
    )
    const indexes = (await stmt.all()) as Array<{ name: string }>
    const indexNames = indexes.map(i => i.name)

    expect(indexNames).toContain('idx__sirannon_changes_node_id')
    expect(indexNames).toContain('idx__sirannon_changes_hlc')
    expect(indexNames).toContain('idx__sirannon_changes_changed_at')
  })

  it('triggers populate replication columns with empty string defaults', async () => {
    await tracker.watch(conn, 'users')

    const insertStmt = await conn.prepare("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
    await insertStmt.run()

    const selectStmt = await conn.prepare('SELECT node_id, tx_id, hlc FROM _sirannon_changes')
    const row = (await selectStmt.get()) as { node_id: string; tx_id: string; hlc: string } | undefined

    expect(row).toBeDefined()
    expect(row?.node_id).toBe('')
    expect(row?.tx_id).toBe('')
    expect(row?.hlc).toBe('')
  })

  it('captures INSERT, UPDATE, and DELETE operations with replication columns', async () => {
    await tracker.watch(conn, 'users')

    const insertStmt = await conn.prepare("INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com')")
    await insertStmt.run()

    const updateStmt = await conn.prepare("UPDATE users SET name = 'Robert' WHERE id = 1")
    await updateStmt.run()

    const deleteStmt = await conn.prepare('DELETE FROM users WHERE id = 1')
    await deleteStmt.run()

    const selectStmt = await conn.prepare('SELECT operation, node_id FROM _sirannon_changes ORDER BY seq')
    const rows = (await selectStmt.all()) as Array<{ operation: string; node_id: string }>

    expect(rows).toHaveLength(3)
    expect(rows[0].operation).toBe('INSERT')
    expect(rows[1].operation).toBe('UPDATE')
    expect(rows[2].operation).toBe('DELETE')

    for (const row of rows) {
      expect(row.node_id).toBe('')
    }
  })
})

describe('ChangeTracker without replication (backward compat)', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker()
  })

  afterEach(async () => {
    await conn.close()
  })

  it('does not include replication columns when replication is disabled', async () => {
    await tracker.watch(conn, 'users')

    const stmt = await conn.prepare("PRAGMA table_info('_sirannon_changes')")
    const columns = (await stmt.all()) as Array<{ name: string }>
    const colNames = columns.map(c => c.name)

    expect(colNames).not.toContain('node_id')
    expect(colNames).not.toContain('tx_id')
    expect(colNames).not.toContain('hlc')
  })

  it('still tracks changes normally', async () => {
    await tracker.watch(conn, 'users')

    const insertStmt = await conn.prepare("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')")
    await insertStmt.run()

    const events = await tracker.poll(conn)
    expect(events).toHaveLength(1)
    expect(events[0].type).toBe('insert')
    expect(events[0].table).toBe('users')
  })
})
