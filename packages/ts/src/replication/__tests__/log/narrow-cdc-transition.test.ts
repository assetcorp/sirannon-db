import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { CHANGES_TABLE } from '../../../core/internal-tables.js'
import { tableColumns } from '../../../core/system-catalog/columns.js'
import { HLC } from '../../hlc.js'
import { ReplicationLog } from '../../log.js'
import { createTestDb, NODE_A } from './helpers.js'

describe('enabling replication on a database with an existing narrow change log', () => {
  let conn: SQLiteConnection
  let tracker: ChangeTracker

  beforeEach(async () => {
    conn = await createTestDb()
    tracker = new ChangeTracker()
    await conn.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
    await tracker.watch(conn, 'users')
    const insert = await conn.prepare("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    await insert.run()
  })

  afterEach(async () => {
    await conn.close()
  })

  it('upgrades the change log to the replication shape, keeping existing rows', async () => {
    const before = await tableColumns(conn, CHANGES_TABLE)
    expect(before.has('node_id')).toBe(false)

    const log = new ReplicationLog(conn, NODE_A, new HLC(NODE_A))
    await log.ensureReplicationTables()

    const after = await tableColumns(conn, CHANGES_TABLE)
    expect(after.has('node_id')).toBe(true)
    expect(after.has('tx_id')).toBe(true)
    expect(after.has('hlc')).toBe(true)

    const rows = await conn.prepare(`SELECT table_name, node_id, tx_id, hlc FROM ${CHANGES_TABLE} ORDER BY seq`)
    expect(await rows.all()).toEqual([{ table_name: 'users', node_id: '', tx_id: '', hlc: '' }])
  })

  it('creates the changed_at, node_id, and hlc indexes during the upgrade', async () => {
    const log = new ReplicationLog(conn, NODE_A, new HLC(NODE_A))
    await log.ensureReplicationTables()

    const stmt = await conn.prepare(
      "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE 'idx_%'",
    )
    const names = ((await stmt.all(CHANGES_TABLE)) as { name: string }[]).map(r => r.name).sort()
    expect(names).toEqual([
      `idx_${CHANGES_TABLE}_changed_at`,
      `idx_${CHANGES_TABLE}_hlc`,
      `idx_${CHANGES_TABLE}_node_id`,
    ])
  })

  it('accepts new change rows written by narrow triggers after the upgrade', async () => {
    const log = new ReplicationLog(conn, NODE_A, new HLC(NODE_A))
    await log.ensureReplicationTables()

    const insert = await conn.prepare("INSERT INTO users (id, name) VALUES (2, 'Bob')")
    await insert.run()

    const rows = await conn.prepare(`SELECT operation, node_id FROM ${CHANGES_TABLE} ORDER BY seq`)
    expect(await rows.all()).toEqual([
      { operation: 'INSERT', node_id: '' },
      { operation: 'INSERT', node_id: '' },
    ])
  })
})
