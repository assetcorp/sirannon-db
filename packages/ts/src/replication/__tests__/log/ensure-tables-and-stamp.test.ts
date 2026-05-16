import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { HLC } from '../../hlc.js'
import { ReplicationLog } from '../../log.js'
import { createTestDb, NODE_A, setupTrackerAndTable } from './helpers.js'

describe('ReplicationLog', () => {
  let conn: SQLiteConnection
  let hlcA: HLC
  let log: ReplicationLog

  beforeEach(async () => {
    conn = await createTestDb()
    hlcA = new HLC(NODE_A)
    await setupTrackerAndTable(conn)
    log = new ReplicationLog(conn, NODE_A, hlcA)
    await log.ensureReplicationTables()
  })

  afterEach(async () => {
    await conn.close()
  })

  describe('ensureReplicationTables', () => {
    it('creates the _sirannon_peer_state table', async () => {
      const stmt = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_sirannon_peer_state'")
      expect(await stmt.get()).toBeDefined()
    })

    it('creates the _sirannon_applied_changes table', async () => {
      const stmt = await conn.prepare(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_sirannon_applied_changes'",
      )
      expect(await stmt.get()).toBeDefined()
    })

    it('creates the _sirannon_column_versions table', async () => {
      const stmt = await conn.prepare(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_sirannon_column_versions'",
      )
      expect(await stmt.get()).toBeDefined()
    })
  })

  describe('stampChanges', () => {
    it('stamps newly inserted changes with node info', async () => {
      const insertStmt = await conn.prepare("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@t.com')")
      await insertStmt.run()

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0n, 'test-tx-1')
      })

      const selectStmt = await conn.prepare('SELECT node_id, tx_id, hlc FROM _sirannon_changes WHERE seq = 1')
      const row = (await selectStmt.get()) as { node_id: string; tx_id: string; hlc: string } | undefined

      expect(row).toBeDefined()
      expect(row?.node_id).toBe(NODE_A)
      expect(row?.tx_id).toBe('test-tx-1')
      expect(row?.hlc).toBeTruthy()
    })

    it('does not re-stamp already stamped changes', async () => {
      const insertStmt = await conn.prepare("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@t.com')")
      await insertStmt.run()

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0n, 'tx-first')
      })

      const secondInsertStmt = await conn.prepare("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'b@t.com')")
      await secondInsertStmt.run()

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 1n, 'tx-second')
      })

      const firstStmt = await conn.prepare('SELECT tx_id FROM _sirannon_changes WHERE seq = 1')
      const firstRow = (await firstStmt.get()) as { tx_id: string } | undefined
      expect(firstRow?.tx_id).toBe('tx-first')

      const secondStmt = await conn.prepare('SELECT tx_id FROM _sirannon_changes WHERE seq = 2')
      const secondRow = (await secondStmt.get()) as { tx_id: string } | undefined
      expect(secondRow?.tx_id).toBe('tx-second')
    })
  })
})
