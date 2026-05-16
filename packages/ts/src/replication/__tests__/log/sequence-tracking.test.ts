import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { HLC } from '../../hlc.js'
import { ReplicationLog } from '../../log.js'
import { createTestDb, NODE_A, NODE_B, setupTrackerAndTable } from './helpers.js'

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

  describe('sequence tracking', () => {
    it('getLocalSeq returns the highest local sequence', async () => {
      const insertStmt = await conn.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
      await insertStmt.run(1, 'A', 'a@t.com')
      await insertStmt.run(2, 'B', 'b@t.com')

      const seq = await log.getLocalSeq()
      expect(seq).toBe(2n)
    })

    it('getLocalSeq returns 0 when no changes exist', async () => {
      const seq = await log.getLocalSeq()
      expect(seq).toBe(0n)
    })

    it('setLastAppliedSeq and getMinAckedSeq track peer state', async () => {
      await log.setLastAppliedSeq(NODE_B, 10n)

      const stmt = await conn.prepare('SELECT last_acked_seq FROM _sirannon_peer_state WHERE peer_node_id = ?')
      const row = (await stmt.get(NODE_B)) as { last_acked_seq: number } | undefined
      expect(row?.last_acked_seq).toBe(10)

      const minSeq = await log.getMinAckedSeq()
      expect(minSeq).toBe(10n)
    })
  })
})
