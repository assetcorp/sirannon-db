import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { CHANGES_TABLE } from '../../../core/internal-tables.js'
import { LWWResolver } from '../../../core/sync/conflict/lww.js'
import { HLC } from '../../../core/sync/hlc.js'
import { canonicaliseForChecksum, ReplicationLog } from '../../log.js'
import type { ReplicationBatch, ReplicationChange } from '../../types.js'
import { createTestDb, NODE_A, NODE_B, setupTrackerAndTable } from './helpers.js'

describe('applyBatch echo stamping', () => {
  let conn: SQLiteConnection
  let log: ReplicationLog

  beforeEach(async () => {
    conn = await createTestDb()
    await setupTrackerAndTable(conn)
    log = new ReplicationLog(conn, NODE_A, new HLC(NODE_A))
    await log.ensureReplicationTables()
  })

  afterEach(async () => {
    await conn.close()
  })

  function buildBatch(changes: ReplicationChange[], hlcVal: string): ReplicationBatch {
    const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')
    return {
      sourceNodeId: NODE_B,
      batchId: `${NODE_B}-1-${changes.length}`,
      fromSeq: 1n,
      toSeq: BigInt(changes.length),
      hlcRange: { min: hlcVal, max: hlcVal },
      changes,
      checksum,
    }
  }

  it('stamps trigger-generated rows of applied changes with the source identity', async () => {
    const hlcB = new HLC(NODE_B)
    const hlcVal = hlcB.now()
    const changes: ReplicationChange[] = [
      {
        table: 'users',
        operation: 'insert',
        rowId: '10',
        primaryKey: { id: 10 },
        hlc: hlcVal,
        txId: 'remote-tx',
        nodeId: NODE_B,
        newData: { id: 10, name: 'Remote', email: 'r@t.com' },
        oldData: null,
      },
    ]

    await log.applyBatch(buildBatch(changes, hlcVal), new LWWResolver())

    const stmt = await conn.prepare(`SELECT operation, node_id, tx_id, hlc FROM ${CHANGES_TABLE} ORDER BY seq`)
    const rows = (await stmt.all()) as { operation: string; node_id: string; tx_id: string; hlc: string }[]
    expect(rows).toHaveLength(1)
    expect(rows[0].operation).toBe('INSERT')
    expect(rows[0].node_id).toBe(NODE_B)
    expect(rows[0].tx_id).toBe('remote-tx')
    expect(rows[0].hlc).toBe(hlcVal)
  })

  it('leaves pre-existing unstamped local rows untouched', async () => {
    const insertLocal = await conn.prepare("INSERT INTO users (id, name) VALUES (1, 'Local')")
    await insertLocal.run()

    const hlcB = new HLC(NODE_B)
    const hlcVal = hlcB.now()
    const changes: ReplicationChange[] = [
      {
        table: 'users',
        operation: 'insert',
        rowId: '11',
        primaryKey: { id: 11 },
        hlc: hlcVal,
        txId: 'remote-tx-2',
        nodeId: NODE_B,
        newData: { id: 11, name: 'Remote' },
        oldData: null,
      },
    ]

    await log.applyBatch(buildBatch(changes, hlcVal), new LWWResolver())

    const stmt = await conn.prepare(`SELECT node_id FROM ${CHANGES_TABLE} ORDER BY seq`)
    const rows = (await stmt.all()) as { node_id: string }[]
    expect(rows).toHaveLength(2)
    expect(rows[0].node_id).toBe('')
    expect(rows[1].node_id).toBe(NODE_B)
  })
})
