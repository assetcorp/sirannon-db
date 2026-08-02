import { afterEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../driver/types.js'
import {
  APPLIED_CHANGES_TABLE,
  COLUMN_VERSIONS_TABLE,
  META_TABLE,
  PEER_STATE_TABLE,
  SYNC_STATE_TABLE,
} from '../../internal-tables.js'
import { tableColumns } from '../../system-catalog/columns.js'
import { ensureMetaTable, ensureReplicationStateTables } from '../../system-catalog/index.js'
import { testDriver } from '../helpers/test-driver.js'

let conn: SQLiteConnection | undefined

afterEach(async () => {
  await conn?.close()
  conn = undefined
})

describe('ensureReplicationStateTables', () => {
  it('creates all four replication state tables with their expected columns', async () => {
    conn = await testDriver.open(':memory:')
    await ensureReplicationStateTables(conn)

    expect(await tableColumns(conn, PEER_STATE_TABLE)).toEqual(
      new Set(['peer_node_id', 'last_acked_seq', 'last_received_hlc', 'updated_at']),
    )
    expect(await tableColumns(conn, APPLIED_CHANGES_TABLE)).toEqual(
      new Set(['source_node_id', 'source_seq', 'applied_at']),
    )
    expect(await tableColumns(conn, COLUMN_VERSIONS_TABLE)).toEqual(
      new Set(['table_name', 'row_id', 'column_name', 'hlc', 'node_id']),
    )
    expect(await tableColumns(conn, SYNC_STATE_TABLE)).toEqual(
      new Set([
        'table_name',
        'status',
        'row_count',
        'pk_hash',
        'snapshot_seq',
        'source_peer_id',
        'started_at',
        'completed_at',
        'request_id',
      ]),
    )
  })

  it('is idempotent and preserves rows', async () => {
    conn = await testDriver.open(':memory:')
    await ensureReplicationStateTables(conn)

    const insert = await conn.prepare(
      `INSERT INTO ${PEER_STATE_TABLE} (peer_node_id, last_acked_seq, updated_at) VALUES (?, ?, ?)`,
    )
    await insert.run('peer-a', 7, 1700000000)

    await ensureReplicationStateTables(conn)

    const select = await conn.prepare(`SELECT peer_node_id, last_acked_seq FROM ${PEER_STATE_TABLE}`)
    expect(await select.all()).toEqual([{ peer_node_id: 'peer-a', last_acked_seq: 7 }])
  })
})

describe('ensureMetaTable', () => {
  it('creates the key-value meta table idempotently', async () => {
    conn = await testDriver.open(':memory:')
    await ensureMetaTable(conn)
    await ensureMetaTable(conn)

    expect(await tableColumns(conn, META_TABLE)).toEqual(new Set(['key', 'value']))
  })
})
