import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { LWWResolver } from '../../../core/sync/conflict/lww.js'
import { HLC } from '../../../core/sync/hlc.js'
import { canonicaliseForChecksum, ReplicationLog } from '../../log.js'
import type { ReplicationBatch, ReplicationChange } from '../../types.js'
import { createTestDb, NODE_A, NODE_B } from './helpers.js'

function checksumOf(changes: ReplicationChange[]): string {
  return createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')
}

interface InsertedRow {
  table_name: string
  operation: string
  new_data: string | null
}

describe('ReplicationLog.applyBatch inbound DDL trigger refresh', () => {
  let conn: SQLiteConnection
  let hlcA: HLC

  beforeEach(async () => {
    conn = await createTestDb()
    hlcA = new HLC(NODE_A)
  })

  afterEach(async () => {
    await conn.close()
  })

  async function setupReplicaState(): Promise<{ log: ReplicationLog; tracker: ChangeTracker }> {
    const tracker = new ChangeTracker({ replication: true })
    await conn.exec('CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)')
    await tracker.watch(conn, 'inventory')

    const log = new ReplicationLog(conn, NODE_A, hlcA, '_sirannon_changes', tracker)
    await log.ensureReplicationTables()
    return { log, tracker }
  }

  it('refreshes triggers between DDL and DML so CDC captures the new column', async () => {
    const { log } = await setupReplicaState()
    const hlcB = new HLC(NODE_B)
    const hlcVal = hlcB.now()

    const changes: ReplicationChange[] = [
      {
        table: '__ddl__',
        operation: 'ddl',
        rowId: '',
        primaryKey: {},
        hlc: hlcVal,
        txId: 'tx-ddl-and-dml',
        nodeId: NODE_B,
        newData: null,
        oldData: null,
        ddlStatement: 'ALTER TABLE inventory ADD COLUMN bar TEXT',
      },
      {
        table: 'inventory',
        operation: 'insert',
        rowId: '1',
        primaryKey: { id: 1 },
        hlc: hlcVal,
        txId: 'tx-ddl-and-dml',
        nodeId: NODE_B,
        newData: { id: 1, sku: 'sku-001', bar: 'value-bar' },
        oldData: null,
      },
    ]
    const batch: ReplicationBatch = {
      sourceNodeId: NODE_B,
      batchId: `${NODE_B}-1-2`,
      fromSeq: 1n,
      toSeq: 2n,
      hlcRange: { min: hlcVal, max: hlcVal },
      changes,
      checksum: checksumOf(changes),
    }

    const result = await log.applyBatch(batch, new LWWResolver())
    expect(result.applied).toBe(2)

    const cdcStmt = await conn.prepare(
      "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
    )
    const row = (await cdcStmt.get()) as { new_data: string } | undefined
    expect(row).toBeDefined()
    const parsed = JSON.parse(row?.new_data ?? '{}') as { id: number; sku: string; bar: string }
    expect(parsed).toEqual({ id: 1, sku: 'sku-001', bar: 'value-bar' })
  })

  it('refreshes between consecutive ALTERs so a trailing DML captures every new column', async () => {
    const { log } = await setupReplicaState()
    const hlcB = new HLC(NODE_B)
    const hlcVal = hlcB.now()

    const changes: ReplicationChange[] = [
      {
        table: '__ddl__',
        operation: 'ddl',
        rowId: '',
        primaryKey: {},
        hlc: hlcVal,
        txId: 'tx-two-alters',
        nodeId: NODE_B,
        newData: null,
        oldData: null,
        ddlStatement: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER',
      },
      {
        table: '__ddl__',
        operation: 'ddl',
        rowId: '',
        primaryKey: {},
        hlc: hlcVal,
        txId: 'tx-two-alters',
        nodeId: NODE_B,
        newData: null,
        oldData: null,
        ddlStatement: 'ALTER TABLE inventory ADD COLUMN status TEXT',
      },
      {
        table: 'inventory',
        operation: 'insert',
        rowId: '7',
        primaryKey: { id: 7 },
        hlc: hlcVal,
        txId: 'tx-two-alters',
        nodeId: NODE_B,
        newData: { id: 7, sku: 'sku-007', quantity: 12, status: 'shipped' },
        oldData: null,
      },
    ]
    const batch: ReplicationBatch = {
      sourceNodeId: NODE_B,
      batchId: `${NODE_B}-10-12`,
      fromSeq: 10n,
      toSeq: 12n,
      hlcRange: { min: hlcVal, max: hlcVal },
      changes,
      checksum: checksumOf(changes),
    }

    await log.applyBatch(batch, new LWWResolver())

    const cdcStmt = await conn.prepare(
      "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
    )
    const row = (await cdcStmt.get()) as { new_data: string } | undefined
    const parsed = JSON.parse(row?.new_data ?? '{}') as Record<string, unknown>
    expect(parsed).toEqual({ id: 7, sku: 'sku-007', quantity: 12, status: 'shipped' })
  })

  it('rolls back the schema and surviving CDC reflects the original columns when DML inside the batch fails', async () => {
    const { log, tracker } = await setupReplicaState()

    const colsBefore = (
      (await (await conn.prepare('PRAGMA table_info(inventory)')).all()) as Array<{ name: string }>
    ).map(c => c.name)

    const hlcB = new HLC(NODE_B)
    const hlcVal = hlcB.now()
    const badChanges: ReplicationChange[] = [
      {
        table: '__ddl__',
        operation: 'ddl',
        rowId: '',
        primaryKey: {},
        hlc: hlcVal,
        txId: 'tx-rollback',
        nodeId: NODE_B,
        newData: null,
        oldData: null,
        ddlStatement: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER CHECK (quantity >= 0)',
      },
      {
        table: 'inventory',
        operation: 'insert',
        rowId: '42',
        primaryKey: { id: 42 },
        hlc: hlcVal,
        txId: 'tx-rollback',
        nodeId: NODE_B,
        newData: { id: 42, sku: 'sku-bad', quantity: -5 },
        oldData: null,
      },
    ]
    const badBatch: ReplicationBatch = {
      sourceNodeId: NODE_B,
      batchId: `${NODE_B}-20-21`,
      fromSeq: 20n,
      toSeq: 21n,
      hlcRange: { min: hlcVal, max: hlcVal },
      changes: badChanges,
      checksum: checksumOf(badChanges),
    }

    await expect(log.applyBatch(badBatch, new LWWResolver())).rejects.toThrow()

    const colsAfter = (
      (await (await conn.prepare('PRAGMA table_info(inventory)')).all()) as Array<{ name: string }>
    ).map(c => c.name)
    expect(colsAfter).toEqual(colsBefore)
    expect(colsAfter).not.toContain('quantity')

    expect(tracker.watchedTables.has('inventory')).toBe(true)

    const followInsert = await conn.prepare("INSERT INTO inventory (id, sku) VALUES (2, 'sku-002')")
    await followInsert.run()

    const followStmt = await conn.prepare(
      "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
    )
    const followRow = (await followStmt.get()) as InsertedRow | undefined
    expect(followRow).toBeDefined()
    const followParsed = JSON.parse(followRow?.new_data ?? '{}') as Record<string, unknown>
    expect(followParsed).toEqual({ id: 2, sku: 'sku-002' })
    expect(followParsed).not.toHaveProperty('quantity')
  })

  it('prunes the dropped table while refreshing triggers for surviving tables in the same batch', async () => {
    const tracker = new ChangeTracker({ replication: true })
    await conn.exec('CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)')
    await conn.exec('CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL)')
    await tracker.watch(conn, 'inventory')
    await tracker.watch(conn, 'accounts')

    const log = new ReplicationLog(conn, NODE_A, hlcA, '_sirannon_changes', tracker)
    await log.ensureReplicationTables()

    const hlcB = new HLC(NODE_B)
    const hlcVal = hlcB.now()
    const changes: ReplicationChange[] = [
      {
        table: '__ddl__',
        operation: 'ddl',
        rowId: '',
        primaryKey: {},
        hlc: hlcVal,
        txId: 'tx-drop-and-alter',
        nodeId: NODE_B,
        newData: null,
        oldData: null,
        ddlStatement: 'DROP TABLE inventory',
      },
      {
        table: '__ddl__',
        operation: 'ddl',
        rowId: '',
        primaryKey: {},
        hlc: hlcVal,
        txId: 'tx-drop-and-alter',
        nodeId: NODE_B,
        newData: null,
        oldData: null,
        ddlStatement: 'ALTER TABLE accounts ADD COLUMN balance INTEGER',
      },
      {
        table: 'accounts',
        operation: 'insert',
        rowId: '1',
        primaryKey: { id: 1 },
        hlc: hlcVal,
        txId: 'tx-drop-and-alter',
        nodeId: NODE_B,
        newData: { id: 1, holder: 'alice', balance: 500 },
        oldData: null,
      },
    ]
    const batch: ReplicationBatch = {
      sourceNodeId: NODE_B,
      batchId: `${NODE_B}-30-32`,
      fromSeq: 30n,
      toSeq: 32n,
      hlcRange: { min: hlcVal, max: hlcVal },
      changes,
      checksum: checksumOf(changes),
    }

    const result = await log.applyBatch(batch, new LWWResolver())
    expect(result.applied).toBe(3)
    expect(result.droppedTables).toEqual(['inventory'])

    expect(tracker.watchedTables.has('accounts')).toBe(true)

    const cdcStmt = await conn.prepare(
      "SELECT new_data FROM _sirannon_changes WHERE table_name = 'accounts' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
    )
    const row = (await cdcStmt.get()) as { new_data: string } | undefined
    expect(row).toBeDefined()
    const parsed = JSON.parse(row?.new_data ?? '{}') as Record<string, unknown>
    expect(parsed).toEqual({ id: 1, holder: 'alice', balance: 500 })
  })
})
