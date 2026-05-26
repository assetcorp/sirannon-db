import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { LWWResolver } from '../../conflict/lww.js'
import { BatchValidationError } from '../../errors.js'
import { HLC } from '../../hlc.js'
import { canonicaliseForChecksum, ReplicationLog } from '../../log.js'
import type { ReplicationBatch, ReplicationChange } from '../../types.js'
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

  describe('applyBatch', () => {
    it('applies a batch of inserts from a remote node', async () => {
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

      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-1-1`,
        fromSeq: 1n,
        toSeq: 1n,
        hlcRange: { min: hlcVal, max: hlcVal },
        changes,
        checksum,
      }

      const result = await log.applyBatch(batch, new LWWResolver())
      expect(result.applied).toBe(1)
      expect(result.skipped).toBe(0)

      const selectStmt = await conn.prepare('SELECT name FROM users WHERE id = 10')
      const row = (await selectStmt.get()) as { name: string } | undefined
      expect(row?.name).toBe('Remote')
    })

    it('applies the same batch twice without duplicating rows', async () => {
      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()

      const changes: ReplicationChange[] = [
        {
          table: 'users',
          operation: 'insert',
          rowId: '20',
          primaryKey: { id: 20 },
          hlc: hlcVal,
          txId: 'remote-tx-2',
          nodeId: NODE_B,
          newData: { id: 20, name: 'Dup', email: 'd@t.com' },
          oldData: null,
        },
      ]

      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-5-5`,
        fromSeq: 5n,
        toSeq: 5n,
        hlcRange: { min: hlcVal, max: hlcVal },
        changes,
        checksum,
      }

      await log.applyBatch(batch, new LWWResolver())
      const result2 = await log.applyBatch(batch, new LWWResolver())

      expect(result2.applied).toBe(0)
      expect(result2.skipped).toBe(1)

      const countStmt = await conn.prepare('SELECT COUNT(*) as cnt FROM users WHERE id = 20')
      const countRow = (await countStmt.get()) as { cnt: number }
      expect(countRow.cnt).toBe(1)
    })

    it('rejects tampered batches with incorrect checksum', async () => {
      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()

      const changes: ReplicationChange[] = [
        {
          table: 'users',
          operation: 'insert',
          rowId: '30',
          primaryKey: { id: 30 },
          hlc: hlcVal,
          txId: 'remote-tx-3',
          nodeId: NODE_B,
          newData: { id: 30, name: 'Tampered', email: 't@t.com' },
          oldData: null,
        },
      ]

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-10-10`,
        fromSeq: 10n,
        toSeq: 10n,
        hlcRange: { min: hlcVal, max: hlcVal },
        changes,
        checksum: 'bad_checksum_value',
      }

      await expect(log.applyBatch(batch, new LWWResolver())).rejects.toThrow(BatchValidationError)
    })

    it('detects and resolves conflicts using LWW', async () => {
      const insertStmt = await conn.prepare("INSERT INTO users (id, name, email) VALUES (50, 'Local', 'local@t.com')")
      await insertStmt.run()

      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()

      const changes: ReplicationChange[] = [
        {
          table: 'users',
          operation: 'update',
          rowId: '50',
          primaryKey: { id: 50 },
          hlc: hlcVal,
          txId: 'conflict-tx',
          nodeId: NODE_B,
          newData: { id: 50, name: 'Remote', email: 'remote@t.com' },
          oldData: { id: 50, name: 'Local', email: 'local@t.com' },
        },
      ]

      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-15-15`,
        fromSeq: 15n,
        toSeq: 15n,
        hlcRange: { min: hlcVal, max: hlcVal },
        changes,
        checksum,
      }

      const result = await log.applyBatch(batch, new LWWResolver())
      expect(result.conflicts).toBeGreaterThan(0)
    })

    it('rejects unsafe DDL statements', async () => {
      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()

      const changes: ReplicationChange[] = [
        {
          table: '__ddl__',
          operation: 'ddl',
          rowId: '',
          primaryKey: {},
          hlc: hlcVal,
          txId: 'ddl-tx',
          nodeId: NODE_B,
          newData: null,
          oldData: null,
          ddlStatement: 'DELETE FROM users',
        },
      ]

      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-20-20`,
        fromSeq: 20n,
        toSeq: 20n,
        hlcRange: { min: hlcVal, max: hlcVal },
        changes,
        checksum,
      }

      await expect(log.applyBatch(batch, new LWWResolver())).rejects.toThrow(BatchValidationError)
    })

    it('applies safe DDL statements', async () => {
      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()

      const changes: ReplicationChange[] = [
        {
          table: '__ddl__',
          operation: 'ddl',
          rowId: '',
          primaryKey: {},
          hlc: hlcVal,
          txId: 'ddl-tx-safe',
          nodeId: NODE_B,
          newData: null,
          oldData: null,
          ddlStatement: 'CREATE TABLE notes (id INTEGER PRIMARY KEY, content TEXT)',
        },
      ]

      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-25-25`,
        fromSeq: 25n,
        toSeq: 25n,
        hlcRange: { min: hlcVal, max: hlcVal },
        changes,
        checksum,
      }

      const result = await log.applyBatch(batch, new LWWResolver())
      expect(result.applied).toBe(1)

      const stmt = await conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='notes'")
      expect(await stmt.get()).toBeDefined()
    })
  })
})
