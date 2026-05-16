import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { LWWResolver } from '../conflict/lww.js'
import { BatchValidationError } from '../errors.js'
import { HLC } from '../hlc.js'
import { canonicaliseForChecksum, ReplicationLog } from '../log.js'
import type { ReplicationBatch, ReplicationChange } from '../types.js'

const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

async function createTestDb(): Promise<SQLiteConnection> {
  const conn = await testDriver.open(':memory:')
  await conn.exec('PRAGMA journal_mode = WAL')
  return conn
}

async function setupTrackerAndTable(conn: SQLiteConnection): Promise<void> {
  const tracker = new ChangeTracker({ replication: true })
  await conn.exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)
	`)
  await tracker.watch(conn, 'users')
}

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
        await log.stampChanges(tx, 0, 'test-tx-1')
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
        await log.stampChanges(tx, 0, 'tx-first')
      })

      const secondInsertStmt = await conn.prepare("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'b@t.com')")
      await secondInsertStmt.run()

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 1, 'tx-second')
      })

      const firstStmt = await conn.prepare('SELECT tx_id FROM _sirannon_changes WHERE seq = 1')
      const firstRow = (await firstStmt.get()) as { tx_id: string } | undefined
      expect(firstRow?.tx_id).toBe('tx-first')

      const secondStmt = await conn.prepare('SELECT tx_id FROM _sirannon_changes WHERE seq = 2')
      const secondRow = (await secondStmt.get()) as { tx_id: string } | undefined
      expect(secondRow?.tx_id).toBe('tx-second')
    })
  })

  describe('readBatch', () => {
    it('produces valid batches with checksums', async () => {
      const insertStmt = await conn.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
      await insertStmt.run(1, 'Alice', 'a@t.com')
      await insertStmt.run(2, 'Bob', 'b@t.com')

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0, 'tx-batch')
      })

      const batch = await log.readBatch(0n, 100)
      expect(batch).not.toBeNull()
      expect(batch?.sourceNodeId).toBe(NODE_A)
      expect(batch?.changes).toHaveLength(2)
      expect(batch?.checksum).toBeTruthy()

      const hash = createHash('sha256')
      hash.update(canonicaliseForChecksum(batch?.changes))
      expect(batch?.checksum).toBe(hash.digest('hex'))
    })

    it('returns null when no changes exist', async () => {
      const batch = await log.readBatch(0n, 100)
      expect(batch).toBeNull()
    })

    it('decodes large integers from the __sirannon_int envelope back to BigInt', async () => {
      await conn.exec('CREATE TABLE counters (id INTEGER PRIMARY KEY, big INTEGER)')
      const intTracker = new ChangeTracker({ replication: true })
      await intTracker.watch(conn, 'counters')

      const big = 9007199254740993n
      const insertStmt = await conn.prepare('INSERT INTO counters (id, big) VALUES (?, ?)')
      await insertStmt.run(1, big)

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0, 'tx-int')
      })

      const batch = await log.readBatch(0n, 100)
      expect(batch).not.toBeNull()
      const change = batch?.changes.find(c => c.table === 'counters')
      expect(change).toBeDefined()
      const decodedBig = change?.newData?.big
      expect(typeof decodedBig).toBe('bigint')
      expect(decodedBig).toBe(big)
    })

    it('decodes BLOB columns from the tagged hex envelope back to Buffer', async () => {
      await conn.exec('CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB)')
      const blobTracker = new ChangeTracker({ replication: true })
      await blobTracker.watch(conn, 'files')

      const payload = Buffer.from([0x10, 0x20, 0x30, 0x00, 0xff])
      const insertStmt = await conn.prepare('INSERT INTO files (id, payload) VALUES (?, ?)')
      await insertStmt.run(1, payload)

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0, 'tx-blob')
      })

      const batch = await log.readBatch(0n, 100)
      expect(batch).not.toBeNull()
      const change = batch?.changes.find(c => c.table === 'files')
      expect(change).toBeDefined()
      const decodedPayload = change?.newData?.payload
      expect(Buffer.isBuffer(decodedPayload)).toBe(true)
      expect((decodedPayload as Buffer).equals(payload)).toBe(true)
    })

    it('respects the afterSeq parameter', async () => {
      const insertStmt = await conn.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
      await insertStmt.run(1, 'Alice', 'a@t.com')

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0, 'tx1')
      })

      await insertStmt.run(2, 'Bob', 'b@t.com')

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 1, 'tx2')
      })

      const batch = await log.readBatch(1n, 100)
      expect(batch).not.toBeNull()
      expect(batch?.changes).toHaveLength(1)
      expect(batch?.changes[0].rowId).toBe('2')
    })
  })

  describe('column version tracking', () => {
    it('tracks column versions on update', async () => {
      const insertStmt = await conn.prepare("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@t.com')")
      await insertStmt.run()

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0, 'tx-insert')
        await log.updateColumnVersions(tx, 0)
      })

      const selectStmt = await conn.prepare(
        "SELECT column_name, hlc FROM _sirannon_column_versions WHERE table_name = 'users' AND row_id = '1'",
      )
      const versions = (await selectStmt.all()) as Array<{ column_name: string; hlc: string }>

      expect(versions.length).toBeGreaterThan(0)
      const colNames = versions.map(v => v.column_name)
      expect(colNames).toContain('id')
      expect(colNames).toContain('name')
      expect(colNames).toContain('email')
    })
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

  describe('canonicaliseForChecksum', () => {
    it('does not throw on BigInt values (unlike raw JSON.stringify)', () => {
      const value = { id: 1n, label: 'x' }
      expect(() => JSON.stringify(value)).toThrow()
      expect(() => canonicaliseForChecksum(value)).not.toThrow()
    })

    it('produces the same string for an integer number and the equivalent BigInt', () => {
      const numberInput = { id: 42, count: 7 }
      const bigintInput = { id: 42n, count: 7n }
      expect(canonicaliseForChecksum(numberInput)).toBe(canonicaliseForChecksum(bigintInput))
    })

    it('produces the same string for a mixed number-and-BigInt object as for the all-BigInt equivalent', () => {
      const mixed = { id: 1, count: 99n, label: 'x' }
      const allBigint = { id: 1n, count: 99n, label: 'x' }
      expect(canonicaliseForChecksum(mixed)).toBe(canonicaliseForChecksum(allBigint))
    })

    it('produces the same string for Uint8Array and Buffer with identical bytes', () => {
      const bytes = [0xde, 0xad, 0xbe, 0xef]
      const arr = new Uint8Array(bytes)
      const buf = Buffer.from(bytes)
      expect(canonicaliseForChecksum({ blob: arr })).toBe(canonicaliseForChecksum({ blob: buf }))
    })

    it('preserves float values as-is (non-integer numbers)', () => {
      const value = { ratio: 3.14 }
      expect(canonicaliseForChecksum(value)).toBe('{"ratio":3.14}')
    })

    it('preserves strings, booleans, and null without alteration', () => {
      const value = { name: 'alice', active: true, ghost: null }
      expect(canonicaliseForChecksum(value)).toBe('{"active":true,"ghost":null,"name":"alice"}')
    })

    it('omits properties with undefined values to match JSON.stringify semantics', () => {
      const withUndefined = { a: 1n, b: undefined }
      const without = { a: 1n }
      expect(canonicaliseForChecksum(withUndefined)).toBe(canonicaliseForChecksum(without))
    })

    it('produces stable output regardless of property insertion order', () => {
      const a = { x: 'first', y: 'second', z: 'third' }
      const b = { z: 'third', y: 'second', x: 'first' }
      expect(canonicaliseForChecksum(a)).toBe(canonicaliseForChecksum(b))
    })

    it('represents undefined elements inside arrays as null (matches JSON.stringify)', () => {
      const value = ['a', undefined, 'c']
      expect(canonicaliseForChecksum(value)).toBe('["a",null,"c"]')
    })
  })

  describe('checksum stability across wire-format round-trip', () => {
    it('produces matching checksums for primary-side and replica-side representations', async () => {
      const hlcVal = hlcA.now()

      const primarySideChange: ReplicationChange = {
        table: 'users',
        operation: 'insert',
        rowId: '1',
        primaryKey: { id: 1 },
        hlc: hlcVal,
        txId: 'tx-1',
        nodeId: NODE_A,
        newData: { id: 1, name: 'Alice', email: null },
        oldData: null,
      }
      const replicaSideChange: ReplicationChange = {
        table: 'users',
        operation: 'insert',
        rowId: '1',
        primaryKey: { id: 1n },
        hlc: hlcVal,
        txId: 'tx-1',
        nodeId: NODE_A,
        newData: { id: 1n, name: 'Alice', email: null },
        oldData: null,
      }

      const primaryChecksum = createHash('sha256')
        .update(canonicaliseForChecksum([primarySideChange]))
        .digest('hex')
      const replicaChecksum = createHash('sha256')
        .update(canonicaliseForChecksum([replicaSideChange]))
        .digest('hex')

      expect(primaryChecksum).toBe(replicaChecksum)
    })

    it('applyBatch accepts a batch whose values have been coerced to BigInt by the wire layer', async () => {
      const remoteHlc = new HLC(NODE_B).now()
      const changes: ReplicationChange[] = [
        {
          table: 'users',
          operation: 'insert',
          rowId: '7',
          primaryKey: { id: 7 },
          hlc: remoteHlc,
          txId: 'tx-remote',
          nodeId: NODE_B,
          newData: { id: 7, name: 'Remote', email: null },
          oldData: null,
        },
      ]
      const primaryChecksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const wireRoundtripped: ReplicationChange[] = changes.map(c => ({
        ...c,
        primaryKey: { id: 7n },
        newData: { id: 7n, name: 'Remote', email: null },
      }))

      const batch: ReplicationBatch = {
        sourceNodeId: NODE_B,
        batchId: `${NODE_B}-1-1`,
        fromSeq: 1n,
        toSeq: 1n,
        hlcRange: { min: remoteHlc, max: remoteHlc },
        changes: wireRoundtripped,
        checksum: primaryChecksum,
      }

      const result = await log.applyBatch(batch, new LWWResolver())
      expect(result.applied).toBe(1)
      expect(result.conflicts).toBe(0)
    })
  })
})
