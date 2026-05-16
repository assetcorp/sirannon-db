import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { HLC } from '../../hlc.js'
import { canonicaliseForChecksum, ReplicationLog } from '../../log.js'
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

  describe('readBatch', () => {
    it('produces valid batches with checksums', async () => {
      const insertStmt = await conn.prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)')
      await insertStmt.run(1, 'Alice', 'a@t.com')
      await insertStmt.run(2, 'Bob', 'b@t.com')

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 0n, 'tx-batch')
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
        await log.stampChanges(tx, 0n, 'tx-int')
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
        await log.stampChanges(tx, 0n, 'tx-blob')
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
        await log.stampChanges(tx, 0n, 'tx1')
      })

      await insertStmt.run(2, 'Bob', 'b@t.com')

      await conn.transaction(async tx => {
        await log.stampChanges(tx, 1n, 'tx2')
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
        await log.stampChanges(tx, 0n, 'tx-insert')
        await log.updateColumnVersions(tx, 0n)
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
})
