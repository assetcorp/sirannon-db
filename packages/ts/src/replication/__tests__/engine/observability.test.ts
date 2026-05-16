import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ReplicationEngine } from '../../engine.js'
import { HLC } from '../../hlc.js'
import { canonicaliseForChecksum } from '../../log.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  makeConfig,
  NODE_B,
  teardownHarness,
} from './helpers.js'

describe('ReplicationEngine', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  describe('observability', () => {
    it('returns 0 from getCurrentSeq before start', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))

      expect(engine.getCurrentSeq()).toBe(0n)
    })

    it('returns 0 from getAppliedSeq for unknown peers', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      expect(engine.getAppliedSeq('unknown-peer')).toBe(0n)
      expect(engine.getAppliedSeq(NODE_B)).toBe(0n)

      await engine.stop()
    })

    it('advances getCurrentSeq after a local write', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      const before = engine.getCurrentSeq()
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
      const after = engine.getCurrentSeq()

      expect(after).toBeGreaterThan(before)

      await engine.stop()
    })

    it('reports the same seq for the same write across repeated reads', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
      const first = engine.getCurrentSeq()
      const second = engine.getCurrentSeq()
      const third = engine.getCurrentSeq()

      expect(first).toBe(second)
      expect(second).toBe(third)

      await engine.stop()
    })

    it('updates getAppliedSeq after a remote batch is applied', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica') }),
      )
      await engine.start()
      harness.transport.addPeer(NODE_B, 'primary')

      expect(engine.getAppliedSeq(NODE_B)).toBe(0n)

      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()
      const changes = [
        {
          table: 'users',
          operation: 'insert' as const,
          rowId: '7',
          primaryKey: { id: 7 },
          hlc: hlcVal,
          txId: 'remote-tx',
          nodeId: NODE_B,
          newData: { id: 7, name: 'Remote' },
          oldData: null,
        },
      ]
      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      await harness.transport.triggerBatchReceived(
        {
          sourceNodeId: NODE_B,
          batchId: `${NODE_B}-1-1`,
          fromSeq: 1n,
          toSeq: 1n,
          hlcRange: { min: hlcVal, max: hlcVal },
          changes,
          checksum,
        },
        NODE_B,
      )

      expect(engine.getAppliedSeq(NODE_B)).toBe(1n)
      expect(engine.getAppliedSeq('different-peer')).toBe(0n)

      await engine.stop()
    })

    it('does not regress getCurrentSeq if reads are interleaved with writes', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      const seqs: bigint[] = []
      for (let i = 1; i <= 5; i++) {
        await engine.execute(`INSERT INTO users (id, name) VALUES (${i}, 'u${i}')`)
        seqs.push(engine.getCurrentSeq())
      }

      for (let i = 1; i < seqs.length; i++) {
        expect(seqs[i]).toBeGreaterThanOrEqual(seqs[i - 1])
      }
      expect(seqs[seqs.length - 1]).toBeGreaterThan(0n)

      await engine.stop()
    })

    it('preserves getCurrentSeq across restart by reading from durable state', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engineA = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engineA.start()
      await engineA.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
      const seqA = engineA.getCurrentSeq()
      await engineA.stop()

      expect(seqA).toBeGreaterThan(0n)

      const engineB = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engineB.start()
      const seqB = engineB.getCurrentSeq()
      await engineB.stop()

      expect(seqB).toBe(seqA)
    })
  })
})
