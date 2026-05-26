import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ReplicationEngine } from '../../engine.js'
import { ReplicationError } from '../../errors.js'
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

  describe('forwardStatements', () => {
    it('executes locally when the node is writable', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      const result = await engine.forwardStatements([{ sql: "INSERT INTO products (id, name) VALUES (1, 'Widget')" }])

      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)

      await engine.stop()
    })
  })

  describe('forward authorization', () => {
    it('rejects forward from unknown peer', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      await expect(
        harness.transport.triggerForwardReceived(
          { requestId: 'fwd-1', statements: [{ sql: "INSERT INTO items VALUES (1, 'x')" }] },
          'unknown-peer-id',
        ),
      ).rejects.toThrow(ReplicationError)

      await engine.stop()
    })

    it('accepts forward from known peer', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()
      harness.transport.addPeer(NODE_B)

      const result = await harness.transport.triggerForwardReceived(
        { requestId: 'fwd-2', statements: [{ sql: "INSERT INTO items VALUES (1, 'test')" }] },
        NODE_B,
      )

      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)

      await engine.stop()
    })

    it('forward handler not registered on replica', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica') }),
      )
      await engine.start()

      await expect(
        harness.transport.triggerForwardReceived(
          { requestId: 'fwd-3', statements: [{ sql: "INSERT INTO items VALUES (1, 'x')" }] },
          NODE_B,
        ),
      ).rejects.toThrow('No forward handler registered')

      await engine.stop()
    })
  })
})
