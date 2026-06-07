import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ReplicationEngine } from '../../engine.js'
import { TopologyError } from '../../errors.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  makeConfig,
  NODE_A,
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

  describe('lifecycle', () => {
    it('starts and stops cleanly', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()

      const status = engine.status()
      expect(status.nodeId).toBe(NODE_A)
      expect(status.replicating).toBe(true)

      await engine.stop()
      expect(engine.status().replicating).toBe(false)
    })

    it('hydrates connected peer ACK state from durable peer progress', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engine.start()
      await engine.log.setLastAppliedSeq(NODE_B, 17n)

      harness.transport.addPeer(NODE_B, 'replica')

      await waitForPeerAck(engine, NODE_B, 17n)
      expect(engine.peerTracker.getPeerState(NODE_B)?.lastAckedSeq).toBe(17n)

      await engine.stop()
    })
  })

  describe('write routing', () => {
    it('throws TopologyError for writes on non-writable nodes without forwarding', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, {
          topology: new PrimaryReplicaTopology('replica'),
          writeForwarding: false,
        }),
      )
      await engine.start()

      await expect(engine.execute("INSERT INTO users VALUES (1, 'x')")).rejects.toThrow(TopologyError)

      await engine.stop()
    })

    it('forwards writes when writeForwarding is enabled', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      harness.transport.addPeer('primary1', 'primary')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, {
          topology: new PrimaryReplicaTopology('replica'),
          writeForwarding: true,
        }),
      )
      await engine.start()

      const result = await engine.execute("INSERT INTO users VALUES (1, 'x')")
      expect(result.changes).toBe(1)

      await engine.stop()
    })

    it('throws TopologyError for transaction on non-writable node', async () => {
      const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig(harness.transport, {
          topology: new PrimaryReplicaTopology('replica'),
        }),
      )
      await engine.start()

      await expect(
        engine.transaction(async tx => {
          await tx.execute("INSERT INTO users VALUES (1, 'x')")
        }),
      ).rejects.toThrow(TopologyError)

      await engine.stop()
    })
  })
})

async function waitForPeerAck(engine: ReplicationEngine, peerId: string, seq: bigint, timeoutMs: number = 250) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    const peer = engine.peerTracker.getPeerState(peerId)
    if (peer && peer.lastAckedSeq >= seq) {
      return
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for peer '${peerId}' to ACK seq ${seq}`)
}
