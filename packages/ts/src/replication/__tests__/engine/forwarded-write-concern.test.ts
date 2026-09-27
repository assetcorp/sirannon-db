import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C } from './coordinator-helpers.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  makeConfig,
  NODE_A,
  NODE_B,
  teardownHarness,
} from './helpers.js'

async function startCoordinatorPrimary(harness: EngineTestHarness): Promise<{
  engine: ReplicationEngine
  coordinator: InMemoryClusterCoordinator
}> {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
  const coordinator = new InMemoryClusterCoordinator()
  await coordinator.setReplicationGroupState({
    clusterId: 'cluster-a',
    groupId: 'orders',
    votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
    currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
    primaryTerm: 3n,
    inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
  })
  const engine = new ReplicationEngine(db, conn, {
    nodeId: NODE_A,
    topology: new PrimaryReplicaTopology('primary'),
    transport: harness.transport,
    initialSync: false,
    coordinator: { clusterId: 'cluster-a', groupId: 'orders', coordinator, controller: false },
  })
  await engine.start()
  harness.transport.addPeer(NODE_B)
  engine.peerTracker.addPeer(NODE_B)
  engine.peerTracker.addPeer(NODE_C)
  return { engine, coordinator }
}

describe('write concern on a forwarded write', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('waits for a majority on a coordinator-mode primary when the forwarded write states no concern', async () => {
    const { engine, coordinator } = await startCoordinatorPrimary(harness)
    engine.peerTracker.onAckReceived(NODE_B, 1n)

    await harness.transport.triggerForwardReceived(
      {
        statements: [{ sql: "INSERT INTO items (name) VALUES ('alpha')" }],
        requestId: 'forward-default-concern',
        groupId: 'orders',
        primaryTerm: 3n,
      },
      NODE_B,
    )

    const state = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(state?.durabilityPointSeq).toBe(1n)
    await engine.stop()
  })

  it('fails the forwarded write with WRITE_CONCERN_ERROR when the stated concern goes unmet', async () => {
    const { engine } = await startCoordinatorPrimary(harness)

    await expect(
      harness.transport.triggerForwardReceived(
        {
          statements: [{ sql: "INSERT INTO items (name) VALUES ('alpha')" }],
          requestId: 'forward-stated-concern',
          groupId: 'orders',
          primaryTerm: 3n,
          writeConcern: { level: 'majority', timeoutMs: 5 },
        },
        NODE_B,
      ),
    ).rejects.toMatchObject({ code: 'WRITE_CONCERN_ERROR' })
    await engine.stop()
  })

  it('sends the caller write concern with the forwarded write', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    harness.transport.addPeer('primary1', 'primary')
    const forward = vi.spyOn(harness.transport, 'forward')
    const engine = new ReplicationEngine(
      db,
      conn,
      makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica'), writeForwarding: true }),
    )
    await engine.start()

    await engine.execute("INSERT INTO users VALUES (1, 'x')", undefined, {
      writeConcern: { level: 'all', timeoutMs: 250 },
    })

    expect(forward).toHaveBeenCalledWith(
      'primary1',
      expect.objectContaining({ writeConcern: { level: 'all', timeoutMs: 250 } }),
    )
    await engine.stop()
  })
})
