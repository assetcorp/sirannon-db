import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

async function startPrimary(harness: EngineTestHarness, coordinator: InMemoryClusterCoordinator) {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
  await coordinator.setReplicationGroupState({
    clusterId: 'cluster-a',
    groupId: 'orders',
    votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
    currentPrimary: { nodeId: NODE_A },
    primaryTerm: 1n,
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
  harness.transport.addPeer(NODE_C)
  return engine
}

function groupState(coordinator: InMemoryClusterCoordinator) {
  return coordinator.getReplicationGroupState('cluster-a', 'orders')
}

describe('in-sync set reconciliation', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('re-admits a replica that reaches the durability point after the write was acknowledged', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    const engine = await startPrimary(harness, coordinator)

    const write = engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'])
    await vi.waitFor(() => expect(harness.transport.sentBatches.length).toBeGreaterThan(0))
    const seq = harness.transport.sentBatches[0].batch.toSeq

    engine.peerTracker.onAckReceived(NODE_B, seq)
    await write

    const afterWrite = await groupState(coordinator)
    expect(afterWrite?.durabilityPointSeq).toBe(seq)
    expect(afterWrite?.inSyncNodeIds).not.toContain(NODE_C)

    engine.peerTracker.onAckReceived(NODE_C, seq)

    await vi.waitFor(
      async () => {
        const state = await groupState(coordinator)
        expect(state?.inSyncNodeIds).toContain(NODE_C)
      },
      { timeout: 5_000, interval: 25 },
    )

    await engine.stop()
  })

  it('leaves a replica out while it is still behind the durability point', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    const engine = await startPrimary(harness, coordinator)

    const write = engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'])
    await vi.waitFor(() => expect(harness.transport.sentBatches.length).toBeGreaterThan(0))
    const seq = harness.transport.sentBatches[0].batch.toSeq

    engine.peerTracker.onAckReceived(NODE_B, seq)
    await write

    engine.peerTracker.onAckReceived(NODE_C, seq - 1n)

    await new Promise(resolve => setTimeout(resolve, 2_000))
    const state = await groupState(coordinator)
    expect(state?.inSyncNodeIds).not.toContain(NODE_C)

    await engine.stop()
  })
})
