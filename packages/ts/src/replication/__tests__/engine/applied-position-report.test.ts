import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const CLUSTER_ID = 'cluster-a'
const GROUP_ID = 'orders'

async function startReplica(harness: EngineTestHarness): Promise<ReplicationEngine> {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
  const engine = new ReplicationEngine(db, conn, {
    nodeId: NODE_B,
    topology: new PrimaryReplicaTopology('replica'),
    transport: harness.transport,
    initialSync: false,
  })
  await engine.start()
  return engine
}

async function startPrimary(
  harness: EngineTestHarness,
  coordinator: InMemoryClusterCoordinator,
): Promise<ReplicationEngine> {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
  await coordinator.setReplicationGroupState({
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
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
    coordinator: { clusterId: CLUSTER_ID, groupId: GROUP_ID, coordinator, controller: false },
  })
  await engine.start()
  harness.transport.addPeer(NODE_B)
  harness.transport.addPeer(NODE_C)
  return engine
}

describe('applied position reporting', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('reports the applied position to a primary that connects', async () => {
    const engine = await startReplica(harness)
    await engine.log.setLastAppliedSeq(NODE_A, 17n)

    harness.transport.addPeer(NODE_A, 'primary')

    await vi.waitFor(() => expect(harness.transport.sentAcks).toHaveLength(1))
    expect(harness.transport.sentAcks[0]).toEqual({
      peerId: NODE_A,
      ack: { batchId: '', ackedSeq: 17n, nodeId: NODE_B },
    })

    await engine.stop()
  })

  it('stays silent towards a peer it does not replicate from', async () => {
    const engine = await startReplica(harness)
    await engine.log.setLastAppliedSeq(NODE_C, 17n)

    harness.transport.addPeer(NODE_C, 'replica')

    await new Promise(resolve => setTimeout(resolve, 100))
    expect(harness.transport.sentAcks).toHaveLength(0)

    await engine.stop()
  })

  it('stays silent while the durable sync state says the data is about to be replaced', async () => {
    const engine = await startReplica(harness)
    await engine.log.setLastAppliedSeq(NODE_A, 17n)
    await engine.log.setSyncMeta('syncing', 17n, NODE_A)

    harness.transport.addPeer(NODE_A, 'primary')

    await new Promise(resolve => setTimeout(resolve, 100))
    expect(harness.transport.sentAcks).toHaveLength(0)

    await engine.stop()
  })

  it('re-admits a reconnecting replica from its position report while no batch flows', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    const engine = await startPrimary(harness, coordinator)

    const write = engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'])
    await vi.waitFor(() => expect(harness.transport.sentBatches.length).toBeGreaterThan(0))
    const seq = harness.transport.sentBatches[0].batch.toSeq

    engine.peerTracker.onAckReceived(NODE_B, seq)
    await write

    const afterWrite = await coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
    expect(afterWrite?.inSyncNodeIds).not.toContain(NODE_C)

    harness.transport.triggerAckReceived(
      { batchId: '', ackedSeq: seq, nodeId: NODE_C, groupId: GROUP_ID, primaryTerm: 1n },
      NODE_C,
    )

    await vi.waitFor(async () => {
      const state = await coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
      expect(state?.inSyncNodeIds).toContain(NODE_C)
    })

    await engine.stop()
  })
})
