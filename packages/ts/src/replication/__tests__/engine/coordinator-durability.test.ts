import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C, recordAppliedSeq, waitForInSyncNodes } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

describe('ReplicationEngine coordinator mode', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('uses configured voting data-bearing nodes for coordinator majority writes', async () => {
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
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await engine.start()

    await expect(
      engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'], {
        writeConcern: { level: 'majority', timeoutMs: 5 },
      }),
    ).rejects.toMatchObject({ code: 'WRITE_CONCERN_ERROR' })

    await engine.stop()
  })

  it('excludes drained voters from coordinator all writes', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
      primaryTerm: 3n,
      inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
      drainingNodeIds: [NODE_C],
    })

    const engine = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      initialSync: false,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await engine.start()
    engine.peerTracker.addPeer(NODE_B)
    engine.peerTracker.addPeer(NODE_C)
    engine.peerTracker.onAckReceived(NODE_B, 1n)

    await engine.execute("INSERT INTO items (name) VALUES ('alpha')", undefined, {
      writeConcern: { level: 'all', timeoutMs: 5 },
    })

    const state = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(state?.durabilityPointSeq).toBe(1n)
    expect(state?.inSyncNodeIds).toEqual([NODE_A, NODE_B])

    await engine.stop()
  })

  it('admits ACKing voters at the advanced durability point after alternating majority writes', async () => {
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
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await engine.start()
    engine.peerTracker.addPeer(NODE_B)
    engine.peerTracker.addPeer(NODE_C)

    engine.peerTracker.onAckReceived(NODE_B, 1n)
    await engine.execute("INSERT INTO items (id, name) VALUES (1, 'first')", undefined, {
      writeConcern: { level: 'majority', timeoutMs: 5 },
    })
    let state = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(state?.durabilityPointSeq).toBe(1n)
    expect(state?.inSyncNodeIds).toEqual([NODE_A, NODE_B])

    const nodeB = engine.peerTracker.getPeerState(NODE_B)
    if (!nodeB) throw new Error('node B peer state missing')
    nodeB.lastAckedSeq = 1n
    engine.peerTracker.onAckReceived(NODE_C, 2n)

    await engine.execute("UPDATE items SET name = 'second' WHERE id = 1", undefined, {
      writeConcern: { level: 'majority', timeoutMs: 5 },
    })
    state = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(state?.durabilityPointSeq).toBe(2n)
    expect(state?.inSyncNodeIds).toEqual([NODE_A, NODE_C])

    await engine.stop()
  })

  it('admits a slower voter when its ACK reaches the current durability point', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
      primaryTerm: 3n,
      inSyncNodeIds: [NODE_A, NODE_B],
      durabilityPointSeq: 5n,
    })

    const engine = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      initialSync: false,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await engine.start()
    engine.peerTracker.addPeer(NODE_C)

    harness.transport.triggerAckReceived(
      {
        batchId: 'late-node-c',
        ackedSeq: 5n,
        nodeId: NODE_C,
        groupId: 'orders',
        primaryTerm: 3n,
      },
      NODE_C,
    )

    const state = await waitForInSyncNodes(coordinator, [NODE_A, NODE_B, NODE_C])
    expect(state.durabilityPointSeq).toBe(5n)

    await engine.stop()
  })

  it('admits a voter from durable ACK progress when the peer reconnects', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
      primaryTerm: 3n,
      inSyncNodeIds: [NODE_A, NODE_B],
      durabilityPointSeq: 5n,
    })

    const engine = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      initialSync: false,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await engine.start()
    await engine.log.setLastAppliedSeq(NODE_C, 5n)

    harness.transport.addPeer(NODE_C)

    const state = await waitForInSyncNodes(coordinator, [NODE_A, NODE_B, NODE_C])
    expect(state.durabilityPointSeq).toBe(5n)

    await engine.stop()
  })

  it('does not mark a deadline-ready coordinator replica in sync without durability proof', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_B, endpoint: 'https://node-b.example.com' },
      primaryTerm: 2n,
      durabilityPointSeq: 10n,
      inSyncNodeIds: [NODE_B],
      repairingNodeIds: [NODE_A],
    })

    const replica = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('replica'),
      transport: harness.transport,
      initialSync: false,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await replica.start()
    await recordAppliedSeq(conn, NODE_B, 5n)
    replica.syncState = {
      phase: 'ready',
      sourcePeerId: NODE_B,
      snapshotSeq: 5n,
      completedTables: [],
      totalTables: 0,
      startedAt: null,
      error: null,
    }

    await replica.markCoordinatorSyncReady()

    const state = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(state?.inSyncNodeIds).toEqual([NODE_B])
    expect(state?.repairingNodeIds).toEqual([NODE_A])
    await expect(
      coordinator.promoteEligibleReplica({
        clusterId: 'cluster-a',
        groupId: 'orders',
        excludeNodeIds: [NODE_B],
      }),
    ).rejects.toMatchObject({ code: 'NO_SAFE_PRIMARY' })

    await replica.stop()
  })

  it('marks a coordinator replica in sync after durable catch-up reaches the durability point', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_B, endpoint: 'https://node-b.example.com' },
      primaryTerm: 2n,
      durabilityPointSeq: 10n,
      inSyncNodeIds: [NODE_B],
      repairingNodeIds: [NODE_A],
    })

    const replica = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('replica'),
      transport: harness.transport,
      initialSync: false,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await replica.start()
    await recordAppliedSeq(conn, NODE_B, 10n)
    replica.syncState = {
      phase: 'ready',
      sourcePeerId: NODE_B,
      snapshotSeq: 10n,
      completedTables: [],
      totalTables: 0,
      startedAt: null,
      error: null,
    }

    await replica.markCoordinatorSyncReady()

    const state = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(state?.inSyncNodeIds).toEqual([NODE_B, NODE_A])
    expect(state?.repairingNodeIds).toEqual([])

    await replica.stop()
  })
})
