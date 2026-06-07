import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const NODE_C = 'cccc0000cccc0000cccc0000cccc0000'

describe('ReplicationEngine coordinator mode', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('requires a stable node id before coordinator mode can start', async () => {
    const { db, conn } = await createDbAndConn(harness)
    const coordinator = new InMemoryClusterCoordinator()

    expect(
      () =>
        new ReplicationEngine(db, conn, {
          topology: new PrimaryReplicaTopology('primary'),
          transport: harness.transport,
          initialSync: false,
          coordinator: {
            clusterId: 'cluster-a',
            groupId: 'orders',
            coordinator,
            votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
          },
        }),
    ).toThrow(/stable persisted nodeId/)
  })

  it('rejects local writes on a stale primary with routing details', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_B, endpoint: 'https://node-b.example.com' },
      primaryTerm: 7n,
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

    await expect(engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'])).rejects.toMatchObject({
      code: 'STALE_PRIMARY',
      details: {
        replicationGroupId: 'orders',
        primaryTerm: '7',
      },
    })

    await engine.stop()
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

  it('rejects current-primary writes when local compatibility metadata is incompatible', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
      primaryTerm: 3n,
      inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
      compatibility: {
        packageVersion: '1.0.0',
        protocolVersion: '2.0.0',
        specVersion: '1.0.0',
      },
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
        compatibility: {
          packageVersion: '1.4.0',
          protocolVersion: '1.9.0',
          specVersion: '1.1.0',
        },
      },
    })
    await engine.start()

    await expect(
      engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'], { writeConcern: { level: 'local' } }),
    ).rejects.toMatchObject({
      code: 'PROTOCOL_VERSION_MISMATCH',
    })

    await engine.stop()
  })

  it('faults a returning former primary when local-only writes are present', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
      primaryTerm: 1n,
      inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
    })

    const originalPrimary = new ReplicationEngine(db, conn, {
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
    await originalPrimary.start()
    await originalPrimary.execute('INSERT INTO items (name) VALUES (?)', ['local-only'], {
      writeConcern: { level: 'local' },
    })
    await originalPrimary.stop()

    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_B, endpoint: 'https://node-b.example.com' },
      primaryTerm: 2n,
      inSyncNodeIds: [NODE_B, NODE_C],
      repairingNodeIds: [NODE_A],
    })

    const returningPrimary = new ReplicationEngine(db, conn, {
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
    await returningPrimary.start()

    const status = returningPrimary.status().coordinator
    expect(status?.faultedNodeIds).toContain(NODE_A)
    expect(status?.repairingNodeIds).not.toContain(NODE_A)
    await expect(
      returningPrimary.execute('INSERT INTO items (name) VALUES (?)', ['after-return']),
    ).rejects.toMatchObject({ code: 'STALE_PRIMARY' })

    await returningPrimary.stop()
  })

  it('rejoins a non-divergent former primary through sync even when static topology says primary', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const tracker = new ChangeTracker({ replication: true })
    await tracker.watch(conn, 'items')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_B, endpoint: 'https://node-b.example.com' },
      primaryTerm: 2n,
      inSyncNodeIds: [NODE_B, NODE_C],
      repairingNodeIds: [NODE_A],
    })
    harness.transport.addPeer(NODE_B, 'primary')

    const returningPrimary = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      changeTracker: tracker,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await returningPrimary.start()

    expect(returningPrimary.status().syncState?.phase).toBe('syncing')

    await returningPrimary.stop()
  })

  it('starts rejoin sync when a coordinator update points pending repair at a connected primary', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const tracker = new ChangeTracker({ replication: true })
    await tracker.watch(conn, 'items')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_B, endpoint: 'https://node-b.example.com' },
      primaryTerm: 2n,
      inSyncNodeIds: [NODE_B],
      repairingNodeIds: [NODE_A],
    })

    const repairingReplica = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('replica'),
      transport: harness.transport,
      changeTracker: tracker,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        coordinator,
        controller: false,
      },
    })
    await repairingReplica.start()

    expect(repairingReplica.status().syncState?.phase).toBe('pending')
    harness.transport.addPeer(NODE_C, 'replica')
    expect(harness.transport.sentSyncRequests).toHaveLength(0)

    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_C, endpoint: 'https://node-c.example.com' },
      primaryTerm: 3n,
      inSyncNodeIds: [NODE_C],
      repairingNodeIds: [NODE_A, NODE_B],
    })

    await waitForSyncRequest(harness.transport, NODE_C)
    expect(repairingReplica.status().syncState?.phase).toBe('syncing')

    await repairingReplica.stop()
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

  it('moves primary duty away from a draining live primary', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: NODE_B,
      ttlMs: 1_000,
      endpoint: 'https://node-b.example.com',
    })
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A, endpoint: 'https://node-a.example.com' },
      primaryTerm: 4n,
      inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
      drainingNodeIds: [NODE_A],
    })

    const primary = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      initialSync: false,
      coordinator: {
        clusterId: 'cluster-a',
        groupId: 'orders',
        endpoint: 'https://node-a.example.com',
        coordinator,
        controller: {
          enabled: true,
          leaseTtlMs: 1_000,
          tickIntervalMs: 1,
        },
      },
    })
    await primary.start()

    const promoted = await waitForCurrentPrimary(coordinator, NODE_B)

    expect(promoted.primaryTerm).toBe(5n)
    expect(promoted.drainingNodeIds).toContain(NODE_A)
    expect(promoted.currentPrimary).toEqual({ nodeId: NODE_B, endpoint: 'https://node-b.example.com' })

    await primary.stop()
  })
})

async function waitForCurrentPrimary(coordinator: InMemoryClusterCoordinator, nodeId: string, timeoutMs: number = 250) {
  const startedAt = Date.now()
  let latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
  while (Date.now() - startedAt < timeoutMs) {
    latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    if (latest?.currentPrimary?.nodeId === nodeId) {
      return latest
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for primary '${nodeId}'`)
}

async function waitForInSyncNodes(coordinator: InMemoryClusterCoordinator, nodeIds: string[], timeoutMs: number = 250) {
  const startedAt = Date.now()
  let latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
  while (Date.now() - startedAt < timeoutMs) {
    latest = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    const state = latest
    if (state && nodeIds.every(nodeId => state.inSyncNodeIds.includes(nodeId))) {
      return state
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for in-sync nodes '${nodeIds.join(', ')}'`)
}

async function waitForSyncRequest(transport: EngineTestHarness['transport'], peerId: string, timeoutMs: number = 250) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    const request = transport.sentSyncRequests.find(entry => entry.peerId === peerId)
    if (request) {
      return request
    }
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(`Timed out waiting for sync request to '${peerId}'`)
}

async function recordAppliedSeq(conn: SQLiteConnection, sourceNodeId: string, seq: bigint) {
  const stmt = await conn.prepare(
    'INSERT OR IGNORE INTO _sirannon_applied_changes (source_node_id, source_seq, applied_at) VALUES (?, ?, ?)',
  )
  await stmt.run(sourceNodeId, seq.toString(), Date.now() / 1000)
}
