import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C, waitForSyncRequest } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

describe('ReplicationEngine coordinator mode', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
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
    const tracker = new ChangeTracker()
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
    const tracker = new ChangeTracker()
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
})
