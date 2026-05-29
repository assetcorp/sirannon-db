import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
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
