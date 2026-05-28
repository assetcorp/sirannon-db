import { afterEach, beforeEach, describe, expect, it } from 'vitest'
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
})
