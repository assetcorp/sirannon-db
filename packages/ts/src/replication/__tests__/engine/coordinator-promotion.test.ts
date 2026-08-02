import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C, waitForCurrentPrimary } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

describe('ReplicationEngine coordinator mode', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
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
