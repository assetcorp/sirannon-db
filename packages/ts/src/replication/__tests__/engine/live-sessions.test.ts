import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { toClusterReadEndpoints } from '../../cluster-status.js'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const CLUSTER_ID = 'cluster-a'
const GROUP_ID = 'orders'
const ENDPOINTS = {
  [NODE_A]: 'https://a.example:8080',
  [NODE_B]: 'https://b.example:8080',
  [NODE_C]: 'https://c.example:8080',
}

describe('the read list of a node in coordinator mode', () => {
  let harness: EngineTestHarness
  let engine: ReplicationEngine | null = null

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await engine?.stop()
    engine = null
    await teardownHarness(harness)
  })

  async function startPrimary(coordinator: InMemoryClusterCoordinator): Promise<ReplicationEngine> {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    await coordinator.setReplicationGroupState({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A },
      primaryTerm: 1n,
      inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
    })
    const started = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      initialSync: false,
      coordinator: { clusterId: CLUSTER_ID, groupId: GROUP_ID, coordinator, controller: false },
    })
    await started.start()
    return started
  }

  function readableNodeIds(started: ReplicationEngine): string[] {
    const coordinator = started.status().coordinator
    if (!coordinator) throw new Error('the engine reports no coordinator status')
    return toClusterReadEndpoints(coordinator, ENDPOINTS).map(endpoint => endpoint.nodeId)
  }

  it('drops a node whose session lapses and takes it back when the node returns', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    engine = await startPrimary(coordinator)
    await coordinator.registerNodeSession({ clusterId: CLUSTER_ID, nodeId: NODE_B, ttlMs: 10_000 })
    await coordinator.registerNodeSession({ clusterId: CLUSTER_ID, nodeId: NODE_C, ttlMs: 10_000 })

    expect(readableNodeIds(engine)).toEqual([NODE_A, NODE_B, NODE_C])

    await coordinator.deregisterNodeSession(CLUSTER_ID, NODE_C)
    expect(readableNodeIds(engine)).toEqual([NODE_A, NODE_B])

    await coordinator.registerNodeSession({ clusterId: CLUSTER_ID, nodeId: NODE_C, ttlMs: 10_000 })
    expect(readableNodeIds(engine)).toEqual([NODE_A, NODE_B, NODE_C])
  })

  it('lists every node of the group once this node loses contact with the coordinator', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    engine = await startPrimary(coordinator)
    await coordinator.deregisterNodeSession(CLUSTER_ID, NODE_B)
    await coordinator.deregisterNodeSession(CLUSTER_ID, NODE_C)
    expect(readableNodeIds(engine)).toEqual([NODE_A])

    engine.coordinatorLastContactMs = Date.now() - 60_000

    expect(readableNodeIds(engine)).toEqual([NODE_A, NODE_B, NODE_C])
  })
})
