import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { TopologyRole } from '../../types.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const SESSION_TTL_MS = 10_000

async function startNode(
  harness: EngineTestHarness,
  coordinator: InMemoryClusterCoordinator,
  configuredRole: TopologyRole,
  currentPrimaryNodeId: string,
): Promise<ReplicationEngine> {
  const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
  await coordinator.setReplicationGroupState({
    clusterId: 'cluster-a',
    groupId: 'orders',
    votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
    currentPrimary: { nodeId: currentPrimaryNodeId },
    primaryTerm: 1n,
    inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
  })
  const engine = new ReplicationEngine(db, conn, {
    nodeId: NODE_A,
    topology: new PrimaryReplicaTopology(configuredRole),
    transport: harness.transport,
    initialSync: false,
    coordinator: {
      clusterId: 'cluster-a',
      groupId: 'orders',
      coordinator,
      controller: false,
      sessionTtlMs: SESSION_TTL_MS,
    },
  })
  await engine.start()
  return engine
}

describe('replication status reporting', () => {
  let harness: EngineTestHarness
  let engine: ReplicationEngine | null = null

  beforeEach(() => {
    harness = createHarness()
    engine = null
  })

  afterEach(async () => {
    await engine?.stop()
    await teardownHarness(harness)
  })

  it('reports the role the coordinator records, not the role the node was configured with', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    engine = await startNode(harness, coordinator, 'replica', NODE_A)

    expect(engine.config.topology.role).toBe('replica')
    expect(engine.status().role).toBe('primary')
  })

  it('reports replica once the coordinator names another node primary', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    engine = await startNode(harness, coordinator, 'primary', NODE_B)

    expect(engine.config.topology.role).toBe('primary')
    expect(engine.status().role).toBe('replica')
  })

  it('reports the coordinator as connected after a successful session registration', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    engine = await startNode(harness, coordinator, 'primary', NODE_A)

    expect(engine.status().coordinator?.connected).toBe(true)
  })

  it('reports the coordinator as disconnected once contact goes older than the session lease', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    engine = await startNode(harness, coordinator, 'primary', NODE_A)

    const staleNow = Date.now() + SESSION_TTL_MS + 1
    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(staleNow)
    try {
      expect(engine.status().coordinator?.connected).toBe(false)
    } finally {
      nowSpy.mockRestore()
    }

    expect(engine.status().coordinator?.connected).toBe(true)
  })
})
