import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import type { SetReplicationGroupStateInput } from '../../coordinator/types.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { TopologyRole } from '../../types.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const SESSION_TTL_MS = 10_000
const TABLE_SQL = 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'

type GroupOverrides = Partial<Omit<SetReplicationGroupStateInput, 'clusterId' | 'groupId'>>

async function startNode(
  harness: EngineTestHarness,
  coordinator: InMemoryClusterCoordinator,
  configuredRole: TopologyRole,
  group: GroupOverrides,
): Promise<ReplicationEngine> {
  const { db, conn } = await createDbAndConn(harness, TABLE_SQL)
  await coordinator.setReplicationGroupState({
    clusterId: 'cluster-a',
    groupId: 'orders',
    votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
    currentPrimary: { nodeId: NODE_A },
    primaryTerm: 1n,
    inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
    ...group,
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
  let coordinator: InMemoryClusterCoordinator

  beforeEach(() => {
    harness = createHarness()
    coordinator = new InMemoryClusterCoordinator()
    engine = null
  })

  afterEach(async () => {
    await engine?.stop()
    await teardownHarness(harness)
  })

  it('reports the role the coordinator records, not the role the node was configured with', async () => {
    engine = await startNode(harness, coordinator, 'replica', {})

    expect(engine.config.topology.role).toBe('replica')
    expect(engine.status().role).toBe('primary')
  })

  it('reports replica once the coordinator names another node primary', async () => {
    engine = await startNode(harness, coordinator, 'primary', { currentPrimary: { nodeId: NODE_B } })

    expect(engine.config.topology.role).toBe('primary')
    expect(engine.status().role).toBe('replica')
  })

  it('reports the coordinator as connected after a successful session registration', async () => {
    engine = await startNode(harness, coordinator, 'primary', {})

    expect(engine.status().coordinator?.connected).toBe(true)
  })

  it('reports the coordinator as disconnected once contact goes older than the session lease', async () => {
    engine = await startNode(harness, coordinator, 'primary', {})

    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + SESSION_TTL_MS + 1)
    try {
      expect(engine.status().coordinator?.connected).toBe(false)
    } finally {
      nowSpy.mockRestore()
    }

    expect(engine.status().coordinator?.connected).toBe(true)
  })
})

describe('node health', () => {
  let harness: EngineTestHarness
  let engine: ReplicationEngine | null = null
  let coordinator: InMemoryClusterCoordinator

  beforeEach(() => {
    harness = createHarness()
    coordinator = new InMemoryClusterCoordinator()
    engine = null
  })

  afterEach(async () => {
    await engine?.stop()
    await teardownHarness(harness)
  })

  it('reports a current, connected primary as healthy and writable', async () => {
    engine = await startNode(harness, coordinator, 'primary', {})

    expect(engine.status().health).toEqual({ state: 'healthy', reason: 'in-sync', canRead: true, canWrite: true })
  })

  it('reports a replica outside the in-sync set as degraded, and still readable', async () => {
    engine = await startNode(harness, coordinator, 'replica', {
      currentPrimary: { nodeId: NODE_B },
      inSyncNodeIds: [NODE_B, NODE_C],
    })

    expect(engine.status().health).toEqual({ state: 'degraded', reason: 'lagging', canRead: true, canWrite: false })
  })

  it('names the unreachable coordinator ahead of the lagging set', async () => {
    engine = await startNode(harness, coordinator, 'replica', {
      currentPrimary: { nodeId: NODE_B },
      inSyncNodeIds: [NODE_B, NODE_C],
    })

    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + SESSION_TTL_MS + 1)
    try {
      expect(engine.status().health.state).toBe('degraded')
      expect(engine.status().health.reason).toBe('coordinator-unreachable')
    } finally {
      nowSpy.mockRestore()
    }
  })

  it('reports a draining replica as unavailable', async () => {
    engine = await startNode(harness, coordinator, 'replica', {
      currentPrimary: { nodeId: NODE_B },
      drainingNodeIds: [NODE_A],
    })

    expect(engine.status().health).toEqual({
      state: 'unavailable',
      reason: 'draining',
      canRead: false,
      canWrite: false,
    })
  })

  it('reports a faulted replica as unavailable', async () => {
    engine = await startNode(harness, coordinator, 'replica', {
      currentPrimary: { nodeId: NODE_B },
      faultedNodeIds: [NODE_A],
    })

    expect(engine.status().health.state).toBe('unavailable')
    expect(engine.status().health.reason).toBe('faulted')
  })

  it('reports a repairing node before any other condition', async () => {
    engine = await startNode(harness, coordinator, 'replica', {
      currentPrimary: { nodeId: NODE_B },
      repairingNodeIds: [NODE_A],
      inSyncNodeIds: [NODE_B],
    })

    expect(engine.status().health.state).toBe('repairing')
    expect(engine.status().health.reason).toBe('repairing')
  })

  it('reports a primary that holds authority but cannot write as failing over', async () => {
    engine = await startNode(harness, coordinator, 'primary', { drainingNodeIds: [NODE_A] })

    expect(engine.status().health).toEqual({
      state: 'failing_over',
      reason: 'draining',
      canRead: false,
      canWrite: false,
    })
  })

  it('reports a healthy writable primary outside coordinator mode', async () => {
    const { db, conn } = await createDbAndConn(harness, TABLE_SQL)
    engine = new ReplicationEngine(db, conn, {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport: harness.transport,
      initialSync: false,
    })
    await engine.start()

    expect(engine.status().health).toEqual({ state: 'healthy', reason: 'in-sync', canRead: true, canWrite: true })
  })
})
