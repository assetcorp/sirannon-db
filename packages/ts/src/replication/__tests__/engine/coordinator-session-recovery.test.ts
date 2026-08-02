import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const SESSION_TTL_MS = 3_000

describe('coordinator session recovery', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('re-registers the node session and restores write authority after the lease is lost', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A],
      currentPrimary: { nodeId: NODE_A },
      primaryTerm: 1n,
      inSyncNodeIds: [NODE_A],
    })

    let loseNextRenewal = false
    const renewLease = coordinator.renewLease.bind(coordinator)
    coordinator.renewLease = async (leaseId: string, ttlMs: number) => {
      if (!loseNextRenewal) return renewLease(leaseId, ttlMs)
      loseNextRenewal = false
      await coordinator.deregisterNodeSession('cluster-a', NODE_A)
      return false
    }

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
        sessionTtlMs: SESSION_TTL_MS,
      },
    })
    await engine.start()

    const firstSession = await coordinator.getLiveNodeSession('cluster-a', NODE_A)
    expect(firstSession).not.toBeNull()
    expect(engine.status().coordinator?.authority).toBe(true)

    loseNextRenewal = true

    await vi.waitFor(
      async () => {
        const session = await coordinator.getLiveNodeSession('cluster-a', NODE_A)
        expect(session).not.toBeNull()
        expect(session?.lease.id).not.toBe(firstSession?.lease.id)
      },
      { timeout: 5_000, interval: 50 },
    )

    expect(engine.status().coordinator?.authority).toBe(true)
    await expect(engine.execute('INSERT INTO items (name) VALUES (?)', ['alpha'])).resolves.toMatchObject({
      changes: 1,
    })

    await engine.stop()
  })

  it('keeps the session when a renewal fails without losing the lease', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
      currentPrimary: { nodeId: NODE_A },
      primaryTerm: 1n,
      inSyncNodeIds: [NODE_A],
    })

    let failNextRenewal = false
    const renewLease = coordinator.renewLease.bind(coordinator)
    coordinator.renewLease = async (leaseId: string, ttlMs: number) => {
      if (!failNextRenewal) return renewLease(leaseId, ttlMs)
      failNextRenewal = false
      throw new Error('Execution prevented because the circuit breaker is open')
    }

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
        sessionTtlMs: SESSION_TTL_MS,
      },
    })
    await engine.start()

    const firstSession = await coordinator.getLiveNodeSession('cluster-a', NODE_A)
    failNextRenewal = true

    await vi.waitFor(() => expect(engine.status().coordinator?.authority).toBe(false), {
      timeout: 5_000,
      interval: 25,
    })
    await vi.waitFor(() => expect(engine.status().coordinator?.authority).toBe(true), {
      timeout: 5_000,
      interval: 25,
    })

    const session = await coordinator.getLiveNodeSession('cluster-a', NODE_A)
    expect(session?.lease.id).toBe(firstSession?.lease.id)

    await engine.stop()
  })
})
