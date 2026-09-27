import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'
import type { ClusterCoordinator } from '../../coordinator/types.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import { NODE_C } from './coordinator-helpers.js'
import { createDbAndConn, createHarness, type EngineTestHarness, NODE_A, NODE_B, teardownHarness } from './helpers.js'

const CLUSTER_ID = 'cluster-a'
const GROUP_ID = 'orders'

describe('a node bidding for the controller lease', () => {
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

  async function startStandby(coordinator: ClusterCoordinator, tickIntervalMs: number): Promise<ReplicationEngine> {
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
      coordinator: {
        clusterId: CLUSTER_ID,
        groupId: GROUP_ID,
        coordinator,
        controller: { enabled: true, leaseTtlMs: 10_000, tickIntervalMs },
      },
    })
    await started.start()
    return started
  }

  async function waitForControllerState(started: ReplicationEngine, state: string, timeoutMs = 500): Promise<void> {
    const startedAt = Date.now()
    while (Date.now() - startedAt < timeoutMs) {
      if (started.controllerState === state) return
      await new Promise(resolve => setTimeout(resolve, 5))
    }
    throw new Error(`Timed out waiting for controller state '${state}', which stayed '${started.controllerState}'`)
  }

  it('leaves the lease alone while the watch shows another node holding it', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    await coordinator.tryAcquireControllerLease({ clusterId: CLUSTER_ID, holderId: NODE_B, ttlMs: 10_000 })
    const bid = vi.spyOn(coordinator, 'tryAcquireControllerLease')

    engine = await startStandby(coordinator, 1)
    await new Promise(resolve => setTimeout(resolve, 50))

    expect(bid).not.toHaveBeenCalled()
    expect(engine.controllerState).toBe('standby')
  })

  it('takes the lease as soon as the watch reports that nobody holds it', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    const held = await coordinator.tryAcquireControllerLease({
      clusterId: CLUSTER_ID,
      holderId: NODE_B,
      ttlMs: 10_000,
    })
    if (!held.acquired) throw new Error('the first bid lost the lease')

    engine = await startStandby(coordinator, 60_000)
    expect(engine.controllerState).toBe('standby')

    await coordinator.releaseLease(held.lease.id)

    await waitForControllerState(engine, 'active')
  })

  it('bids on its own timer when the coordinator serves no watch', async () => {
    const coordinator = new InMemoryClusterCoordinator()
    const withoutWatch = coordinator as ClusterCoordinator & { watchControllerLease?: unknown }
    withoutWatch.watchControllerLease = undefined

    engine = await startStandby(coordinator, 1)

    await waitForControllerState(engine, 'active')
  })
})
