import { describe, expect, it } from 'vitest'
import { InMemoryClusterCoordinator } from '../../coordinator/in-memory.js'

describe('InMemoryClusterCoordinator', () => {
  it('grants one live controller lease and allows takeover after expiry', async () => {
    let nowMs = 1_000
    const coordinator = new InMemoryClusterCoordinator({ now: () => nowMs })

    const first = await coordinator.tryAcquireControllerLease({
      clusterId: 'cluster-a',
      holderId: 'node-a',
      ttlMs: 500,
    })
    expect(first.acquired).toBe(true)
    if (!first.acquired) throw new Error('first controller lease was not acquired')

    const denied = await coordinator.tryAcquireControllerLease({
      clusterId: 'cluster-a',
      holderId: 'node-b',
      ttlMs: 500,
    })
    expect(denied.acquired).toBe(false)
    expect(denied.lease?.holderId).toBe('node-a')

    const renewed = await coordinator.renewLease(first.lease.id, 750)
    expect(renewed).toBe(true)

    nowMs = 1_700
    const stillDenied = await coordinator.tryAcquireControllerLease({
      clusterId: 'cluster-a',
      holderId: 'node-b',
      ttlMs: 500,
    })
    expect(stillDenied.acquired).toBe(false)

    nowMs = 1_751
    const takeover = await coordinator.tryAcquireControllerLease({
      clusterId: 'cluster-a',
      holderId: 'node-b',
      ttlMs: 500,
    })
    expect(takeover.acquired).toBe(true)
    if (!takeover.acquired) throw new Error('controller lease takeover was not acquired')
    expect(takeover.lease.holderId).toBe('node-b')
  })

  it('tracks live node sessions independently from stable node identity', async () => {
    let nowMs = 2_000
    const coordinator = new InMemoryClusterCoordinator({ now: () => nowMs })

    const session = await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-a',
      ttlMs: 400,
      endpoint: 'https://node-a.example.com',
      dataBearing: true,
      voting: true,
      compatibility: {
        packageVersion: '0.1.4',
        protocolVersion: '1',
        specVersion: '0.1',
      },
    })

    expect(session.nodeId).toBe('node-a')
    expect(session.lease.holderId).toBe('node-a')
    expect(await coordinator.getLiveNodeSession('cluster-a', 'node-a')).toMatchObject({
      nodeId: 'node-a',
      endpoint: 'https://node-a.example.com',
      dataBearing: true,
      voting: true,
    })

    nowMs = 2_401
    expect(await coordinator.getLiveNodeSession('cluster-a', 'node-a')).toBeNull()

    nowMs = 3_000
    const replacement = await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-a',
      ttlMs: 400,
    })
    expect(replacement.lease.id).not.toBe(session.lease.id)

    await coordinator.deregisterNodeSession('cluster-a', 'node-a')
    expect(await coordinator.getLiveNodeSession('cluster-a', 'node-a')).toBeNull()
  })

  it('releases controller leases and node-session leases by lease id', async () => {
    const coordinator = new InMemoryClusterCoordinator({ now: () => 3_000 })

    const controller = await coordinator.tryAcquireControllerLease({
      clusterId: 'cluster-a',
      holderId: 'node-a',
      ttlMs: 500,
    })
    expect(controller.acquired).toBe(true)
    if (!controller.acquired) throw new Error('controller lease was not acquired')

    expect(await coordinator.releaseLease(controller.lease.id)).toBe(true)
    const takeover = await coordinator.tryAcquireControllerLease({
      clusterId: 'cluster-a',
      holderId: 'node-b',
      ttlMs: 500,
    })
    expect(takeover.acquired).toBe(true)
    if (!takeover.acquired) throw new Error('released controller lease was not available')
    expect(takeover.lease.holderId).toBe('node-b')

    const session = await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-a',
      ttlMs: 500,
    })
    expect(await coordinator.releaseLease(session.lease.id)).toBe(true)
    expect(await coordinator.getLiveNodeSession('cluster-a', 'node-a')).toBeNull()
    expect(await coordinator.releaseLease('missing-lease')).toBe(false)
  })

  it('watches replication-group authority and advances the primary term by compare-and-set', async () => {
    const coordinator = new InMemoryClusterCoordinator({ now: () => 4_000 })
    const observedTerms: bigint[] = []

    const unsubscribe = coordinator.watchReplicationGroup('cluster-a', 'orders', state => {
      observedTerms.push(state.primaryTerm)
      state.inSyncNodeIds.push('mutated-observer')
    })

    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: ['node-a', 'node-b', 'node-c'],
      currentPrimary: { nodeId: 'node-a', endpoint: 'https://node-a.example.com' },
      primaryTerm: 1n,
      inSyncNodeIds: ['node-a', 'node-b'],
    })

    const stale = await coordinator.compareAndAdvancePrimaryTerm({
      clusterId: 'cluster-a',
      groupId: 'orders',
      expectedPrimaryTerm: 0n,
      nextPrimary: { nodeId: 'node-c', endpoint: 'https://node-c.example.com' },
    })
    expect(stale.advanced).toBe(false)
    expect(stale.state?.primaryTerm).toBe(1n)

    const promoted = await coordinator.compareAndAdvancePrimaryTerm({
      clusterId: 'cluster-a',
      groupId: 'orders',
      expectedPrimaryTerm: 1n,
      nextPrimary: { nodeId: 'node-b', endpoint: 'https://node-b.example.com' },
    })
    expect(promoted.advanced).toBe(true)
    if (!promoted.state) throw new Error('primary term was not advanced')
    expect(promoted.state.primaryTerm).toBe(2n)
    expect(promoted.state.currentPrimary?.nodeId).toBe('node-b')

    const stored = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(stored?.inSyncNodeIds).toEqual(['node-b'])
    expect(stored?.repairingNodeIds).toEqual(['node-a'])
    expect(observedTerms).toEqual([1n, 2n])

    unsubscribe()
    await coordinator.updateInSyncSet({
      clusterId: 'cluster-a',
      groupId: 'orders',
      inSyncNodeIds: ['node-b'],
    })

    const updated = await coordinator.getReplicationGroupState('cluster-a', 'orders')
    expect(updated?.inSyncNodeIds).toEqual(['node-b'])
    expect(observedTerms).toEqual([1n, 2n])
  })

  it('admits a node to the in-sync set only with catch-up proof for the durability point', async () => {
    const coordinator = new InMemoryClusterCoordinator({ now: () => 4_500 })
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: ['node-a', 'node-b', 'node-c'],
      currentPrimary: { nodeId: 'node-a', endpoint: 'https://node-a.example.com' },
      primaryTerm: 1n,
      durabilityPointSeq: 10n,
      inSyncNodeIds: ['node-a'],
      repairingNodeIds: ['node-b'],
    })

    await expect(
      coordinator.updateInSyncSet({
        clusterId: 'cluster-a',
        groupId: 'orders',
        inSyncNodeIds: ['node-a', 'node-b'],
      }),
    ).rejects.toThrow(/catch-up proof/)

    const staleProof = await coordinator.admitNodeToInSyncSet({
      clusterId: 'cluster-a',
      groupId: 'orders',
      nodeId: 'node-b',
      sourceNodeId: 'node-a',
      appliedSeq: 9n,
    })
    expect(staleProof?.inSyncNodeIds).toEqual(['node-a'])
    expect(staleProof?.repairingNodeIds).toEqual(['node-b'])

    const admitted = await coordinator.admitNodeToInSyncSet({
      clusterId: 'cluster-a',
      groupId: 'orders',
      nodeId: 'node-b',
      sourceNodeId: 'node-a',
      appliedSeq: 10n,
    })
    expect(admitted?.inSyncNodeIds).toEqual(['node-a', 'node-b'])
    expect(admitted?.repairingNodeIds).toEqual([])
  })

  it('promotes only a live in-sync configured voter and fails closed when none is safe', async () => {
    let nowMs = 5_000
    const coordinator = new InMemoryClusterCoordinator({ now: () => nowMs })

    await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-b',
      ttlMs: 500,
      endpoint: 'https://node-b.example.com',
      dataBearing: true,
      voting: true,
    })
    await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-c',
      ttlMs: 500,
      endpoint: 'https://node-c.example.com',
      dataBearing: true,
      voting: true,
    })

    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: ['node-a', 'node-b', 'node-c'],
      currentPrimary: { nodeId: 'node-a', endpoint: 'https://node-a.example.com' },
      primaryTerm: 8n,
      inSyncNodeIds: ['node-a', 'node-b'],
    })

    const promoted = await coordinator.promoteEligibleReplica({
      clusterId: 'cluster-a',
      groupId: 'orders',
    })
    expect(promoted.primaryTerm).toBe(9n)
    expect(promoted.currentPrimary).toEqual({ nodeId: 'node-b', endpoint: 'https://node-b.example.com' })
    expect(promoted.inSyncNodeIds).toEqual(['node-b'])
    expect(promoted.repairingNodeIds).toContain('node-a')

    await coordinator.updateInSyncSet({
      clusterId: 'cluster-a',
      groupId: 'orders',
      inSyncNodeIds: [],
    })
    nowMs = 5_600

    await expect(
      coordinator.promoteEligibleReplica({
        clusterId: 'cluster-a',
        groupId: 'orders',
      }),
    ).rejects.toMatchObject({ code: 'NO_SAFE_PRIMARY' })
  })

  it('fails closed for automatic promotion with fewer than three voting data-bearing nodes', async () => {
    const coordinator = new InMemoryClusterCoordinator({ now: () => 6_000 })
    await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-b',
      ttlMs: 500,
      endpoint: 'https://node-b.example.com',
    })
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: ['node-a', 'node-b'],
      currentPrimary: { nodeId: 'node-a', endpoint: 'https://node-a.example.com' },
      primaryTerm: 2n,
      inSyncNodeIds: ['node-b'],
    })

    await expect(
      coordinator.promoteEligibleReplica({
        clusterId: 'cluster-a',
        groupId: 'orders',
      }),
    ).rejects.toMatchObject({ code: 'NO_SAFE_PRIMARY' })
  })

  it('skips candidates with incompatible major compatibility metadata', async () => {
    const coordinator = new InMemoryClusterCoordinator({ now: () => 7_000 })
    await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-b',
      ttlMs: 500,
      endpoint: 'https://node-b.example.com',
      compatibility: {
        packageVersion: '1.1.0',
        protocolVersion: '2.0.0',
        specVersion: '1.0.0',
      },
    })
    await coordinator.registerNodeSession({
      clusterId: 'cluster-a',
      nodeId: 'node-c',
      ttlMs: 500,
      endpoint: 'https://node-c.example.com',
      compatibility: {
        packageVersion: '1.2.0',
        protocolVersion: '1.5.0',
        specVersion: '1.1.0',
      },
    })
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: ['node-a', 'node-b', 'node-c'],
      currentPrimary: { nodeId: 'node-a', endpoint: 'https://node-a.example.com' },
      primaryTerm: 2n,
      inSyncNodeIds: ['node-b', 'node-c'],
      compatibility: {
        packageVersion: '1.0.0',
        protocolVersion: '1.0.0',
        specVersion: '1.0.0',
      },
    })

    const promoted = await coordinator.promoteEligibleReplica({
      clusterId: 'cluster-a',
      groupId: 'orders',
    })

    expect(promoted.currentPrimary).toEqual({ nodeId: 'node-c', endpoint: 'https://node-c.example.com' })
  })

  it('removes nodes from the safe set when maintenance state starts', async () => {
    const coordinator = new InMemoryClusterCoordinator({ now: () => 8_000 })
    await coordinator.setReplicationGroupState({
      clusterId: 'cluster-a',
      groupId: 'orders',
      votingDataBearingNodeIds: ['node-a', 'node-b', 'node-c'],
      currentPrimary: { nodeId: 'node-a', endpoint: 'https://node-a.example.com' },
      primaryTerm: 2n,
      inSyncNodeIds: ['node-a', 'node-b', 'node-c'],
    })

    const drained = await coordinator.updateNodeMaintenance({
      clusterId: 'cluster-a',
      groupId: 'orders',
      nodeId: 'node-b',
      draining: true,
    })

    expect(drained?.drainingNodeIds).toEqual(['node-b'])
    expect(drained?.inSyncNodeIds).toEqual(['node-a', 'node-c'])
  })
})
