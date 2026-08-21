import { describe, expect, it } from 'vitest'
import { coordinatorBackupGroup } from '../backup-group.js'
import { InMemoryClusterCoordinator } from '../coordinator/in-memory.js'
import type { ClusterCoordinator } from '../coordinator/types.js'

const CLUSTER_ID = 'cluster-a'
const GROUP_ID = 'group-a'
const VOTERS = ['node-a', 'node-b', 'node-c']

async function seeded(): Promise<InMemoryClusterCoordinator> {
  const coordinator = new InMemoryClusterCoordinator()
  await coordinator.setReplicationGroupState({
    clusterId: CLUSTER_ID,
    groupId: GROUP_ID,
    votingDataBearingNodeIds: VOTERS,
    currentPrimary: { nodeId: 'node-a' },
    inSyncNodeIds: VOTERS,
  })
  return coordinator
}

function groupFor(coordinator: ClusterCoordinator, nodeId = 'node-b') {
  return coordinatorBackupGroup({ coordinator, clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId })
}

describe('the group a backup cycle reads from its coordinator', () => {
  it('reports the primary and the nodes the group counts as in sync', async () => {
    const membership = await groupFor(await seeded()).readMembership()

    expect(membership.primaryNodeId).toBe('node-a')
    expect(membership.nodeIds).toEqual(VOTERS)
  })

  it('leaves out a node being drained, rebuilt, or held out as faulted', async () => {
    const coordinator = await seeded()
    await coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: 'node-b',
      draining: true,
    })
    await coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: 'node-c',
      faulted: true,
    })

    expect((await groupFor(coordinator).readMembership()).nodeIds).toEqual(['node-a'])
  })

  it('follows a failover to the node the group now names primary', async () => {
    const coordinator = await seeded()
    const before = await coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
    await coordinator.compareAndAdvancePrimaryTerm({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      expectedPrimaryTerm: before?.primaryTerm ?? 0n,
      nextPrimary: { nodeId: 'node-b' },
    })

    expect((await groupFor(coordinator).readMembership()).primaryNodeId).toBe('node-b')
  })

  it('carries the identifier of this node through to the cycle', () => {
    expect(groupFor(new InMemoryClusterCoordinator(), 'node-c').nodeId).toBe('node-c')
  })

  it('refuses to answer for a group the coordinator holds nothing about', async () => {
    const reading = groupFor(new InMemoryClusterCoordinator()).readMembership()

    await expect(reading).rejects.toThrow(/holds no state for replication group/)
  })
})
