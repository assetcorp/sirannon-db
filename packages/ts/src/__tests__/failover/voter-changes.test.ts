import { describe, expect, it } from 'vitest'
import type { StoredGroup } from './etcd-oracle.js'
import { expectRejectsWith, waitForItemsOnDisk } from './gate-assertions.js'
import { waitForGroup, waitForInSyncSet } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  type GateEnvironment,
  GROUP_ID,
  NODE_A,
  NODE_B,
  NODE_C,
  operatorCoordinator,
  requireNode,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { statusOf } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import type { ItemRow } from './sqlite-oracle.js'

async function writeRows(env: GateEnvironment, ids: readonly number[], note: string): Promise<ItemRow[]> {
  const rows: ItemRow[] = []
  for (const id of ids) {
    const row = itemRow(id, NODE_A, note)
    await writeItemMajority(requireNode(env, NODE_A), row)
    rows.push(row)
  }
  return rows
}

async function storedGroup(env: GateEnvironment): Promise<StoredGroup> {
  const group = await env.etcdOracle.group()
  if (!group) throw new Error(`etcd holds no group '${GROUP_ID}'`)
  return group
}

async function votersOf(env: GateEnvironment): Promise<string[]> {
  return (await storedGroup(env)).votingDataBearingNodeIds
}

function voterChange(nodeId: string, expectedVoterNodeIds: string[], groupId = GROUP_ID) {
  return { clusterId: CLUSTER_ID, groupId, nodeId, expectedVoterNodeIds }
}

describe('voter changes', () => {
  const environment = useFailoverEnvironment()

  it('removes a voter, which then refuses reads and writes, and adds it back through a full copy without losing an acknowledged write', async () => {
    const env = environment()
    await startCluster(env)
    const before = await writeRows(env, [1, 2, 3], 'before-removal')

    const removal = await env.coordinator.removeVoter(voterChange(NODE_C, await votersOf(env)))
    expect(removal.changed).toBe(true)
    expect(removal.state?.votingDataBearingNodeIds.sort()).toEqual([NODE_A, NODE_B].sort())

    const removed = requireNode(env, NODE_C)
    await waitForGroup(env, group => !group.votingDataBearingNodeIds.includes(NODE_C), 5_000, `${NODE_C} removed`)
    await expect.poll(async () => (await statusOf(removed)).health.reason, { timeout: 5_000 }).toBe('not-member')
    await expectRejectsWith(removed.query('SELECT id FROM failover_items'), ['TOPOLOGY_ERROR'])
    await expectRejectsWith(
      removed.execute('INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)', [90, NODE_C, 90, 'x']),
      ['TOPOLOGY_ERROR'],
    )
    const whileRemoved = await writeRows(env, [4, 5, 6], 'while-removed')

    await setReplicationLinks(env, NODE_C, false)
    const addition = await env.coordinator.addVoter(voterChange(NODE_C, await votersOf(env)))
    expect(addition.changed).toBe(true)
    expect(addition.state?.repairingNodeIds).toContain(NODE_C)
    expect(addition.state?.votingDataBearingNodeIds).not.toContain(NODE_C)

    await expect(env.coordinator.removeVoter(voterChange(NODE_B, await votersOf(env)))).rejects.toThrow(RangeError)
    await expect(env.coordinator.addVoter(voterChange(NODE_B, await votersOf(env)))).rejects.toThrow(RangeError)

    await setReplicationLinks(env, NODE_C, true)
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 60_000)
    const group = await storedGroup(env)
    expect(group.pendingVoterNodeIds).toEqual([])
    expect(group.votingDataBearingNodeIds.sort()).toEqual([NODE_A, NODE_B, NODE_C].sort())
    expect(group.repairingNodeIds).not.toContain(NODE_C)
    const afterReturn = await writeRows(env, [7, 8], 'after-return')
    await waitForItemsOnDisk(env, [NODE_A, NODE_B, NODE_C], [...before, ...whileRemoved, ...afterReturn], 30_000)
  })

  it('refuses to remove the primary or to leave fewer in-sync voters than a majority, and ignores a stale voter list', async () => {
    const env = environment()
    await startCluster(env)
    await writeRows(env, [1], 'all-in-sync')

    await expect(env.coordinator.removeVoter(voterChange(NODE_A, await votersOf(env)))).rejects.toThrow(RangeError)

    await setReplicationLinks(env, NODE_C, false)
    await writeRows(env, [2], 'c-cut-off')
    await waitForGroup(env, group => !group.inSyncNodeIds.includes(NODE_C), 10_000, `${NODE_C} out of the in-sync set`)
    await expect(env.coordinator.removeVoter(voterChange(NODE_B, await votersOf(env)))).rejects.toThrow(RangeError)

    const stale = await env.coordinator.removeVoter(voterChange(NODE_C, [NODE_A, NODE_B]))
    expect(stale.changed).toBe(false)
    const group = await storedGroup(env)
    expect(group.votingDataBearingNodeIds.sort()).toEqual([NODE_A, NODE_B, NODE_C].sort())
    expect(group.currentPrimary?.nodeId).toBe(NODE_A)
  })

  it('creates the group record once when two nodes create it together', async () => {
    const env = environment()
    const groupId = 'orders-created-together'
    const second = operatorCoordinator(env.etcd, env.keyPrefix)
    try {
      const create = (coordinator: typeof second, primary: string) =>
        coordinator.createReplicationGroupState({
          clusterId: CLUSTER_ID,
          groupId,
          votingDataBearingNodeIds: [NODE_A, NODE_B, NODE_C],
          currentPrimary: { nodeId: primary },
          primaryTerm: 1n,
          inSyncNodeIds: [primary],
        })
      const results = await Promise.all([create(env.coordinator, NODE_A), create(second, NODE_B)])
      expect(results.filter(result => result.created)).toHaveLength(1)
      const stored = await env.etcdOracle.groupRecord(groupId)
      const storedPrimary = (stored?.currentPrimary as { nodeId?: string } | null)?.nodeId
      for (const result of results) {
        expect(result.state?.currentPrimary?.nodeId).toBe(storedPrimary)
      }
    } finally {
      await second.close()
    }
  })

  it('changes voters on a group record that an earlier release wrote', async () => {
    const env = environment()
    const groupId = 'orders-earlier-release'
    await env.etcdOracle.writeGroupRecord(
      groupId,
      JSON.stringify({
        repairingNodeIds: [],
        drainingNodeIds: [],
        inSyncNodeIds: [NODE_A, NODE_B],
        primaryTerm: '3',
        currentPrimary: { nodeId: NODE_A },
        votingDataBearingNodeIds: [NODE_A, NODE_B],
        groupId,
        clusterId: CLUSTER_ID,
        updatedAtMs: 1,
      }),
    )

    const addition = await env.coordinator.addVoter(voterChange(NODE_C, [NODE_A, NODE_B], groupId))
    expect(addition.changed).toBe(true)
    expect((await env.etcdOracle.groupRecord(groupId))?.pendingVoterNodeIds).toEqual([NODE_C])

    const removal = await env.coordinator.removeVoter(voterChange(NODE_C, [NODE_A, NODE_B], groupId))
    expect(removal.changed).toBe(true)
    const stored = await env.etcdOracle.groupRecord(groupId)
    expect(stored?.pendingVoterNodeIds).toBeUndefined()
    expect(stored?.repairingNodeIds).toEqual([])
    expect(stored?.primaryTerm).toBe('3')
  })
})
