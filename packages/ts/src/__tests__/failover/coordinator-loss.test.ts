import { describe, expect, it } from 'vitest'
import { pauseContainer, unpauseContainer } from './docker.js'
import { expectItemsAbsentOnDisk, expectRejectsWith, waitForItemsOnDisk } from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  expectGroupHolds,
  waitForGroup,
  waitForInSyncSet,
  waitForPrimary,
  waitForSessionGone,
} from './gate-cluster-state.js'
import { type GateEnvironment, NODE_A, NODE_B, NODE_C, NODE_IDS, requireNode, setEtcdLink } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForReadableNodes, waitForReadiness } from './gate-observations.js'
import { expectPublicHttpRejectsWith, itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { sleep, waitForCondition } from './gate-support.js'
import type { ItemRow } from './sqlite-oracle.js'

const COORDINATOR_REFUSALS = ['COORDINATOR_UNAVAILABLE', 'STALE_PRIMARY', 'TOPOLOGY_ERROR']

async function writeToPrimaryUntilAccepted(env: GateEnvironment, row: ItemRow, timeoutMs: number): Promise<void> {
  await waitForCondition(async () => {
    const primaryNodeId = (await env.etcdOracle.group())?.currentPrimary?.nodeId
    if (primaryNodeId === undefined) return false
    await requireNode(env, primaryNodeId).execute(
      'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET note = excluded.note',
      [row.id, row.owner, row.value, row.note],
      { writeConcern: { level: 'majority', timeoutMs: 5_000 } },
    )
    return true
  }, timeoutMs)
}

describe('losing contact with etcd', () => {
  const environment = useFailoverEnvironment()

  it('fences a primary cut off from etcd, promotes a replica once its session lapses, and takes the old primary back without the writes that it refused', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 3; id++) {
      const row = itemRow(id, NODE_A, `before-etcd-cut-${id}`)
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await setEtcdLink(env, NODE_A, false)
    await expectRejectsWith(
      requireNode(env, NODE_A).execute(
        'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
        [90, NODE_A, 90, 'refused-through-the-engine'],
        { writeConcern: { level: 'majority', timeoutMs: 5_000 } },
      ),
      COORDINATOR_REFUSALS,
    )
    await expectPublicHttpRejectsWith(
      env,
      NODE_A,
      "INSERT INTO failover_items (id, owner, value, note) VALUES (91, 'a', 91, 'refused-over-http')",
      COORDINATOR_REFUSALS,
    )
    const cutReadiness = await waitForReadiness(
      env,
      NODE_A,
      readiness => readiness.replication?.coordinator?.connected === false,
      10_000,
      'a lost coordinator',
    )
    expect(cutReadiness.replication?.writeAvailability).toBe('unavailable')
    expect(cutReadiness.replication?.coordinator?.authority).toBe(false)
    await waitForReadableNodes(env, NODE_A, NODE_IDS, 10_000)

    await waitForSessionGone(env, NODE_A, 15_000)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(promotion.primaryTerm).toBe(2n)
    expect(promotion.repairingNodeIds).toContain(NODE_A)
    const promoted = promotion.currentPrimary.nodeId
    await waitForItemsOnDisk(env, [promoted], acknowledged, 10_000)
    await expectItemsAbsentOnDisk(env, NODE_IDS, [90, 91], 1_000)

    await setEtcdLink(env, NODE_A, true)
    await waitForGroup(
      env,
      group => group.inSyncNodeIds.includes(NODE_A) && !group.repairingNodeIds.includes(NODE_A),
      30_000,
      `${NODE_A} back in the in-sync set`,
    )
    const afterReturn = itemRow(4, promoted, 'after-the-old-primary-returned')
    await writeItemMajority(requireNode(env, promoted), afterReturn)
    acknowledged.push(afterReturn)
    await waitForItemsOnDisk(env, NODE_IDS, acknowledged, 20_000)
    await expectItemsAbsentOnDisk(env, NODE_IDS, [90, 91], 1_000)
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
  })

  it('keeps the primary writable with one etcd member paused, and fences every node once a second member pauses', async () => {
    const env = environment()
    await startCluster(env, { everyNode: { sessionTtlMs: 10_000, controllerLeaseTtlMs: 10_000 } })
    const acknowledged: ItemRow[] = [itemRow(1, NODE_A, 'all-members-up')]
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'all-members-up'))
    const [firstMember, secondMember] = env.etcd.containerNames
    if (firstMember === undefined || secondMember === undefined) throw new Error('etcd started fewer than two members')

    await pauseContainer(firstMember)
    const firstAfterPause = itemRow(2, NODE_A, 'one-member-paused')
    await writeToPrimaryUntilAccepted(env, firstAfterPause, 20_000)
    acknowledged.push(firstAfterPause)
    await sleep(6_000)
    for (let id = 3; id <= 8; id++) {
      const row = itemRow(id, NODE_A, 'one-member-paused')
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await expectGroupHolds(env, NODE_A, 1n, 1_000)

    await pauseContainer(secondMember)
    await waitForReadiness(
      env,
      NODE_A,
      readiness => readiness.replication?.coordinator?.connected === false,
      25_000,
      'a coordinator without quorum',
    )
    let probeId = 80
    for (const nodeId of NODE_IDS) {
      probeId += 1
      await expectRejectsWith(requireNode(env, nodeId).localWriteProbe(probeId, 'no-etcd-quorum'), COORDINATOR_REFUSALS)
    }
    await expectItemsAbsentOnDisk(env, NODE_IDS, [81, 82, 83], 2_000)

    await unpauseContainer(secondMember)
    await unpauseContainer(firstMember)
    const afterQuorum = itemRow(9, 'failover-client', 'quorum-restored')
    await writeToPrimaryUntilAccepted(env, afterQuorum, 30_000)
    acknowledged.push(afterQuorum)
    await waitForItemsOnDisk(env, NODE_IDS, acknowledged, 20_000)
    await expectItemsAbsentOnDisk(env, NODE_IDS, [81, 82, 83], 1_000)
    const settled = await waitForPrimary(env, NODE_IDS, 5_000)
    await expectExactlyOneWritablePrimary(env, settled.currentPrimary.nodeId, settled.primaryTerm)
  })
})
