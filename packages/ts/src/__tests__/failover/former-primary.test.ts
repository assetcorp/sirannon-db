import { describe, expect, it } from 'vitest'
import { expectRejectsWith, waitForIdenticalFiles, waitForItemsOnDisk } from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  waitForGroup,
  waitForInSyncSet,
  waitForPrimary,
  waitForSessionGone,
} from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  dbPathFor,
  GROUP_ID,
  healNodeSirannonLinks,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  NODE_IDS,
  requireNode,
  setEtcdLink,
  setReplicationLinks,
  startNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { readableNodeIds, waitForNodeReady, waitForReadiness } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { rebuildThroughRepair, startCluster } from './gate-scenarios.js'
import { holdsThroughout, waitForCondition } from './gate-support.js'
import { type ItemRow, readItemIds, readMetaValue } from './sqlite-oracle.js'

const LOCAL_ONLY_IDS = [50, 51, 52]
const QUARANTINED_META_KEY = 'replication_quarantined'

describe('a former primary that comes back', () => {
  const environment = useFailoverEnvironment()

  it('faults a former primary holding writes that nobody else received, keeps it out of reads and promotion, and takes it back once an operator rebuilds it', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 2; id++) {
      const row = itemRow(id, NODE_A, `shared-${id}`)
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await setReplicationLinks(env, NODE_A, false)
    for (const id of LOCAL_ONLY_IDS) {
      await requireNode(env, NODE_A).localWriteProbe(id, 'only-on-the-old-primary')
    }
    expect(readItemIds(dbPathFor(env, NODE_A))).toEqual(expect.arrayContaining(LOCAL_ONLY_IDS))
    await setEtcdLink(env, NODE_A, false)
    await waitForSessionGone(env, NODE_A, 15_000)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    const promoted = promotion.currentPrimary.nodeId
    const survivor = promoted === NODE_B ? NODE_C : NODE_B

    await setEtcdLink(env, NODE_A, true)
    await setReplicationLinks(env, NODE_A, true)
    const faulted = await waitForGroup(
      env,
      group => group.faultedNodeIds.includes(NODE_A),
      20_000,
      `${NODE_A} faulted for its local-only writes`,
    )
    expect(faulted.inSyncNodeIds).not.toContain(NODE_A)
    expect(faulted.repairingNodeIds).not.toContain(NODE_A)
    const readiness = await waitForReadiness(
      env,
      NODE_A,
      latest => latest.replication?.healthReason === 'faulted',
      10_000,
      'the faulted reason',
    )
    expect(readiness.status).toBe('unavailable')
    expect(readiness.replication?.writeAvailability).toBe('unavailable')
    expect(readiness.replication?.readAvailability).toBe('unavailable')
    await expectRejectsWith(requireNode(env, NODE_A).localWriteProbe(60, 'from-the-faulted-node'), [
      'STALE_PRIMARY',
      'TOPOLOGY_ERROR',
      'AUTHORITY_LOST',
    ])
    for (const nodeId of NODE_IDS) {
      expect(await readableNodeIds(env, nodeId)).not.toContain(NODE_A)
    }
    await holdsThroughout(
      async () => {
        const group = await env.etcdOracle.group()
        return group?.faultedNodeIds.includes(NODE_A) === true && !group.inSyncNodeIds.includes(NODE_A)
      },
      3_000,
      `${NODE_A} to stay faulted and out of the in-sync set`,
    )

    await killNode(env, promoted)
    const secondPromotion = await waitForPrimary(env, [survivor], 30_000)
    expect(secondPromotion.primaryTerm).toBe(3n)
    expect(secondPromotion.faultedNodeIds).toContain(NODE_A)
    await waitForItemsOnDisk(env, [survivor], acknowledged, 10_000)

    await rebuildThroughRepair(env, NODE_A, survivor)
    await waitForGroup(
      env,
      group => group.inSyncNodeIds.includes(NODE_A) && !group.faultedNodeIds.includes(NODE_A),
      30_000,
      `${NODE_A} rebuilt and back in the in-sync set`,
    )
    const afterRebuild = itemRow(3, survivor, 'after-the-rebuild')
    await writeItemMajority(requireNode(env, survivor), afterRebuild)
    acknowledged.push(afterRebuild)
    const rebuiltRows = await waitForIdenticalFiles(env, [survivor, NODE_A], 20_000)
    expect(rebuiltRows).toEqual(acknowledged)
    await expectExactlyOneWritablePrimary(env, survivor, 3n)
  })

  it('records quarantine on restart for a former primary that an operator faulted while it was down, and rebuilds it once an operator marks it repairing', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 2; id++) {
      const row = itemRow(id, NODE_A, `shared-${id}`)
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await setReplicationLinks(env, NODE_A, false)
    for (const id of LOCAL_ONLY_IDS) {
      await requireNode(env, NODE_A).localWriteProbe(id, 'only-on-the-old-primary')
    }
    await setEtcdLink(env, NODE_A, false)
    await waitForSessionGone(env, NODE_A, 15_000)
    const promoted = (await waitForPrimary(env, [NODE_B, NODE_C], 30_000)).currentPrimary.nodeId
    await killNode(env, NODE_A)
    expect(readMetaValue(dbPathFor(env, NODE_A), QUARANTINED_META_KEY)).not.toBe('true')

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_A,
      draining: false,
      repairing: false,
      faulted: true,
    })
    await healNodeSirannonLinks(env, NODE_A)
    await waitForSessionGone(env, NODE_A, 15_000)
    await startNode(env, NODE_A)
    await waitForCondition(async () => readMetaValue(dbPathFor(env, NODE_A), QUARANTINED_META_KEY) === 'true', 20_000)

    await rebuildThroughRepair(env, NODE_A, promoted)
    await waitForGroup(
      env,
      group => group.inSyncNodeIds.includes(NODE_A) && !group.faultedNodeIds.includes(NODE_A),
      30_000,
      `${NODE_A} rebuilt and back in the in-sync set`,
    )
    const afterRebuild = itemRow(3, promoted, 'after-the-rebuild')
    await writeItemMajority(requireNode(env, promoted), afterRebuild)
    acknowledged.push(afterRebuild)
    const rebuiltRows = await waitForIdenticalFiles(env, NODE_IDS, 20_000)
    expect(rebuiltRows).toEqual(acknowledged)
    expect(readMetaValue(dbPathFor(env, NODE_A), QUARANTINED_META_KEY)).toBe('false')
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
  })

  it('restarts a node whose previous session is still registered in etcd', async () => {
    const env = environment()
    await startCluster(env)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'before-the-crash'))

    await killNode(env, NODE_C)
    const restarted = await startNode(env, NODE_C)
    await waitForNodeReady(restarted, 30_000)
    await waitForInSyncSet(env, NODE_IDS, 30_000)
  })
})
