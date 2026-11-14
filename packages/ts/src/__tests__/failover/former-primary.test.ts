import { describe, expect, it } from 'vitest'
import { waitForIdenticalFiles } from './gate-assertions.js'
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
import { waitForNodeReady } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { rebuildThroughRepair, startCluster } from './gate-scenarios.js'
import { waitForCondition } from './gate-support.js'
import { type ItemRow, readItemIds, readMetaValue, readRolledBackItemInsertIds } from './sqlite-oracle.js'

const LOCAL_ONLY_IDS = [50, 51, 52]
const QUARANTINED_META_KEY = 'replication_quarantined'

describe('a former primary that comes back', () => {
  const environment = useFailoverEnvironment()

  it('rolls back the writes that only a returning former primary held, keeps them for review, and takes it back without an operator', async () => {
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
    const promoted = (await waitForPrimary(env, [NODE_B, NODE_C], 30_000)).currentPrimary.nodeId

    await setEtcdLink(env, NODE_A, true)
    await setReplicationLinks(env, NODE_A, true)
    const rejoined = await waitForGroup(
      env,
      group => group.inSyncNodeIds.includes(NODE_A),
      30_000,
      `${NODE_A} back in the in-sync set`,
    )
    expect(rejoined.faultedNodeIds).not.toContain(NODE_A)
    expect(rejoined.repairingNodeIds).not.toContain(NODE_A)

    const afterReturn = itemRow(3, promoted, 'after-the-return')
    await writeItemMajority(requireNode(env, promoted), afterReturn)
    acknowledged.push(afterReturn)
    const rows = await waitForIdenticalFiles(env, NODE_IDS, 20_000)
    expect(rows).toEqual(acknowledged)
    expect(readRolledBackItemInsertIds(dbPathFor(env, NODE_A))).toEqual(LOCAL_ONLY_IDS)
    expect(readMetaValue(dbPathFor(env, NODE_A), QUARANTINED_META_KEY)).not.toBe('true')
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
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
    expect(readRolledBackItemInsertIds(dbPathFor(env, NODE_A))).toEqual(LOCAL_ONLY_IDS)
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
