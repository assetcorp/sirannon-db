import { describe, expect, it } from 'vitest'
import { expectRejectsWith, waitForItemsOnDisk } from './gate-assertions.js'
import { waitForGroup, waitForInSyncSet, waitForPrimary } from './gate-cluster-state.js'
import {
  CLUSTER_ID,
  dbPathFor,
  type GateEnvironment,
  GROUP_ID,
  grpcProxyName,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  requireNode,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForStatus } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { rebuildThroughRepair, restartNode, startCluster } from './gate-scenarios.js'
import { type ItemRow, readHighestOwnChangeSeq, readItems } from './sqlite-oracle.js'

async function writeRows(
  env: GateEnvironment,
  nodeId: string,
  ids: readonly number[],
  note: string,
): Promise<ItemRow[]> {
  const rows: ItemRow[] = []
  for (const id of ids) {
    const row = itemRow(id, nodeId, note)
    await writeItemMajority(requireNode(env, nodeId), row)
    rows.push(row)
  }
  return rows
}

async function expectAdmittedHoldingRows(
  env: GateEnvironment,
  nodeId: string,
  rows: readonly ItemRow[],
): Promise<void> {
  await waitForGroup(env, group => group.inSyncNodeIds.includes(nodeId), 20_000, `${nodeId} back in the in-sync set`)
  const stored = new Map(readItems(dbPathFor(env, nodeId)).map(row => [row.id, row]))
  for (const row of rows) {
    expect(stored.get(row.id), `${nodeId} was admitted without row ${row.id}`).toEqual(row)
  }
}

describe('the in-sync set and its durability point', () => {
  const environment = useFailoverEnvironment()

  it('records the durability point of each acknowledged write and refuses to readmit a voter without proof', async () => {
    const env = environment()
    await startCluster(env)
    await setReplicationLinks(env, NODE_C, false)
    await writeRows(env, NODE_A, [1, 2, 3], 'c-is-cut-off')

    const afterWrites = await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    expect(afterWrites.durabilityPointSeq).toBe(readHighestOwnChangeSeq(dbPathFor(env, NODE_A), NODE_A))

    await expect(
      env.coordinator.updateInSyncSet({
        clusterId: CLUSTER_ID,
        groupId: GROUP_ID,
        inSyncNodeIds: [NODE_A, NODE_B, NODE_C],
      }),
    ).rejects.toThrow(/catch-up proof/)
    await env.coordinator.admitNodeToInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_C,
      sourceNodeId: NODE_A,
      appliedSeq: afterWrites.durabilityPointSeq - 1n,
    })
    await env.coordinator.admitNodeToInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_C,
      sourceNodeId: NODE_B,
      appliedSeq: afterWrites.durabilityPointSeq,
    })
    const afterRefusals = await env.etcdOracle.group()
    expect(afterRefusals?.inSyncNodeIds).toEqual([NODE_A, NODE_B])

    await expectRejectsWith(
      requireNode(env, NODE_A).execute(
        'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
        [4, NODE_A, 4, 'all-without-c'],
        { writeConcern: { level: 'all', timeoutMs: 1_500 } },
      ),
      ['WRITE_CONCERN_ERROR'],
    )
    expect((await env.etcdOracle.group())?.durabilityPointSeq).toBe(afterWrites.durabilityPointSeq)

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_C,
      draining: true,
    })
    await requireNode(env, NODE_A).execute(
      'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
      [5, NODE_A, 5, 'all-with-c-drained'],
      { writeConcern: { level: 'all', timeoutMs: 5_000 } },
    )
    const afterDrain = await env.etcdOracle.group()
    expect(afterDrain?.durabilityPointSeq).toBe(readHighestOwnChangeSeq(dbPathFor(env, NODE_A), NODE_A))
    expect(afterDrain?.durabilityPointSeq).toBeGreaterThan(afterWrites.durabilityPointSeq)
    expect(afterDrain?.inSyncNodeIds).toEqual([NODE_A, NODE_B])
  })

  it('readmits a late replica only once its file holds every row up to the durability point', async () => {
    const env = environment()
    await startCluster(env)
    const written: ItemRow[] = []

    for (const proxy of [grpcProxyName(NODE_A, NODE_C), grpcProxyName(NODE_C, NODE_A)]) {
      await env.toxiproxy.addLatency(proxy, 'slow-third-node', 400)
    }
    written.push(...(await writeRows(env, NODE_A, [1, 2, 3, 4, 5], 'third-node-is-slow')))
    await expectAdmittedHoldingRows(env, NODE_C, written)
    for (const proxy of [grpcProxyName(NODE_A, NODE_C), grpcProxyName(NODE_C, NODE_A)]) {
      await env.toxiproxy.clearToxic(proxy, 'slow-third-node')
    }

    await setReplicationLinks(env, NODE_C, false)
    written.push(...(await writeRows(env, NODE_A, [6, 7, 8], 'third-node-cut-off')))
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    await setReplicationLinks(env, NODE_C, true)
    await expectAdmittedHoldingRows(env, NODE_C, written)

    await killNode(env, NODE_C)
    written.push(...(await writeRows(env, NODE_A, [9, 10, 11], 'third-node-down')))
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    await restartNode(env, NODE_C)
    await expectAdmittedHoldingRows(env, NODE_C, written)
  })

  it('delivers every write that the primary acknowledges while a rebuilt replica copies its tables', async () => {
    const env = environment()
    await startCluster(env)
    const written = await writeRows(env, NODE_A, [1, 2, 3], 'before-the-rebuild')
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    let writing = true
    const writer = (async () => {
      for (let id = 100; writing; id++) {
        written.push(...(await writeRows(env, NODE_A, [id], 'during-the-rebuild')))
      }
    })()
    try {
      const rebuilt = await rebuildThroughRepair(env, NODE_C, NODE_A)
      await waitForStatus(rebuilt, status => status.syncState?.phase === 'ready', 30_000, 'a finished copy')
    } finally {
      writing = false
      await writer
    }
    expect(written.length).toBeGreaterThan(3)
    await waitForGroup(
      env,
      group => group.inSyncNodeIds.includes(NODE_C) && !group.repairingNodeIds.includes(NODE_C),
      30_000,
      `${NODE_C} rebuilt and back in the in-sync set`,
    )
    await waitForItemsOnDisk(env, [NODE_C], written, 20_000)
  })

  it('delivers the writes that a replica missed before a failover from the promoted node before readmitting it', async () => {
    const env = environment()
    await startCluster(env)
    await setReplicationLinks(env, NODE_C, false)
    const beforeFailover = await writeRows(env, NODE_A, [1, 2, 3], 'acknowledged-by-a-and-b')
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_B], 30_000)
    expect(promotion.primaryTerm).toBe(2n)
    await setReplicationLinks(env, NODE_C, true, [NODE_B])
    const afterFailover = await writeRows(env, NODE_B, [4], 'acknowledged-by-b-and-c')

    await waitForItemsOnDisk(env, [NODE_C], [...beforeFailover, ...afterFailover], 20_000)
    await expectAdmittedHoldingRows(env, NODE_C, [...beforeFailover, ...afterFailover])
  })
})
