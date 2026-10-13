import { statSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { expectRejectsWith, waitForIdenticalFiles, waitForItemsOnDisk } from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  expectGroupHolds,
  waitForGroup,
  waitForInSyncSet,
  waitForPrimary,
  waitForSessionGone,
} from './gate-cluster-state.js'
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
  NODE_IDS,
  operatorCoordinator,
  requireNode,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForNoSafePrimaryReport } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import { type ItemRow, readHighestAppliedSeq, readHighestOwnChangeSeq } from './sqlite-oracle.js'

const WRITE_REFUSALS = ['TOPOLOGY_ERROR', 'STALE_PRIMARY', 'NO_SAFE_PRIMARY', 'NODE_DRAINING', 'AUTHORITY_LOST']
const STORAGE_CEILING_BYTES = 64 * 1024 * 1024

async function writeRows(env: GateEnvironment, nodeId: string, ids: readonly number[], note: string) {
  const rows: ItemRow[] = []
  for (const id of ids) {
    const row = itemRow(id, nodeId, note)
    await writeItemMajority(requireNode(env, nodeId), row)
    rows.push(row)
  }
  return rows
}

function storageBytes(dbPath: string): number {
  return [dbPath, `${dbPath}-wal`, `${dbPath}-shm`].reduce((total, path) => {
    try {
      return total + statSync(path).size
    } catch {
      return total
    }
  }, 0)
}

describe('operators changing a running group', () => {
  const environment = useFailoverEnvironment()

  it('hands primary duty away from a draining primary, takes the node back once undrained, and keeps every file equal under a slow link', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged = await writeRows(env, NODE_A, [1, 2, 3], 'before-the-drain')
    await waitForInSyncSet(env, NODE_IDS, 15_000)

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_A,
      draining: true,
    })
    const handover = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(handover.primaryTerm).toBe(2n)
    expect(handover.drainingNodeIds).toContain(NODE_A)
    const promoted = handover.currentPrimary.nodeId
    const other = promoted === NODE_B ? NODE_C : NODE_B
    await expectRejectsWith(requireNode(env, NODE_A).localWriteProbe(90, 'on-the-drained-node'), [
      ...WRITE_REFUSALS,
      'SYNC_ERROR',
    ])
    acknowledged.push(...(await writeRows(env, promoted, [4], 'after-the-handover')))

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_A,
      draining: false,
    })
    acknowledged.push(...(await writeRows(env, promoted, [5], 'after-the-undrain')))
    await waitForGroup(env, group => group.inSyncNodeIds.includes(NODE_A), 30_000, `${NODE_A} back in the in-sync set`)

    for (const proxy of [grpcProxyName(promoted, other), grpcProxyName(other, promoted)]) {
      await env.toxiproxy.addLatency(proxy, 'slow-replica-link', 40)
    }
    for (let id = 20; id < 32; id++) {
      acknowledged.push(...(await writeRows(env, promoted, [id], 'under-a-slow-link')))
      if (id % 3 === 0) {
        await requireNode(env, promoted).execute(
          "UPDATE failover_items SET note = 'updated-under-a-slow-link' WHERE id = ?",
          [id],
          { writeConcern: { level: 'majority', timeoutMs: 15_000 } },
        )
        const index = acknowledged.findIndex(row => row.id === id)
        acknowledged[index] = itemRow(id, promoted, 'updated-under-a-slow-link')
      }
    }
    const files = await waitForIdenticalFiles(env, NODE_IDS, 30_000)
    expect(files).toEqual([...acknowledged].sort((left, right) => left.id - right.id))
    for (const nodeId of NODE_IDS) {
      expect(storageBytes(dbPathFor(env, nodeId))).toBeLessThan(STORAGE_CEILING_BYTES)
    }
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
  })

  it('keeps both changes when two operators update maintenance flags on the group at the same instant', async () => {
    const env = environment()
    await startCluster(env)
    const second = operatorCoordinator(env.etcd, env.keyPrefix)
    try {
      for (let round = 0; round < 5; round++) {
        await Promise.all([
          env.coordinator.updateNodeMaintenance({
            clusterId: CLUSTER_ID,
            groupId: GROUP_ID,
            nodeId: NODE_B,
            draining: true,
          }),
          second.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId: NODE_C, repairing: true }),
        ])
        const marked = await env.etcdOracle.group()
        expect(marked?.drainingNodeIds).toContain(NODE_B)
        expect(marked?.repairingNodeIds).toContain(NODE_C)
        await Promise.all([
          env.coordinator.updateNodeMaintenance({
            clusterId: CLUSTER_ID,
            groupId: GROUP_ID,
            nodeId: NODE_B,
            draining: false,
          }),
          second.updateNodeMaintenance({ clusterId: CLUSTER_ID, groupId: GROUP_ID, nodeId: NODE_C, repairing: false }),
        ])
        const cleared = await env.etcdOracle.group()
        expect(cleared?.drainingNodeIds).not.toContain(NODE_B)
        expect(cleared?.repairingNodeIds).not.toContain(NODE_C)
      }
    } finally {
      await second.close()
    }
    await expectGroupHolds(env, NODE_A, 1n, 1_000)
    const row = (await writeRows(env, NODE_A, [1], 'after-the-race'))[0]
    if (!row) throw new Error('no row was written')
    await waitForInSyncSet(env, NODE_IDS, 20_000)
    await waitForItemsOnDisk(env, NODE_IDS, [row], 15_000)
  })

  it('promotes nobody while every replica is set aside, and promotes the replica that an operator vouches for with its applied position', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged = await writeRows(env, NODE_A, [1, 2], 'before-every-replica-is-set-aside')
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_B,
      draining: true,
    })
    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_C,
      repairing: true,
    })
    const durabilityPoint = readHighestOwnChangeSeq(dbPathFor(env, NODE_A), NODE_A)

    await killNode(env, NODE_A)
    await waitForSessionGone(env, NODE_A, 10_000)
    await waitForNoSafePrimaryReport(env)
    await expectGroupHolds(env, NODE_A, 1n, 3_000)
    for (const nodeId of [NODE_B, NODE_C]) {
      await expectRejectsWith(
        requireNode(env, nodeId).localWriteProbe(80, 'with-every-replica-set-aside'),
        WRITE_REFUSALS,
      )
    }

    await env.coordinator.updateNodeMaintenance({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_B,
      draining: false,
    })
    await expectGroupHolds(env, NODE_A, 1n, 2_000)
    const appliedOnB = readHighestAppliedSeq(dbPathFor(env, NODE_B), NODE_A)
    expect(appliedOnB).toBe(durabilityPoint)
    await env.coordinator.admitNodeToInSyncSet({
      clusterId: CLUSTER_ID,
      groupId: GROUP_ID,
      nodeId: NODE_B,
      sourceNodeId: NODE_A,
      appliedSeq: appliedOnB,
    })
    const promotion = await waitForPrimary(env, [NODE_B], 15_000)
    expect(promotion.primaryTerm).toBe(2n)
    await waitForItemsOnDisk(env, [NODE_B], acknowledged, 5_000)
    await expectExactlyOneWritablePrimary(env, NODE_B, 2n)
  })
})
