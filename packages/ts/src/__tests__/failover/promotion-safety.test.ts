import { describe, expect, it } from 'vitest'
import { expectItemsAbsentOnDisk, expectRejectsWith, waitForItemsOnDisk } from './gate-assertions.js'
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
  type GateEnvironment,
  GROUP_ID,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  requireNode,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { waitForNoSafePrimaryReport } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { startCluster } from './gate-scenarios.js'
import type { ItemRow } from './sqlite-oracle.js'

const WRITE_REFUSALS = ['TOPOLOGY_ERROR', 'STALE_PRIMARY', 'NO_SAFE_PRIMARY', 'COORDINATOR_UNAVAILABLE']

async function raiseCompatibilityGate(env: GateEnvironment, packageVersion: string): Promise<void> {
  const current = await env.coordinator.getReplicationGroupState(CLUSTER_ID, GROUP_ID)
  if (!current) throw new Error('etcd holds no group to raise the gate on')
  await env.coordinator.setReplicationGroupState({
    ...current,
    compatibility: { packageVersion, specVersion: '1.0.0', protocolVersion: '1.0.0' },
  })
}

describe('which node may be promoted', () => {
  const environment = useFailoverEnvironment()

  it('never promotes the survivor of a group with two voters', async () => {
    const env = environment()
    await startCluster(env, { members: [NODE_A, NODE_B], voters: [NODE_A, NODE_B] })
    const row = itemRow(1, NODE_A, 'two-voter-write')
    await writeItemMajority(requireNode(env, NODE_A), row)
    await waitForItemsOnDisk(env, [NODE_B], [row], 10_000)

    await killNode(env, NODE_A)
    await waitForSessionGone(env, NODE_A, 10_000)
    await waitForNoSafePrimaryReport(env)
    await expectGroupHolds(env, NODE_A, 1n, 4_000)
    await expectRejectsWith(requireNode(env, NODE_B).localWriteProbe(2, 'two-voter-survivor'), WRITE_REFUSALS)
    await expectItemsAbsentOnDisk(env, [NODE_B], [2], 1_000)
  })

  it('promotes only a replica in the in-sync set, and promotes nobody once that replica is gone', async () => {
    const env = environment()
    await startCluster(env)
    const first = itemRow(1, NODE_A, 'everyone-has-it')
    const acknowledged: ItemRow[] = [first]
    await writeItemMajority(requireNode(env, NODE_A), first)
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    await setReplicationLinks(env, NODE_C, false)
    for (let id = 2; id <= 4; id++) {
      const row = itemRow(id, NODE_A, 'c-never-sees-it')
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    const afterCut = await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    expect(afterCut.inSyncNodeIds).not.toContain(NODE_C)
    await expectItemsAbsentOnDisk(env, [NODE_C], [2, 3, 4], 1_000)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(promotion.currentPrimary.nodeId).toBe(NODE_B)
    expect(promotion.primaryTerm).toBe(2n)
    await waitForItemsOnDisk(env, [NODE_B], acknowledged, 10_000)

    await killNode(env, NODE_B)
    await waitForSessionGone(env, NODE_B, 10_000)
    await waitForNoSafePrimaryReport(env)
    await expectGroupHolds(env, NODE_B, 2n, 4_000)
    await expectRejectsWith(requireNode(env, NODE_C).localWriteProbe(5, 'out-of-sync-survivor'), WRITE_REFUSALS)
    await expectItemsAbsentOnDisk(env, [NODE_C], [2, 3, 4, 5], 1_000)
  })

  it('moves primary duty to the only node that meets a raised compatibility gate, and to nobody once none does', async () => {
    const env = environment()
    await startCluster(env, {
      perNode: {
        [NODE_C]: { compatibility: { packageVersion: '2.1.0', specVersion: '1.0.0', protocolVersion: '1.0.0' } },
      },
    })
    const row = itemRow(1, NODE_A, 'before-the-gate')
    await writeItemMajority(requireNode(env, NODE_A), row)
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)
    const session = await env.etcdOracle.nodeSession(NODE_C)
    expect(session?.compatibility?.packageVersion).toBe('2.1.0')

    await raiseCompatibilityGate(env, '2.0.0')
    await expectRejectsWith(requireNode(env, NODE_A).localWriteProbe(9, 'below-the-gate'), [
      'PROTOCOL_VERSION_MISMATCH',
      'TOPOLOGY_ERROR',
      'SYNC_ERROR',
    ])
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(promotion.currentPrimary.nodeId).toBe(NODE_C)
    expect(promotion.primaryTerm).toBe(2n)
    await expectExactlyOneWritablePrimary(env, NODE_C, 2n)
    await waitForItemsOnDisk(env, [NODE_C], [row], 10_000)
    await expectItemsAbsentOnDisk(env, [NODE_A, NODE_B, NODE_C], [9], 1_000)

    await raiseCompatibilityGate(env, '3.0.0')
    await waitForGroup(env, group => group.compatibility?.packageVersion === '3.0.0', 5_000, 'the raised gate')
    await expectRejectsWith(requireNode(env, NODE_C).localWriteProbe(10, 'below-the-second-gate'), [
      'PROTOCOL_VERSION_MISMATCH',
    ])
    await waitForNoSafePrimaryReport(env)
    await expectGroupHolds(env, NODE_C, 2n, 3_000)
    await expectItemsAbsentOnDisk(env, [NODE_A, NODE_B, NODE_C], [10], 1_000)
  })
})
