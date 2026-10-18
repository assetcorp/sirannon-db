import { describe, expect, it } from 'vitest'
import { expectItemsAbsentOnDisk, waitForItemsOnDisk } from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  waitForControllerHolder,
  waitForGroup,
  waitForInSyncSet,
  waitForPrimary,
} from './gate-cluster-state.js'
import { killNode, NODE_A, NODE_B, NODE_C, NODE_IDS, requireNode } from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { recentErrorCount, waitForReadiness, waitForRecentErrorCountAbove } from './gate-observations.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { restartNode, startCluster } from './gate-scenarios.js'
import { holdsThroughout, sleep, waitForCondition } from './gate-support.js'
import type { ItemRow } from './sqlite-oracle.js'

describe('the controller lease', () => {
  const environment = useFailoverEnvironment()

  it('leaves a held lease alone, hands it over at once on shutdown, and hands it over again once a killed holder lapses', async () => {
    const env = environment()
    await startCluster(env, { perNode: { [NODE_A]: { controllerEnabled: false } } })
    const holder = await waitForControllerHolder(env, [NODE_B, NODE_C], 10_000)
    const standby = holder === NODE_B ? NODE_C : NODE_B
    const held = await env.etcdOracle.controller()
    if (!held) throw new Error('etcd holds no controller lease')
    const grantsBefore = await env.etcdOracle.leaseGrantedTotal()

    await holdsThroughout(
      async () => {
        const current = await env.etcdOracle.controller()
        return current?.holderId === holder && current.createRevision === held.createRevision
      },
      3_000,
      `${holder} to keep the controller lease it already holds`,
    )
    expect(await env.etcdOracle.leaseGrantedTotal()).toBe(grantsBefore)

    await requireNode(env, holder).shutdown()
    env.nodes.delete(holder)
    const releasedWithin = Date.now() + 500
    await waitForCondition(async () => {
      const controller = await env.etcdOracle.controller()
      const session = await env.etcdOracle.nodeSession(holder)
      return session === null && controller?.holderId !== holder
    }, 500)
    expect(Date.now()).toBeLessThanOrEqual(releasedWithin + 100)
    const takeover = await waitForControllerHolder(env, [standby], 5_000)
    expect(takeover).toBe(standby)
    const handedOver = await env.etcdOracle.controller()
    expect(handedOver?.createRevision).toBeGreaterThan(held.createRevision)

    await restartNode(env, holder)
    await killNode(env, standby)
    const afterKill = await waitForControllerHolder(env, [holder], 10_000)
    expect(afterKill).toBe(holder)
    expect((await env.etcdOracle.controller())?.createRevision).toBeGreaterThan(handedOver?.createRevision ?? 0n)
    expect((await env.etcdOracle.group())?.currentPrimary?.nodeId).toBe(NODE_A)
  })

  it('leaves a held lease alone when the holder clock runs twenty seconds behind the standby clocks', async () => {
    const env = environment()
    await startCluster(env, {
      perNode: {
        [NODE_A]: { clockOffsetMs: -20_000 },
        [NODE_B]: { clockOffsetMs: 20_000 },
      },
    })
    const holder = await waitForControllerHolder(env, NODE_IDS, 10_000)
    expect(holder).toBe(NODE_A)
    const grantsBefore = await env.etcdOracle.leaseGrantedTotal()
    await sleep(3_000)
    expect(await env.etcdOracle.leaseGrantedTotal()).toBe(grantsBefore)
  })

  it('keeps one controller and one writable primary while node clocks run forty seconds apart, through a primary loss', async () => {
    const env = environment()
    await startCluster(env, {
      perNode: {
        [NODE_A]: { clockOffsetMs: -20_000 },
        [NODE_B]: { clockOffsetMs: 20_000 },
      },
    })
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 3; id++) {
      const row = itemRow(id, NODE_A, 'clocks-apart')
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForItemsOnDisk(env, NODE_IDS, acknowledged, 15_000)
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await expectExactlyOneWritablePrimary(env, NODE_A, 1n)

    await killNode(env, NODE_A)
    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(promotion.primaryTerm).toBe(2n)
    const promoted = promotion.currentPrimary.nodeId
    await waitForControllerHolder(env, [NODE_B, NODE_C], 10_000)
    const afterLoss = itemRow(4, promoted, 'clocks-apart-after-loss')
    await writeItemMajority(requireNode(env, promoted), afterLoss)
    acknowledged.push(afterLoss)
    await waitForItemsOnDisk(env, [NODE_B, NODE_C], acknowledged, 15_000)
    await expectExactlyOneWritablePrimary(env, promoted, 2n)
  })

  it('accepts batches stamped more than a minute behind the replica clock, and keeps the replica in the in-sync set', async () => {
    const env = environment()
    await startCluster(env, { perNode: { [NODE_C]: { clockOffsetMs: 90_000 } } })
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 3; id++) {
      const row = itemRow(id, NODE_A, 'replica-clock-far-ahead')
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForItemsOnDisk(env, NODE_IDS, acknowledged, 15_000)
    await waitForInSyncSet(env, NODE_IDS, 15_000)
    await holdsThroughout(
      async () => {
        const group = await env.etcdOracle.group()
        return group?.inSyncNodeIds.includes(NODE_C) === true
      },
      2_000,
      `${NODE_C} to stay in the in-sync set`,
    )
    expect(await recentErrorCount(requireNode(env, NODE_C), 'BATCH_VALIDATION_ERROR')).toBe(0)
  })

  it('refuses batches stamped more than a minute ahead of the replica clock, and readmits the replica once its clock is right', async () => {
    const env = environment()
    await startCluster(env, { perNode: { [NODE_C]: { clockOffsetMs: -90_000 } } })
    const replica = requireNode(env, NODE_C)
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 3; id++) {
      const row = itemRow(id, NODE_A, 'replica-clock-far-behind')
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForRecentErrorCountAbove(replica, 'BATCH_VALIDATION_ERROR', 0, 10_000)
    await waitForInSyncSet(env, [NODE_A, NODE_B], 10_000)
    await waitForItemsOnDisk(env, [NODE_A, NODE_B], acknowledged, 10_000)
    await expectItemsAbsentOnDisk(env, [NODE_C], [1, 2, 3], 2_000)
    const lagging = await waitForReadiness(
      env,
      NODE_A,
      readiness => readiness.replication?.laggingReplicas?.includes(NODE_C) === true,
      10_000,
      `${NODE_C} lagging`,
    )
    expect(lagging.replication?.inSyncReplicas).toEqual([NODE_B])

    await killNode(env, NODE_C)
    await restartNode(env, NODE_C, { clockOffsetMs: 0 })
    await waitForItemsOnDisk(env, [NODE_C], acknowledged, 20_000)
    await waitForGroup(env, group => group.inSyncNodeIds.includes(NODE_C), 20_000, `${NODE_C} readmitted`)
  })
})
