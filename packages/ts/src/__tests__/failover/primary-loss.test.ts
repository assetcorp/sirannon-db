import { describe, expect, it } from 'vitest'
import { SirannonClient } from '../../client/index.js'
import type { ChangeEvent } from '../../core/query-types.js'
import { waitForIdenticalFiles, waitForItemsOnDisk } from './gate-assertions.js'
import {
  expectExactlyOneWritablePrimary,
  waitForControllerHolder,
  waitForInSyncSet,
  waitForPrimary,
} from './gate-cluster-state.js'
import {
  dbPathFor,
  GROUP_ID,
  killNode,
  NODE_A,
  NODE_B,
  NODE_C,
  requireNode,
  serverBaseUrlFor,
  setReplicationLinks,
} from './gate-environment.js'
import { useFailoverEnvironment } from './gate-lifecycle.js'
import { itemRow, writeItemMajority } from './gate-public-api.js'
import { restartNode, startCluster } from './gate-scenarios.js'
import { waitForCondition } from './gate-support.js'
import { type ItemRow, readItemIds } from './sqlite-oracle.js'

describe('losing the primary', () => {
  const environment = useFailoverEnvironment()

  it('promotes an in-sync replica, keeps every majority write, and promotes again when the new primary dies at once', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 5; id++) {
      const row = itemRow(id, NODE_A, `before-loss-${id}`)
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await expectExactlyOneWritablePrimary(env, NODE_A, 1n)
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    await killNode(env, NODE_A)
    const firstPromotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    expect(firstPromotion.primaryTerm).toBe(2n)
    expect(firstPromotion.repairingNodeIds).toContain(NODE_A)
    expect(firstPromotion.inSyncNodeIds).not.toContain(NODE_A)
    const promoted = firstPromotion.currentPrimary.nodeId
    const survivor = promoted === NODE_B ? NODE_C : NODE_B

    await killNode(env, promoted)
    const secondPromotion = await waitForPrimary(env, [survivor], 30_000)
    expect(secondPromotion.primaryTerm).toBe(3n)
    expect(secondPromotion.repairingNodeIds).toContain(promoted)
    await waitForItemsOnDisk(env, [survivor], acknowledged, 10_000)

    await restartNode(env, promoted, { role: 'primary' })
    await waitForInSyncSet(env, [survivor, promoted], 30_000)
    for (let id = 6; id <= 8; id++) {
      const row = itemRow(id, survivor, `after-second-loss-${id}`)
      await writeItemMajority(requireNode(env, survivor), row)
      acknowledged.push(row)
    }
    await waitForItemsOnDisk(env, [survivor, promoted], acknowledged, 20_000)
    await expectExactlyOneWritablePrimary(env, survivor, 3n)
  })

  it('promotes the last replica when the primary and the controller holder die in the same instant', async () => {
    const env = environment()
    await startCluster(env, { perNode: { [NODE_A]: { controllerEnabled: false } } })
    const acknowledged: ItemRow[] = []
    for (let id = 1; id <= 3; id++) {
      const row = itemRow(id, NODE_A, `before-double-loss-${id}`)
      await writeItemMajority(requireNode(env, NODE_A), row)
      acknowledged.push(row)
    }
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)
    const holder = await waitForControllerHolder(env, [NODE_B, NODE_C], 10_000)
    const remaining = holder === NODE_B ? NODE_C : NODE_B

    await Promise.all([killNode(env, NODE_A), killNode(env, holder)])

    const promotion = await waitForPrimary(env, [remaining], 30_000)
    expect(promotion.primaryTerm).toBe(2n)
    await waitForCondition(async () => (await env.etcdOracle.controller())?.holderId === remaining, 10_000)
    await waitForItemsOnDisk(env, [remaining], acknowledged, 10_000)
    await expectExactlyOneWritablePrimary(env, remaining, 2n)
  })

  it('discards the rows of the lost primary that the promoted node never received', async () => {
    const env = environment()
    await startCluster(env)
    const acknowledged = itemRow(1, NODE_A, 'acknowledged-by-majority')
    await writeItemMajority(requireNode(env, NODE_A), acknowledged)
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)

    await setReplicationLinks(env, NODE_A, false, [NODE_B])
    const primary = requireNode(env, NODE_A)
    for (let id = 2; id <= 4; id++) {
      await primary.execute(
        'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
        [id, NODE_A, id, 'local-only'],
        { writeConcern: { level: 'local' } },
      )
    }
    await waitForCondition(async () => [2, 3, 4].every(id => readItemIds(dbPathFor(env, NODE_C)).includes(id)), 10_000)
    await killNode(env, NODE_A)

    const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
    const promoted = promotion.currentPrimary.nodeId
    const other = promoted === NODE_B ? NODE_C : NODE_B
    await writeItemMajority(requireNode(env, promoted), itemRow(5, promoted, 'after-promotion'))

    const survivingRows = await waitForIdenticalFiles(env, [promoted, other], 20_000)
    expect(survivingRows.map(row => row.id)).toContain(1)
    expect(survivingRows.map(row => row.id)).toContain(5)
  })

  it('sends the subscribers and live queries of a replica a delete for every row that it discards after a failover', async () => {
    const env = environment()
    await startCluster(env)
    await writeItemMajority(requireNode(env, NODE_A), itemRow(1, NODE_A, 'acknowledged-by-majority'))
    await waitForInSyncSet(env, [NODE_A, NODE_B, NODE_C], 15_000)
    const replica = requireNode(env, NODE_C)
    await replica.openLiveItems()
    const client = new SirannonClient(serverBaseUrlFor(env, NODE_C), { transport: 'websocket', autoReconnect: false })
    const events: ChangeEvent[] = []
    const subscription = await client
      .database(GROUP_ID)
      .on('failover_items')
      .subscribe(event => {
        events.push(event)
      })
    try {
      await setReplicationLinks(env, NODE_A, false, [NODE_B])
      for (let id = 2; id <= 4; id++) {
        await requireNode(env, NODE_A).execute(
          'INSERT INTO failover_items (id, owner, value, note) VALUES (?, ?, ?, ?)',
          [id, NODE_A, id, 'local-only'],
          { writeConcern: { level: 'local' } },
        )
      }
      await waitForCondition(async () => (await replica.liveItemIds())?.join(',') === '1,2,3,4', 10_000)
      await waitForCondition(async () => idsOf(events, 'insert').join(',') === '2,3,4', 10_000)
      await killNode(env, NODE_A)

      const promotion = await waitForPrimary(env, [NODE_B, NODE_C], 30_000)
      expect(promotion.currentPrimary.nodeId).toBe(NODE_B)
      await waitForCondition(async () => readItemIds(dbPathFor(env, NODE_C)).join(',') === '1', 20_000)
      await waitForCondition(async () => (await replica.liveItemIds())?.join(',') === '1', 10_000)
      await waitForCondition(async () => idsOf(events, 'delete').join(',') === '2,3,4', 10_000)
      const deletes = events.filter(event => event.type === 'delete')
      expect(deletes.map(event => event.oldRow?.note)).toEqual(['local-only', 'local-only', 'local-only'])
      expect(idsOf(events, 'insert')).toEqual([2, 3, 4])
    } finally {
      subscription.unsubscribe()
      client.close()
    }
  })
})

function idsOf(events: readonly ChangeEvent[], type: string): number[] {
  const source = (event: ChangeEvent) => (type === 'delete' ? event.oldRow : event.row)
  return events
    .filter(event => event.type === type)
    .map(event => Number(source(event)?.id))
    .sort((left, right) => left - right)
}
