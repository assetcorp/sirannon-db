import { afterEach, describe, expect, it } from 'vitest'
import { TopologyAwareClient } from '../../../client/topology.js'
import type { Database } from '../../../core/database.js'
import { SirannonError } from '../../../core/errors.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { ChangeEvent, OperationRegistry, ServerExecutionTarget } from '../../../core/types.js'
import { waitUntil } from '../lib/served-registry.js'
import { DATABASE_ID, INSERT_ORDER, middlebox, pause, releaseClientServers, storedCustomers } from './harness.js'
import { coordinatedNode, healthyRouting, readable, routingHolder } from './topology-cluster.js'

const everyCustomer = operationRef<Record<string, never>, { customer: string }>('everyCustomer')
const OPERATIONS: OperationRegistry = {
  [DATABASE_ID]: {
    reads: { everyCustomer: { statement: () => ({ sql: 'SELECT customer FROM orders ORDER BY id' }) } },
  },
}

afterEach(releaseClientServers)

function customersSeen(events: ChangeEvent[]): unknown[] {
  return events.map(event => event.row.customer)
}

describe('subscriptions through a topology client', () => {
  it('opens one read connection for every subscription and live query it holds', async () => {
    const holder = routingHolder()
    const primary = await coordinatedNode('primary-row', holder, { operations: OPERATIONS })
    const replicaA = await coordinatedNode('replica-a-row', holder, { operations: OPERATIONS })
    const replicaB = await coordinatedNode('replica-b-row', holder, { operations: OPERATIONS })
    holder.current = healthyRouting(primary.base, [
      readable('primary', primary.base, 'local', 'majority', 'linearizable'),
      readable('a', replicaA.base, 'local', 'majority'),
      readable('b', replicaB.base, 'local', 'majority'),
    ])
    const client = new TopologyAwareClient({
      endpoints: [primary.base],
      discovery: 'coordinator',
      readPreference: 'replica',
      transport: 'websocket',
    })
    const db = client.database(DATABASE_ID)
    const seen: ChangeEvent[][] = Array.from({ length: 5 }, () => [])
    const tables = ['orders', 'orders', 'orders', 'orders', 'orders']

    const [live, ...subscriptions] = await Promise.all([
      db.live(everyCustomer, {}),
      ...tables.map((table, index) =>
        db.on(table).subscribe(event => {
          seen[index]?.push(event)
        }),
      ),
    ])
    const chosen = replicaA.tally.upgrades.length === 1 ? replicaA : replicaB
    await chosen.shop.database.execute(INSERT_ORDER, ['written-on-reader', 1])
    await waitUntil(() => seen.every(events => events.length === 1), 'one change on every subscription')
    await waitUntil(() => {
      const state = live?.getState()
      return state?.status === 'ready' && state.rows.length === 2
    }, 'the live query reading the new row')
    for (const subscription of subscriptions) subscription.unsubscribe()
    client.close()

    expect([primary, replicaA, replicaB].map(node => node.tally.upgrades.length).sort()).toEqual([0, 0, 1])
    expect(primary.tally.upgrades).toEqual([])
    expect(seen.map(customersSeen)).toEqual(Array(5).fill(['written-on-reader']))
    expect(live?.getState()).toMatchObject({
      status: 'ready',
      rows: storedCustomers(chosen.shop.path).map(customer => ({ customer })),
    })
  })

  it('moves its subscriptions to the read endpoint a refreshed route names, and raises ROUTING_ERROR when that endpoint refuses them', async () => {
    const holder = routingHolder()
    let staleWrites = 0
    let refuseOnA = false
    let primaryDatabase: Database | undefined
    const writer = (): Database => {
      if (primaryDatabase === undefined) throw new Error('The primary has not opened its database')
      return primaryDatabase
    }
    const staleOnce: ServerExecutionTarget = {
      query: (sql, params, options) => writer().query(sql, params, options),
      execute: (sql, params, options) => {
        if (staleWrites > 0) {
          staleWrites -= 1
          return Promise.reject(new SirannonError('This node is no longer the primary', 'STALE_PRIMARY'))
        }
        return writer().execute(sql, params, options)
      },
      transaction: fn => writer().transaction(fn),
    }
    const primary = await coordinatedNode('primary-row', holder, { resolveExecutionTarget: () => staleOnce })
    primaryDatabase = primary.shop.database
    const replicaA = await coordinatedNode(
      'replica-a-row',
      holder,
      {},
      {
        hooks: {
          onBeforeSubscribe: () => {
            if (refuseOnA) throw new SirannonError('Replica A is draining', 'HOOK_DENIED')
          },
        },
      },
    )
    const replicaB = await coordinatedNode('replica-b-row', holder)
    const boxA = await middlebox(replicaA.shop.served.port)
    const boxB = await middlebox(replicaB.shop.served.port)
    const routeTo = (reader: string) =>
      healthyRouting(primary.base, [
        readable('primary', primary.base, 'local', 'majority', 'linearizable'),
        readable('reader', reader, 'local', 'majority'),
      ])
    holder.current = routeTo(boxA.base)
    const client = new TopologyAwareClient({
      endpoints: [primary.base],
      discovery: 'coordinator',
      readPreference: 'replica',
      transport: 'websocket',
    })
    const db = client.database(DATABASE_ID)
    const kept: ChangeEvent[] = []
    const dropped: ChangeEvent[] = []
    await db.on('orders').subscribe(event => {
      kept.push(event)
    })
    const ending = await db.on('orders').subscribe(event => {
      dropped.push(event)
    })
    await replicaA.shop.database.execute(INSERT_ORDER, ['a-before-move', 1])
    await waitUntil(() => kept.length === 1 && dropped.length === 1, 'the change on replica A')
    ending.unsubscribe()
    await replicaB.shop.database.execute(INSERT_ORDER, ['b-before-move', 2])

    holder.current = routeTo(boxB.base)
    staleWrites = 1
    const staleWrite = await db.execute(INSERT_ORDER, ['refused-write', 3]).then(
      () => 'resolved',
      (err: { code?: string }) => err.code,
    )
    await replicaB.shop.database.execute(INSERT_ORDER, ['b-after-move', 4])
    await replicaA.shop.database.execute(INSERT_ORDER, ['a-after-move', 5])
    await waitUntil(() => kept.length === 2, 'the change on replica B after the move')

    holder.current = routeTo(boxA.base)
    refuseOnA = true
    staleWrites = 1
    const failedMove = await db.execute(INSERT_ORDER, ['second-refused-write', 6]).then(
      () => ({ code: 'resolved', message: '' }),
      (err: { code?: string; message: string }) => ({ code: err.code, message: err.message }),
    )
    await replicaB.shop.database.execute(INSERT_ORDER, ['b-after-failed-move', 7])
    await waitUntil(() => kept.length === 3, 'the change on replica B after the failed move')
    const landed = await db.execute(INSERT_ORDER, ['landed-write', 8])
    await pause(100)
    client.close()

    expect(staleWrite).toBe('STALE_PRIMARY')
    expect(failedMove.code).toBe('ROUTING_ERROR')
    expect(failedMove.message).toContain('Could not re-establish active subscriptions')
    expect(customersSeen(kept)).toEqual(['a-before-move', 'b-after-move', 'b-after-failed-move'])
    expect(customersSeen(dropped)).toEqual(['a-before-move'])
    expect(boxB.messages('client', 'subscribe').map(frame => [frame.table, frame.sinceSeq])).toEqual([
      ['orders', undefined],
    ])
    expect(boxA.messages('client', 'subscribe')).toHaveLength(3)
    expect(boxA.messages('server', 'error').map(frame => (frame.error as { code: string }).code)).toEqual([
      'HOOK_DENIED',
    ])
    expect(landed.changes).toBe(1)
    expect(storedCustomers(primary.shop.path)).toEqual(['primary-row', 'landed-write'])
  })
})
