import { afterEach, describe, expect, it } from 'vitest'
import { TopologyAwareClient } from '../../../client/topology.js'
import { operationRef } from '../../../core/operation-registry.js'
import type { OperationRegistry } from '../../../core/types.js'
import { DATABASE_ID, droppingListener, INSERT_ORDER, pause, releaseClientServers, storedCustomers } from './harness.js'
import {
  clusterNode,
  coordinatedNode,
  dataRequests,
  healthyRouting,
  readable,
  routingHolder,
} from './topology-cluster.js'

const SET_ASIDE_MS = 5_000
const customers = operationRef<Record<string, never>, { customer: string }>('customers')
const OPERATIONS: OperationRegistry = {
  [DATABASE_ID]: { reads: { customers: { statement: () => ({ sql: 'SELECT customer FROM orders ORDER BY id' }) } } },
}

afterEach(releaseClientServers)

function codeOf(promise: Promise<unknown>): Promise<string> {
  return promise.then(
    () => 'resolved',
    (err: { code?: string }) => err.code ?? 'none',
  )
}

async function answeringNode(client: TopologyAwareClient): Promise<string> {
  const rows = await client.database(DATABASE_ID).query<{ customer: string }>('SELECT customer FROM orders LIMIT 1')
  return rows[0]?.customer ?? 'none'
}

async function readsFrom(client: TopologyAwareClient, count: number): Promise<Set<string>> {
  const answered = new Set<string>()
  for (let index = 0; index < count; index++) answered.add(await answeringNode(client))
  return answered
}

describe('a topology client routing between nodes it is given', () => {
  it('sends every write to the primary and each read where its preference points', async () => {
    const primary = await clusterNode('primary-row')
    const replicaA = await clusterNode('replica-a-row')
    const replicaB = await clusterNode('replica-b-row')
    const silent = await droppingListener()
    const toReplicas = new TopologyAwareClient({
      primary: primary.base,
      replicas: [replicaA.base, replicaB.base],
      readPreference: 'replica',
      transport: 'http',
    })
    const toPrimary = new TopologyAwareClient({ primary: primary.base, replicas: [replicaA.base], transport: 'http' })
    const nearest = new TopologyAwareClient({
      primary: primary.base,
      replicas: [silent.base, replicaA.base],
      readPreference: 'nearest',
      transport: 'http',
    })
    const db = toReplicas.database(DATABASE_ID)

    await db.execute(INSERT_ORDER, ['written-once', 1])
    await db.transaction([{ sql: INSERT_ORDER, params: ['written-in-transaction', 2] }])
    await db.batch(INSERT_ORDER, [['written-in-batch', 3]])
    await db.load(INSERT_ORDER, [['written-in-load', 4]])
    const replicaAnswers = await readsFrom(toReplicas, 30)
    const primaryAnswers = await readsFrom(toPrimary, 5)
    const nearestAnswers = await readsFrom(nearest, 10)
    const perCallConcern = await codeOf(db.query('SELECT 1', [], { readConcern: { level: 'majority' } }))
    for (const client of [toReplicas, toPrimary, nearest]) client.close()

    expect(storedCustomers(primary.shop.path)).toEqual([
      'primary-row',
      'written-once',
      'written-in-transaction',
      'written-in-batch',
      'written-in-load',
    ])
    expect(storedCustomers(replicaA.shop.path)).toEqual(['replica-a-row'])
    expect(storedCustomers(replicaB.shop.path)).toEqual(['replica-b-row'])
    expect([...replicaAnswers].sort()).toEqual(['replica-a-row', 'replica-b-row'])
    expect([...primaryAnswers]).toEqual(['primary-row'])
    expect(nearestAnswers.size).toBe(1)
    expect(['primary-row', 'replica-a-row']).toEqual(expect.arrayContaining([...nearestAnswers]))
    expect(silent.attempts).toBe(1)
    expect(perCallConcern).toBe('INVALID_ARGUMENT')
    expect(dataRequests(primary, 'query')).toBe(5 + (nearestAnswers.has('primary-row') ? 10 : 0))
    expect(
      [replicaA, replicaB].flatMap(node =>
        (['execute', 'transaction', 'batch', 'load'] as const).map(route => dataRequests(node, route)),
      ),
    ).toEqual(Array(8).fill(0))
  })

  it('sets an unreachable replica aside for five seconds, answers from the primary, then tries the replica again', async () => {
    const primary = await clusterNode('primary-row', { operations: OPERATIONS })
    const silent = await droppingListener()
    const client = new TopologyAwareClient({
      primary: primary.base,
      replicas: [silent.base],
      readPreference: 'replica',
      transport: 'http',
    })
    const unreachablePrimary = new TopologyAwareClient({ primary: silent.base, transport: 'http' })
    const db = client.database(DATABASE_ID)

    const first = await db.query<{ customer: string }>('SELECT customer FROM orders')
    const attemptsAfterFirst = silent.attempts
    const setAsideStarted = Date.now()
    const during = [await db.query(customers, {}), await db.query('SELECT customer FROM orders')]
    const attemptsDuring = silent.attempts
    await pause(SET_ASIDE_MS + 200 - (Date.now() - setAsideStarted))
    const after = await db.query(customers, {})
    const attemptsAfter = silent.attempts
    const primaryDown = await codeOf(unreachablePrimary.database(DATABASE_ID).query('SELECT 1'))
    client.close()
    unreachablePrimary.close()

    expect([first, ...during, after]).toEqual(Array(4).fill([{ customer: 'primary-row' }]))
    expect([attemptsAfterFirst, attemptsDuring, attemptsAfter]).toEqual([1, 1, 2])
    expect(primaryDown).toBe('CONNECTION_ERROR')
    expect(silent.attempts).toBe(3)
  }, 20_000)

  it('refuses a configured address that is not a plain http or https endpoint before it sends anything', async () => {
    const node = await clusterNode('primary-row')
    const unsafe = [
      'file:///var/data/shop.db',
      'javascript:alert(1)',
      'ftp://127.0.0.1/shop',
      `http://clerk:secret@127.0.0.1:${node.shop.served.port}`,
      `${node.base}/?tenant=north`,
      `${node.base}/#top`,
      'not a url',
    ]

    for (const address of unsafe) {
      for (const options of [
        { primary: address },
        { primary: node.base, replicas: [node.base, address] },
        { endpoints: [node.base, address], discovery: 'coordinator' as const },
      ]) {
        let thrown: unknown
        try {
          new TopologyAwareClient({ ...options, transport: 'http' }).close()
        } catch (err) {
          thrown = err
        }
        expect(thrown, `${JSON.stringify(options)}`).toBeInstanceOf(Error)
      }
    }

    expect(node.tally.requests).toEqual([])
  })
})

describe('a topology client discovering its group from a coordinator', () => {
  it('routes writes to the current primary and each read to a node that serves its read concern', async () => {
    const holder = routingHolder()
    const discoveries: string[] = []
    const primary = await coordinatedNode('primary-row', holder)
    const replicaA = await coordinatedNode('replica-a-row', holder, {
      authorizeClusterStatus: ctx => {
        discoveries.push(String(ctx.headers.authorization))
        return true
      },
    })
    const replicaB = await coordinatedNode('replica-b-row', holder)
    holder.current = healthyRouting(primary.base, [
      readable('primary', primary.base, 'local', 'majority', 'linearizable'),
      readable('a', `${replicaA.base}/db/${DATABASE_ID}`, 'local', 'majority'),
      readable('b', replicaB.base, 'local'),
    ])
    const build = (
      readConcern: 'local' | 'majority' | 'linearizable',
      readPreference: 'replica' | 'nearest' = 'replica',
    ) =>
      new TopologyAwareClient({
        endpoints: [replicaA.base],
        discovery: 'coordinator',
        readPreference,
        readConcern,
        transport: 'http',
        headers: { authorization: 'Bearer operator' },
      })
    const majority = build('majority')
    const local = build('local')
    const linearizable = build('linearizable')
    const nearest = build('majority', 'nearest')

    await majority.database(DATABASE_ID).execute(INSERT_ORDER, ['routed-write', 1])
    const answers = {
      majority: await readsFrom(majority, 15),
      local: await readsFrom(local, 30),
      linearizable: await readsFrom(linearizable, 5),
      nearest: await readsFrom(nearest, 5),
    }
    const perCallConcern = await codeOf(
      majority.database(DATABASE_ID).query('SELECT 1', [], { readConcern: { level: 'local' } }),
    )
    for (const client of [majority, local, linearizable, nearest]) client.close()

    expect(answers.majority).toEqual(new Set(['replica-a-row']))
    expect([...answers.local].sort()).toEqual(['replica-a-row', 'replica-b-row'])
    expect(answers.linearizable).toEqual(new Set(['primary-row']))
    expect(answers.nearest).toEqual(new Set(['primary-row']))
    expect(perCallConcern).toBe('INVALID_ARGUMENT')
    expect(storedCustomers(primary.shop.path)).toEqual(['primary-row', 'routed-write'])
    expect([dataRequests(replicaA, 'execute'), dataRequests(replicaB, 'execute')]).toEqual([0, 0])
    expect(discoveries).toEqual(Array(4).fill('Bearer operator'))
  })

  it('sets an unreachable read endpoint aside and answers statement and registered reads from the refreshed route', async () => {
    const holder = routingHolder()
    const silent = await droppingListener()
    const primary = await coordinatedNode('primary-row', holder, { operations: OPERATIONS })
    holder.current = healthyRouting(primary.base, [
      readable('primary', primary.base, 'local', 'majority', 'linearizable'),
      readable('silent', silent.base, 'local', 'majority'),
    ])
    const build = () =>
      new TopologyAwareClient({
        endpoints: [primary.base],
        discovery: 'coordinator',
        readPreference: 'replica',
        transport: 'http',
      })
    const statements = build()
    const named = build()

    const statementAnswers = [await answeringNode(statements), await answeringNode(statements)]
    const attemptsAfterStatements = silent.attempts
    const namedAnswers = [
      await named.database(DATABASE_ID).query(customers, {}),
      await named.database(DATABASE_ID).query(customers, {}),
    ]
    const attemptsAfterNamed = silent.attempts
    await pause(SET_ASIDE_MS + 200)
    const retried = await answeringNode(statements)
    statements.close()
    named.close()

    expect(statementAnswers).toEqual(['primary-row', 'primary-row'])
    expect(namedAnswers).toEqual([[{ customer: 'primary-row' }], [{ customer: 'primary-row' }]])
    expect(retried).toBe('primary-row')
    expect([attemptsAfterStatements, attemptsAfterNamed, silent.attempts]).toEqual([1, 2, 3])
    expect(primary.tally.requests.filter(ctx => ctx.path.endsWith('/cluster')).length).toBe(5)
  }, 20_000)

  it('raises NO_SAFE_PRIMARY when the group names no primary and ROUTING_ERROR when no starter serves routing', async () => {
    const holder = routingHolder()
    const replica = await coordinatedNode('replica-row', holder)
    const guarded = await clusterNode('guarded-row', {
      getClusterStatus: holder.getClusterStatus,
      authorizeClusterStatus: ctx => ctx.headers.authorization === 'Bearer operator',
    })
    const silent = await droppingListener()
    holder.current = healthyRouting(null, [readable('replica', replica.base, 'local', 'majority')])
    const leaderless = new TopologyAwareClient({
      endpoints: [replica.base],
      discovery: 'coordinator',
      transport: 'http',
    })
    const applicationOnly = new TopologyAwareClient({
      endpoints: [silent.base, guarded.base],
      discovery: 'coordinator',
      transport: 'http',
      headers: { authorization: 'Bearer application' },
    })

    const outcomes = {
      write: await codeOf(leaderless.database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1])),
      batch: await codeOf(leaderless.database(DATABASE_ID).batch(INSERT_ORDER, [['lost', 1]])),
      majorityRead: await answeringNode(leaderless),
      routingRead: await codeOf(applicationOnly.database(DATABASE_ID).query('SELECT 1')),
      routingWrite: await codeOf(applicationOnly.database(DATABASE_ID).execute(INSERT_ORDER, ['lost', 1])),
    }
    for (const client of [leaderless, applicationOnly]) client.close()

    expect(outcomes).toEqual({
      write: 'NO_SAFE_PRIMARY',
      batch: 'NO_SAFE_PRIMARY',
      majorityRead: 'replica-row',
      routingRead: 'ROUTING_ERROR',
      routingWrite: 'ROUTING_ERROR',
    })
    expect(storedCustomers(replica.shop.path)).toEqual(['replica-row'])
    expect(storedCustomers(guarded.shop.path)).toEqual(['guarded-row'])
    expect(
      [replica, guarded]
        .flatMap(node => node.tally.requests.filter(ctx => !ctx.path.endsWith('/cluster')))
        .map(ctx => ctx.path),
    ).toEqual([`/db/${DATABASE_ID}/query`])
    expect(silent.attempts).toBeGreaterThanOrEqual(2)
  })
})
