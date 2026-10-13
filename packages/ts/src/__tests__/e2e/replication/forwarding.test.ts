import { describe, expect, it } from 'vitest'
import { StalePrimaryError } from '../../../replication/index.js'
import { caughtUp, eventually, readRows, tableRows } from '../lib/replication-scene.js'
import {
  converged,
  INSERT_ORDER,
  INTRUDER,
  ORDERS_SCHEMA,
  order,
  PRIMARY,
  REPLICA,
  ready,
  SECOND_REPLICA,
  sceneForEachTest,
} from './harness.js'
import { dialAsPeer, forwardWithoutJoining } from './peers.js'

const handle = sceneForEachTest()

const LEDGER_SCHEMA =
  'CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT, amount INTEGER, payload BLOB)'
const PAYROLL_REFUSAL = 'payroll is closed to forwarded writes'

async function forwardingPair(options: { forwarding?: boolean } = {}) {
  const { scene } = handle
  const primary = await scene.primary(PRIMARY, {
    schema: [ORDERS_SCHEMA, LEDGER_SCHEMA],
    watch: ['orders', 'ledger'],
    config: {
      onBeforeForwardedQuery: sql => {
        if (/\bpayroll\b/i.test(sql)) throw new StalePrimaryError(PAYROLL_REFUSAL)
      },
    },
  })
  const replica = await scene.replica(REPLICA, {
    port: primary.port,
    config: { writeForwarding: options.forwarding ?? true },
  })
  await ready(replica)
  return { primary, replica }
}

function countOrders(path: string): number {
  return Number(readRows(path, 'SELECT count(*) AS n FROM orders')[0]?.n)
}

describe('writes sent to a replica', () => {
  it('run on the primary, return its own results, and come back to the replica', async () => {
    const { primary, replica } = await forwardingPair()

    const single = await replica.engine.execute('INSERT INTO ledger (label, amount, payload) VALUES (?, ?, ?)', [
      'first',
      9007199254740993n,
      Buffer.from([0, 255]),
    ])
    expect(tableRows(primary.dbPath, 'ledger')).toEqual([
      { id: 1, label: 'first', amount: 9007199254740993n, payload: Buffer.from([0, 255]) },
    ])
    const named = await replica.engine.execute(
      'INSERT INTO ledger (label, amount, payload) VALUES (:label, :amount, :payload)',
      { label: 'named', amount: -1, payload: null },
    )
    const batch = await replica.engine.executeBatch(INSERT_ORDER, [
      [1, 'amara', 10],
      [2, 'kofi', 20],
    ])
    const listed = await replica.engine.executeTransaction([
      { sql: 'UPDATE orders SET total = total * 2' },
      { sql: 'INSERT INTO ledger (label) VALUES (?)', params: ['listed'] },
    ])
    const statements = await replica.engine.forwardStatements([
      { sql: 'DELETE FROM orders WHERE id = ?', params: [1] },
      { sql: "INSERT INTO ledger (label) VALUES ('statements')" },
    ])

    expect(single).toMatchObject({ changes: 1, lastInsertRowId: 1 })
    expect(named).toMatchObject({ changes: 1, lastInsertRowId: 2 })
    expect(batch.map(result => result.changes)).toEqual([1, 1])
    expect(listed.map(result => result.changes)).toEqual([2, 1])
    expect(Number(listed[1]?.lastInsertRowId)).toBe(3)
    expect(statements.results.map(result => result.changes)).toEqual([1, 1])
    expect(Number(statements.results[1]?.lastInsertRowId)).toBe(4)
    await converged(primary, [replica], 'orders', [order(2, 'kofi', 40)])
    await converged(primary, [replica], 'ledger', [
      { id: 1, label: 'first', amount: 9007199254740993n, payload: Buffer.from([0, 255]) },
      { id: 2, label: 'named', amount: -1, payload: null },
      { id: 3, label: 'listed', amount: null, payload: null },
      { id: 4, label: 'statements', amount: null, payload: null },
    ])
  })

  it('commit nothing on either node when any statement of a forwarded group fails', async () => {
    const { primary, replica } = await forwardingPair()
    await replica.engine.execute(INSERT_ORDER, [1, 'amara', 10])

    const outcomes = await Promise.all(
      [
        replica.engine.executeBatch(INSERT_ORDER, [
          [2, 'kofi', 20],
          [3, 'esi', 30],
          [1, 'duplicate', 0],
        ]),
        replica.engine.executeTransaction([
          { sql: 'UPDATE orders SET total = 0' },
          { sql: 'INSERT INTO orders (id, customer) VALUES (4, ?)', params: ['no total'] },
        ]),
        replica.engine.forwardStatements([
          { sql: 'DELETE FROM orders' },
          { sql: 'INSERT INTO missing_table VALUES (1)' },
        ]),
      ].map(promise => promise.catch((error: unknown) => error)),
    )

    for (const outcome of outcomes) expect(outcome).toMatchObject({ code: 'QUERY_ERROR' })
    await replica.engine.execute(INSERT_ORDER, [5, 'after', 50])
    await converged(primary, [replica], 'orders', [order(1, 'amara', 10), order(5, 'after', 50)])
  })

  it('fail with the code that the primary raised, and the primary writes nothing', async () => {
    const { primary, replica } = await forwardingPair()

    const constraint = await replica.engine.execute(INSERT_ORDER, [1, null, 1]).catch((error: unknown) => error)
    const reserved = await replica.engine.execute('DELETE FROM _sirannon_changes').catch((error: unknown) => error)
    const reservedBatch = await replica.engine
      .executeBatch('DELETE FROM _sirannon_peer_state WHERE peer_node_id = ?', [['x']])
      .catch((error: unknown) => error)
    const pragma = await replica.engine.execute('PRAGMA user_version = 7').catch((error: unknown) => error)
    const attach = await replica.engine.execute("ATTACH DATABASE ':memory:' AS spare").catch((error: unknown) => error)
    const hooked = await replica.engine
      .execute('INSERT INTO orders (id, customer, total) SELECT 9, name, 0 FROM payroll')
      .catch((error: unknown) => error)

    expect(constraint).toMatchObject({ code: 'QUERY_ERROR' })
    expect(reserved).toMatchObject({ code: 'FORBIDDEN_SQL' })
    expect(reservedBatch).toMatchObject({ code: 'FORBIDDEN_SQL' })
    expect(pragma).toMatchObject({ code: 'REPLICATION_ERROR' })
    expect(attach).toMatchObject({ code: 'REPLICATION_ERROR' })
    expect(hooked).toMatchObject({ code: 'STALE_PRIMARY', message: PAYROLL_REFUSAL })
    expect(countOrders(primary.dbPath)).toBe(0)
    expect(readRows(primary.dbPath, 'PRAGMA user_version')).toEqual([{ user_version: 0 }])
    expect(readRows(primary.dbPath, 'PRAGMA database_list').map(row => row.name)).toEqual(['main'])
    expect(countOrders(replica.dbPath)).toBe(0)
  })

  it('wait on the primary for the write concern that the caller stated, and fail with WRITE_CONCERN_ERROR when it goes unmet', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const forwarder = await scene.replica(REPLICA, { port: primary.port })
    const proxy = await scene.proxy(primary.port)
    const lagging = await scene.replica(SECOND_REPLICA, { port: proxy.port })
    await ready(forwarder, lagging)
    await eventually(() => primary.engine.status().peers.filter(peer => peer.connected).length === 2, 'two replicas')

    proxy.stall()
    const unmet = await forwarder.engine
      .execute(INSERT_ORDER, [1, 'amara', 10], { writeConcern: { level: 'all', timeoutMs: 300 } })
      .catch((error: unknown) => error)
    const unstated = await forwarder.engine.execute(INSERT_ORDER, [2, 'kofi', 20])

    expect(unmet).toMatchObject({ code: 'WRITE_CONCERN_ERROR' })
    expect(unstated.changes).toBe(1)
    expect(tableRows(lagging.dbPath, 'orders')).toEqual([])

    proxy.resume()
    await caughtUp(lagging, primary)
    await forwarder.engine.execute(INSERT_ORDER, [3, 'esi', 30], { writeConcern: { level: 'all', timeoutMs: 5_000 } })
    expect(tableRows(lagging.dbPath, 'orders')).toEqual([
      order(1, 'amara', 10),
      order(2, 'kofi', 20),
      order(3, 'esi', 30),
    ])
    expect(tableRows(forwarder.dbPath, 'orders')).toEqual(tableRows(lagging.dbPath, 'orders'))
  })

  it('are refused with TOPOLOGY_ERROR when forwarding is off, when they come as a callback transaction, or when no primary is connected', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const proxy = await scene.proxy(primary.port)
    const forwarding = await scene.replica(REPLICA, { port: proxy.port, config: { writeForwarding: true } })
    const readOnly = await scene.replica(SECOND_REPLICA, { port: primary.port, config: { writeForwarding: false } })
    await ready(forwarding, readOnly)

    const refused = await readOnly.engine.execute(INSERT_ORDER, [1, 'amara', 10]).catch((error: unknown) => error)
    const refusedBatch = await readOnly.engine
      .executeTransaction([{ sql: 'DELETE FROM orders' }])
      .catch((error: unknown) => error)
    const callback = await forwarding.engine
      .transaction(async tx => {
        await tx.execute(INSERT_ORDER, [2, 'kofi', 20])
      })
      .catch((error: unknown) => error)
    proxy.refuse(true)
    proxy.cut()
    await eventually(() => forwarding.engine.status().peers.every(peer => !peer.connected), 'the primary to drop')
    const orphaned = await forwarding.engine.execute(INSERT_ORDER, [3, 'esi', 30]).catch((error: unknown) => error)

    for (const outcome of [refused, refusedBatch, callback, orphaned]) {
      expect(outcome).toMatchObject({ code: 'TOPOLOGY_ERROR' })
    }
    expect(countOrders(primary.dbPath)).toBe(0)

    proxy.refuse(false)
    await eventually(
      () =>
        primary.engine.status().peers.filter(peer => peer.connected).length === 2 &&
        forwarding.engine.status().peers.some(peer => peer.nodeId === PRIMARY && peer.connected),
      'the redial',
    )
    await forwarding.engine.execute(INSERT_ORDER, [4, 'back', 40])
    await converged(primary, [forwarding, readOnly], 'orders', [order(4, 'back', 40)])
  })

  it('are refused from a node whose certificate the primary trusts but which never opened a replication stream', async () => {
    const { certs, scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const identity = { nodeId: INTRUDER, cert: certs.certForNode(INTRUDER), caCertPath: certs.caCertPath }

    const stranger = await forwardWithoutJoining(primary.port, identity, "INSERT INTO orders VALUES (1, 'stranger', 1)")
    const joined = await dialAsPeer(scene, identity, primary.port)
    await joined.connected(PRIMARY)
    const member = await forwardWithoutJoining(primary.port, identity, "INSERT INTO orders VALUES (2, 'member', 1)")

    expect(stranger.error?.code).toBe(16)
    expect(stranger.response).toBeUndefined()
    expect(member.error).toBeNull()
    expect(tableRows(primary.dbPath, 'orders')).toEqual([order(2, 'member', 1)])
  })
})
