import { describe, expect, it } from 'vitest'
import type { SyncAck, SyncBatch, SyncComplete, SyncRequest } from '../../../replication/index.js'
import { specChecksum } from '../lib/device-sync-oracle.js'
import type { ManagedNode } from '../lib/factory.js'
import {
  errorsOf,
  eventually,
  indexNames,
  type Row,
  readRows,
  tableNames,
  tableRows,
} from '../lib/replication-scene.js'
import {
  converged,
  INSERT_ORDER,
  ORDERS_SCHEMA,
  order,
  PRIMARY,
  REPLICA,
  ready,
  SECOND_REPLICA,
  sceneForEachTest,
} from './harness.js'
import { dialAsPeer, listenAsPeer, type RawPeer, type SyncPlan, serveSync } from './peers.js'

const handle = sceneForEachTest()

const CUSTOMERS_SCHEMA = 'CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'
const SHIPMENTS_SCHEMA =
  'CREATE TABLE shipments (id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL REFERENCES customers(id), carrier TEXT)'
const NOTES_SCHEMA = 'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)'
const SHIPMENTS_INDEX = 'CREATE INDEX shipments_by_customer ON shipments (customer_id)'
const CUSTOMERS_INDEX = 'CREATE INDEX customers_by_name ON customers (name)'
const UNIQUE_CUSTOMERS_INDEX = 'CREATE UNIQUE INDEX customers_unique_name ON customers (name)'

interface FakeSourceScene {
  source: RawPeer
  replica: ManagedNode
}

function identityOf(nodeId: string) {
  const { certs } = handle
  return { nodeId, cert: certs.certForNode(nodeId), caCertPath: certs.caCertPath }
}

async function replicaOfFakeSource(
  config: Record<string, unknown> = {},
  extraEndpoints: number[] = [],
): Promise<FakeSourceScene> {
  const { scene } = handle
  const source = await listenAsPeer(scene, identityOf(PRIMARY), 'primary')
  const replica = await scene.replica(REPLICA, {
    port: source.port,
    config: {
      transportConfig: {
        localRole: 'replica',
        endpoints: [`localhost:${source.port}`, ...extraEndpoints.map(port => `localhost:${port}`)],
      },
      ...config,
    },
  })
  return { source, replica }
}

async function request(source: RawPeer, nth: number): Promise<SyncRequest> {
  await eventually(() => source.syncRequests.length >= nth, `sync request ${nth}`)
  const found = source.syncRequests[nth - 1]
  if (found === undefined) throw new Error(`No sync request ${nth}`)
  return found.message
}

function ordersPlan(rows: Row[], pageSize = rows.length || 1): SyncPlan {
  const batches: Row[][] = []
  for (let at = 0; at < rows.length; at += pageSize) batches.push(rows.slice(at, at + pageSize))
  return { schema: [ORDERS_SCHEMA], tables: [{ table: 'orders', batches }] }
}

function schemaBatch(requestId: string, schema: string[], totalTables = 1): SyncBatch {
  return {
    requestId,
    table: '__schema__',
    batchIndex: 0,
    rows: [],
    schema,
    checksum: specChecksum(schema),
    isLastBatchForTable: true,
    totalTables,
  }
}

function rowsBatch(requestId: string, rows: Row[], overrides: Partial<SyncBatch> = {}): SyncBatch {
  return {
    requestId,
    table: 'orders',
    batchIndex: 0,
    rows,
    checksum: specChecksum(rows),
    isLastBatchForTable: true,
    ...overrides,
  }
}

async function answer(source: RawPeer, joiner: string, batch: SyncBatch): Promise<SyncAck> {
  await source.transport.sendSyncBatch(joiner, batch)
  return source.syncAck(batch.requestId, batch.table, batch.batchIndex)
}

function phaseOf(node: ManagedNode): string | undefined {
  return node.engine.status().syncState?.phase
}

const CUSTOMERS = Array.from({ length: 23 }, (_, at) => ({ id: at + 1, name: `customer-${at + 1}` }))
const SHIPMENTS = Array.from({ length: 41 }, (_, at) => ({
  id: at + 1,
  customer_id: (at % CUSTOMERS.length) + 1,
  carrier: at % 3 === 0 ? null : `carrier-${at % 5}`,
}))

describe('the first copy that a joining replica takes from a real primary', () => {
  it('holds every table, index, and row of the primary, with foreign keys enforced and no change rows from the copy', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [CUSTOMERS_SCHEMA, SHIPMENTS_SCHEMA, NOTES_SCHEMA, SHIPMENTS_INDEX, CUSTOMERS_INDEX],
      watch: ['customers', 'shipments', 'notes'],
      config: { syncBatchSize: 7 },
    })
    await primary.engine.executeBatch(
      'INSERT INTO customers (id, name) VALUES (?, ?)',
      CUSTOMERS.map(row => [row.id, row.name]),
    )
    await primary.engine.executeBatch(
      'INSERT INTO shipments (id, customer_id, carrier) VALUES (?, ?, ?)',
      SHIPMENTS.map(row => [row.id, row.customer_id, row.carrier]),
    )

    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)

    expect(tableNames(replica.dbPath)).toEqual(['customers', 'notes', 'shipments'])
    expect(tableRows(replica.dbPath, 'customers')).toEqual(CUSTOMERS)
    expect(tableRows(replica.dbPath, 'shipments')).toEqual(SHIPMENTS)
    expect(tableRows(replica.dbPath, 'notes')).toEqual([])
    expect(indexNames(replica.dbPath, 'shipments')).toEqual(['shipments_by_customer'])
    expect(indexNames(replica.dbPath, 'customers')).toEqual(['customers_by_name'])
    expect(readRows(replica.dbPath, 'SELECT count(*) AS n FROM _sirannon_changes')).toEqual([{ n: 0 }])
    expect(await replica.conn.prepare('PRAGMA foreign_keys').then(statement => statement.get())).toEqual({
      foreign_keys: 1,
    })
    expect(replica.engine.status().syncState?.totalTables).toBe(3)

    await primary.engine.execute('INSERT INTO notes (id, body) VALUES (?, ?)', [1, 'written after the copy'])
    await converged(primary, [replica], 'notes', [{ id: 1, body: 'written after the copy' }])
  })

  it('carries every unique index of the primary through the first copy and the live stream', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [CUSTOMERS_SCHEMA, UNIQUE_CUSTOMERS_INDEX],
      watch: ['customers'],
    })
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)
    await primary.engine.execute('CREATE UNIQUE INDEX customers_unique_id_name ON customers (id, name)')
    await primary.engine.execute('INSERT INTO customers (id, name) VALUES (?, ?)', [1, 'amara'])

    await converged(primary, [replica], 'customers', [{ id: 1, name: 'amara' }])
    expect(indexNames(replica.dbPath, 'customers')).toEqual(indexNames(primary.dbPath, 'customers'))
  })

  it('holds only the schema of a primary with no rows, and counts its single table', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    const replica = await scene.replica(REPLICA, { port: primary.port })
    await ready(replica)

    expect(tableNames(replica.dbPath)).toEqual(['orders'])
    expect(tableRows(replica.dbPath, 'orders')).toEqual([])
    expect(replica.engine.status().syncState?.totalTables).toBe(1)
  })

  it('carries writes made on the primary while the copy streams', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
      config: { syncBatchSize: 5 },
    })
    const before = Array.from({ length: 40 }, (_, at) => order(at + 1, `before-${at}`, at))
    await primary.engine.executeBatch(
      INSERT_ORDER,
      before.map(row => [row.id, row.customer, row.total]),
    )
    const proxy = await scene.proxy(primary.port)
    proxy.delay('to-target', 40)
    const replica = await scene.replica(REPLICA, { port: proxy.port })
    await eventually(() => phaseOf(replica) === 'syncing', 'the copy to start')

    const during = Array.from({ length: 20 }, (_, at) => order(100 + at, `during-${at}`, at))
    for (const row of during) await primary.engine.execute(INSERT_ORDER, [row.id, row.customer, row.total])
    await primary.engine.execute('UPDATE orders SET total = -1 WHERE id = 1')
    await primary.engine.execute('DELETE FROM orders WHERE id = 2')
    expect(phaseOf(replica)).toBe('syncing')
    proxy.delay('to-target', 0)
    await ready(replica)

    const expected = [order(1, 'before-0', -1), ...before.slice(2), ...during]
    await converged(primary, [replica], 'orders', expected)
  })

  it('wipes a partial copy left by a restart and copies the primary afresh', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
      config: { syncBatchSize: 1 },
    })
    const rows = Array.from({ length: 80 }, (_, at) => order(at + 1, `copied-${at}`, at))
    await primary.engine.executeBatch(
      INSERT_ORDER,
      rows.map(row => [row.id, row.customer, row.total]),
    )
    const proxy = await scene.proxy(primary.port)
    proxy.delay('to-target', 50)
    const interrupted = await scene.replica(REPLICA, { port: proxy.port })
    await eventually(
      () => tableNames(interrupted.dbPath).includes('orders') && tableRows(interrupted.dbPath, 'orders').length >= 3,
      'the first copy to be part done',
    )
    const storage = await scene.stop(interrupted)
    const partial = tableRows(storage.dbPath, 'orders').length

    const restarted = await scene.replica(REPLICA, { port: primary.port, storage })
    await ready(restarted)

    expect(partial).toBeGreaterThan(0)
    expect(partial).toBeLessThan(rows.length)
    await converged(primary, [restarted], 'orders', rows)
  })

  it('is refused while the primary serves its limit of copies, and retries until a slot frees', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
      config: { maxConcurrentSyncs: 1, syncAckTimeoutMs: 60_000 },
    })
    await primary.engine.execute(INSERT_ORDER, [1, 'amara', 10])
    const holder = await dialAsPeer(scene, identityOf(SECOND_REPLICA), primary.port)
    await holder.connected(PRIMARY)
    await holder.transport.requestSync(PRIMARY, {
      requestId: 'copy-holding-the-only-slot',
      joinerNodeId: SECOND_REPLICA,
      completedTables: [],
      supportsStreamVerification: true,
    })
    await eventually(() => holder.syncBatches.length > 0, 'the holder to receive its schema batch')

    const replica = await scene.replica(REPLICA, { port: primary.port })
    await eventually(() => errorsOf(replica, 'sync-refused').length > 0, 'the replica to be refused')
    expect(errorsOf(replica, 'sync-refused')[0]?.error.message).toBe('Sync capacity reached, retry later')
    expect(phaseOf(replica)).toBe('pending')
    await holder.transport.disconnect()
    await ready(replica)

    await converged(primary, [replica], 'orders', [order(1, 'amara', 10)])
  })

  it('completes a copy that lasts longer than the source deadline while every batch is acknowledged within it', async () => {
    const { scene } = handle
    const deadlineMs = 500
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
      config: { maxSyncDurationMs: deadlineMs, syncBatchSize: 4 },
    })
    const rows = Array.from({ length: 60 }, (_, at) => order(at + 1, `slow-${at}`, at))
    await primary.engine.executeBatch(
      INSERT_ORDER,
      rows.map(row => [row.id, row.customer, row.total]),
    )
    const proxy = await scene.proxy(primary.port)
    proxy.delay('to-target', 60)
    const startedAt = Date.now()
    const replica = await scene.replica(REPLICA, { port: proxy.port })
    await ready(replica)

    expect(Date.now() - startedAt).toBeGreaterThan(deadlineMs * 2)
    await converged(primary, [replica], 'orders', rows)
  })

  it('recovers when the source abandons the copy because one acknowledgement outlasts its deadline', async () => {
    const { scene } = handle
    const deadlineMs = 300
    const primary = await scene.primary(PRIMARY, {
      schema: [ORDERS_SCHEMA],
      watch: ['orders'],
      config: { maxSyncDurationMs: deadlineMs, syncBatchSize: 1 },
    })
    const rows = Array.from({ length: 30 }, (_, at) => order(at + 1, `held-${at}`, at))
    await primary.engine.executeBatch(
      INSERT_ORDER,
      rows.map(row => [row.id, row.customer, row.total]),
    )
    const proxy = await scene.proxy(primary.port)
    const replica = await scene.replica(REPLICA, {
      port: proxy.port,
      config: { syncAckTimeoutMs: 2_000 },
    })
    await eventually(
      () => tableNames(replica.dbPath).includes('orders') && tableRows(replica.dbPath, 'orders').length >= 1,
      'the copy to start streaming rows',
    )
    proxy.delay('to-target', 60_000)
    const heldUntil = Date.now() + deadlineMs * 3
    await eventually(() => Date.now() >= heldUntil, 'the source deadline to pass with an acknowledgement held')
    proxy.delay('to-target', 0)
    await ready(replica)

    await converged(primary, [replica], 'orders', rows)
  })
})

describe('the first copy that a joining replica takes from a peer that holds the primary certificate', () => {
  it('refuses reads with SYNC_ERROR until the copy completes', async () => {
    const { source, replica } = await replicaOfFakeSource()
    const first = await request(source, 1)

    await expect(replica.engine.query('SELECT 1 AS one')).rejects.toMatchObject({
      code: 'SYNC_ERROR',
      message: "Node is in 'syncing' phase and cannot serve reads",
    })
    await serveSync(source, first, ordersPlan([order(1, 'amara', 10)]))
    await ready(replica)

    expect(await replica.engine.query('SELECT * FROM orders')).toEqual([order(1, 'amara', 10)])
  })

  it('asks again when its source sends nothing within the acknowledgement timeout', async () => {
    const { source, replica } = await replicaOfFakeSource({ syncAckTimeoutMs: 400 })
    const ignored = await request(source, 1)
    const second = await request(source, 2)

    expect(second.requestId).not.toBe(ignored.requestId)
    expect(errorsOf(replica, 'sync-response-timeout')[0]?.error.message).toBe(
      'The sync source sent no data within 400ms',
    )
    await serveSync(source, second, ordersPlan([order(1, 'amara', 10)]))
    await ready(replica)
    expect(tableRows(replica.dbPath, 'orders')).toEqual([order(1, 'amara', 10)])
  })

  it.each<[string, (complete: SyncComplete) => SyncComplete]>([
    [
      'a chained digest that differs from the batches streamed',
      complete => ({
        ...complete,
        manifests: complete.manifests.map(manifest => ({ ...manifest, batchDigest: 'f'.repeat(64) })),
      }),
    ],
    [
      'a row count that differs from the rows streamed',
      complete => ({
        ...complete,
        manifests: complete.manifests.map(manifest => ({ ...manifest, rowCount: manifest.rowCount + 1 })),
      }),
    ],
    [
      'neither a chained digest nor a key hash',
      complete => ({
        ...complete,
        manifests: complete.manifests.map(manifest => ({ table: manifest.table, rowCount: manifest.rowCount })),
      }),
    ],
    ['no manifest for a table it streamed', complete => ({ ...complete, manifests: [] })],
  ])('wipes the copy and asks again when the completion carries %s', async (_, tamper) => {
    const { source, replica } = await replicaOfFakeSource()
    const honest = Array.from({ length: 9 }, (_, at) => order(at + 1, `honest-${at}`, at))
    const first = await request(source, 1)
    await serveSync(source, first, { ...ordersPlan(honest, 4), tamper: { complete: tamper } })
    const second = await request(source, 2)

    expect(tableRows(replica.dbPath, 'orders')).toEqual([])
    expect(phaseOf(replica)).toBe('syncing')
    await serveSync(source, second, ordersPlan(honest, 4))
    await ready(replica)
    expect(tableRows(replica.dbPath, 'orders')).toEqual(honest)
    expect(source.syncRequests).toHaveLength(2)
  })

  it.each<[string, (requestId: string) => SyncBatch[], RegExp]>([
    [
      'a schema statement that creates a table from a query',
      requestId => [schemaBatch(requestId, [ORDERS_SCHEMA, 'CREATE TABLE stolen AS SELECT * FROM orders'])],
      /^Unsafe DDL in schema batch: CREATE TABLE stolen AS SELECT/,
    ],
    [
      'a schema statement that creates a trigger',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA, 'CREATE TRIGGER planted AFTER INSERT ON orders BEGIN SELECT 1; END']),
      ],
      /^Unsafe DDL in schema batch: CREATE TRIGGER/,
    ],
    [
      'a schema statement that reads a file from disk',
      requestId => [
        schemaBatch(requestId, [
          "CREATE TABLE planted (id INTEGER PRIMARY KEY, body TEXT DEFAULT (readfile('/etc/hosts')))",
        ]),
      ],
      /^Unsafe DDL in schema batch: CREATE TABLE planted/,
    ],
    [
      'a schema statement aimed at a reserved table',
      requestId => [schemaBatch(requestId, ['CREATE TABLE _sirannon_planted (id INTEGER PRIMARY KEY)'])],
      /^Unsafe DDL in schema batch: CREATE TABLE _sirannon_planted/,
    ],
    [
      'rows aimed at a reserved table',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA]),
        rowsBatch(requestId, [{ peer_node_id: 'forged' }], { table: '_sirannon_peer_state' }),
      ],
      /^Invalid table name in sync batch: _sirannon_peer_state$/,
    ],
    [
      'rows aimed at a table name carrying SQL',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA]),
        rowsBatch(requestId, [order(1, 'amara', 10)], { table: 'orders"; DROP TABLE orders; --' }),
      ],
      /^Invalid table name in sync batch: orders"; DROP TABLE orders; --$/,
    ],
    [
      'rows whose checksum does not match the spec canonical form',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA]),
        rowsBatch(requestId, [order(1, 'amara', 10)], { checksum: specChecksum([order(1, 'amara', 11)]) }),
      ],
      /^Checksum mismatch for orders batch 0$/,
    ],
    [
      'rows with a column name carrying SQL',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA]),
        rowsBatch(requestId, [{ id: 1, customer: 'amara', 'total) VALUES (1); --': 10 }]),
      ],
      /^Invalid column name 'total\) VALUES \(1\); --' in sync batch for orders$/,
    ],
    [
      'rows whose columns differ from the first row',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA]),
        rowsBatch(requestId, [order(1, 'amara', 10), { id: 2, customer: 'kofi' }]),
      ],
      /^Row columns do not match batch columns for orders$/,
    ],
    [
      'rows with no columns at all',
      requestId => [schemaBatch(requestId, [ORDERS_SCHEMA]), rowsBatch(requestId, [{}, {}])],
      /^No columns in sync batch for orders$/,
    ],
    [
      'a batch index that skips ahead',
      requestId => [
        schemaBatch(requestId, [ORDERS_SCHEMA]),
        rowsBatch(requestId, [order(2, 'kofi', 20)], { batchIndex: 1 }),
      ],
      /^Out of order batch: expected 0, got 1$/,
    ],
  ])('refuses %s and writes none of its rows', async (_, batchesFor, refusal) => {
    const { source, replica } = await replicaOfFakeSource()
    const { requestId, joinerNodeId } = await request(source, 1)
    const batches = batchesFor(requestId)
    const acks: SyncAck[] = []
    for (const batch of batches) acks.push(await answer(source, joinerNodeId, batch))

    const last = acks.at(-1)
    expect(acks.slice(0, -1).every(ack => ack.success)).toBe(true)
    expect(last?.success).toBe(false)
    expect(last?.error).toMatch(refusal)
    expect(phaseOf(replica)).toBe('syncing')
    if (tableNames(replica.dbPath).includes('orders')) expect(tableRows(replica.dbPath, 'orders')).toEqual([])
    expect(
      readRows(
        replica.dbPath,
        "SELECT name FROM sqlite_master WHERE name IN ('stolen', 'planted', '_sirannon_planted') OR type = 'trigger'",
      ),
    ).toEqual([])
  })

  it('asks again once its source goes quiet after it refuses a corrupt batch', async () => {
    const { source, replica } = await replicaOfFakeSource({ syncAckTimeoutMs: 400 })
    const rows = [order(1, 'amara', 10), order(2, 'kofi', 20)]
    const first = await request(source, 1)
    await answer(source, REPLICA, schemaBatch(first.requestId, [ORDERS_SCHEMA]))
    const refused = await answer(
      source,
      REPLICA,
      rowsBatch(first.requestId, rows, { checksum: specChecksum([order(1, 'amara', 11)]) }),
    )
    const second = await request(source, 2)

    expect(refused.success).toBe(false)
    expect(errorsOf(replica, 'sync-response-timeout')[0]?.error.message).toBe(
      'The sync source sent nothing within 400ms of the last acknowledgement',
    )
    await serveSync(source, second, ordersPlan(rows))
    await ready(replica)
    expect(tableRows(replica.dbPath, 'orders')).toEqual(rows)
  })

  it.each<[string, number, number]>([
    ['a negative table count', 0, -1],
    ['a valid table count', 1, 1],
  ])('reports %s from the schema batch as %d tables', async (_, reported, totalTables) => {
    const { source, replica } = await replicaOfFakeSource()
    const first = await request(source, 1)
    await serveSync(source, first, { ...ordersPlan([order(1, 'amara', 10)]), totalTables })
    await ready(replica)

    expect(replica.engine.status().syncState?.totalTables).toBe(reported)
    expect(tableRows(replica.dbPath, 'orders')).toEqual([order(1, 'amara', 10)])
  })

  it('ignores a refusal sent by a peer other than its source, or naming a request other than the outstanding one', async () => {
    const { scene } = handle
    const bystander = await listenAsPeer(scene, identityOf(SECOND_REPLICA), 'replica')
    const { source, replica } = await replicaOfFakeSource({}, [bystander.port])
    await bystander.connected(REPLICA)
    const first = await request(source, 1)
    const refusal = {
      joinerNodeId: REPLICA,
      table: '__schema__',
      batchIndex: -1,
      success: false,
      error: 'Sync capacity reached, retry later',
    }

    await bystander.transport.sendSyncAck(REPLICA, { ...refusal, requestId: first.requestId })
    await source.transport.sendSyncAck(REPLICA, { ...refusal, requestId: 'another-request' })
    await eventually(
      () => errorsOf(replica, 'sync-ack-identity-mismatch').length >= 2,
      'the replica to set both refusals aside',
    )
    await serveSync(source, first, ordersPlan([order(1, 'amara', 10)]))
    await ready(replica)

    expect(errorsOf(replica, 'sync-refused')).toEqual([])
    expect(source.syncRequests).toHaveLength(1)
    expect(tableRows(replica.dbPath, 'orders')).toEqual([order(1, 'amara', 10)])
  })
})
