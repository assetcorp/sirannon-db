import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import type { Sirannon } from '../../../core/sirannon.js'
import { createServer, type SirannonServer } from '../../../server/server.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import { type FaultProxy, startFaultProxy } from '../lib/fault-proxy.js'
import {
  createMtlsCerts,
  createPrimary,
  createReplica,
  type ManagedNode,
  type MtlsCerts,
  stopNode,
  waitForReady,
  waitForReplica,
} from '../lib/index.js'
import { postJson, releaseServedRegistries, trackedRegistry, waitUntil } from '../lib/served-registry.js'
import {
  billedAs,
  countingDriver,
  INSERT_ORDER,
  Meter,
  ORDERS_SCHEMA,
  rowInserts,
  rowsIn,
  SCAN_BY_CUSTOMER,
  SIRANNON_SQLITE_BINDING,
  scansWholeTable,
  wholeTableScan,
} from './metered-registry.js'

const PRIMARY_ID = 'primary-faults-aaaa'
const REPLICA_ID = 'replica-faults-bbbb'
const DATABASE_ID = 'orders'
const SEEDED_ORDERS = 120
const WRITES_WHILE_CUT = 5
const HELD_REPLY_MS = 60_000
const REPLICATION_DEADLINE_MS = 10_000
const READY_DEADLINE_MS = 20_000

interface MeteredNode {
  node: ManagedNode
  sirannon: Sirannon
  server: SirannonServer
  base: string
  meter: Meter
}

interface FaultedPair {
  primary: MeteredNode
  replica: MeteredNode
  link: FaultProxy
}

let certs: MtlsCerts
const running: MeteredNode[] = []
const links: FaultProxy[] = []

async function seedOrders(conn: SQLiteConnection): Promise<void> {
  for (const statement of ORDERS_SCHEMA) await conn.exec(statement)
  const insert = await conn.prepare(INSERT_ORDER)
  for (let index = 0; index < SEEDED_ORDERS; index++) await insert.run(`customer-${index}`, index % 10)
}

async function serve(node: ManagedNode, sirannon: Sirannon, meter: Meter): Promise<MeteredNode> {
  const server = createServer(sirannon, {
    host: '127.0.0.1',
    port: 0,
    acceptSql: true,
    resolveExecutionTarget: id => (id === DATABASE_ID ? node.engine : null),
  })
  await server.listen()
  const served = { node, sirannon, server, base: `http://127.0.0.1:${server.listeningPort}`, meter }
  running.push(served)
  return served
}

async function startFaultedPair(): Promise<FaultedPair> {
  const primaryMeter = new Meter()
  const primaryRegistry = trackedRegistry({
    driver: countingDriver(),
    metrics: { onQueryComplete: primaryMeter.record },
  })
  const primaryNode = await createPrimary({
    nodeId: PRIMARY_ID,
    certs,
    registry: { sirannon: primaryRegistry, driver: countingDriver(), databaseId: DATABASE_ID },
    initialize: async (conn, tracker) => {
      await seedOrders(conn)
      await tracker.watch(conn, 'orders')
    },
  })
  const primary = await serve(primaryNode, primaryRegistry, primaryMeter)
  const link = await startFaultProxy(primaryNode.port)
  links.push(link)

  const replicaMeter = new Meter()
  const replicaRegistry = trackedRegistry({
    driver: countingDriver(),
    metrics: { onQueryComplete: replicaMeter.record },
  })
  const replicaNode = await createReplica({
    nodeId: REPLICA_ID,
    certs,
    primaryHost: 'localhost',
    primaryPort: link.port,
    registry: { sirannon: replicaRegistry, driver: countingDriver(), databaseId: DATABASE_ID },
    configOverrides: { writeForwarding: true },
  })
  const replica = await serve(replicaNode, replicaRegistry, replicaMeter)
  await waitForReady(replicaNode.engine, READY_DEADLINE_MS)
  primaryMeter.clear()
  replicaMeter.clear()
  return { primary, replica, link }
}

async function caughtUp({ primary, replica }: FaultedPair): Promise<void> {
  await waitForReplica(
    replica.node.engine,
    primary.node.nodeId,
    primary.node.engine.getCurrentSeq(),
    REPLICATION_DEADLINE_MS,
  )
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering replication through a faulty link', () => {
  beforeAll(async () => {
    certs = await createMtlsCerts([PRIMARY_ID, REPLICA_ID])
  })

  afterAll(() => {
    certs?.cleanup()
  })

  afterEach(async () => {
    for (const link of links.splice(0)) await link.close().catch(() => {})
    for (const target of running.splice(0).reverse()) {
      await target.server.close().catch(() => {})
      await stopNode(target.node)
      await target.sirannon.shutdown().catch(() => {})
    }
    await releaseServedRegistries()
  })

  it('bills a forwarded write once on the primary when the link drops before its answer returns, and once per write after the replica reconnects', async () => {
    const pair = await startFaultedPair()
    const { primary, replica, link } = pair
    const inserted = await rowInserts(primary.node.dbPath, 'orders')

    link.delay('to-client', HELD_REPLY_MS)
    const lost = postJson(replica.base, `/db/${DATABASE_ID}/execute`, { sql: INSERT_ORDER, params: ['lost-reply', 1] })
    await waitUntil(() => primary.meter.reports.length === 1, 'the primary running the forwarded write')
    link.cut()
    const lostAnswer = await lost
    link.delay('to-client', 0)
    await caughtUp(pair)
    const resent = await postJson(replica.base, `/db/${DATABASE_ID}/execute`, {
      sql: INSERT_ORDER,
      params: ['after-reconnect', 2],
    })
    await caughtUp(pair)

    expect(lostAnswer.status).not.toBe(200)
    expect(resent.status).toBe(200)
    expect(await rowsIn(replica.node.dbPath, 'orders')).toBe(SEEDED_ORDERS + 2)
    expect(primary.meter.billed(DATABASE_ID)).toEqual([
      billedAs(INSERT_ORDER, false, inserted),
      billedAs(INSERT_ORDER, false, inserted),
    ])
    expect(replica.meter.reports).toEqual([])
  })

  it('bills the primary once for each write made while the link is down, bills the replica nothing for catching up, and bills each read on the node that ran it', async () => {
    const pair = await startFaultedPair()
    const { primary, replica, link } = pair
    const inserted = await rowInserts(primary.node.dbPath, 'orders')
    expect(await scansWholeTable(primary.node.dbPath, SCAN_BY_CUSTOMER, ['x'], 'orders')).toBe(true)

    link.refuse(true)
    link.cut()
    for (let index = 0; index < WRITES_WHILE_CUT; index++) {
      const answer = await postJson(primary.base, `/db/${DATABASE_ID}/execute`, {
        sql: INSERT_ORDER,
        params: [`while-cut-${index}`, index],
      })
      expect(answer.status).toBe(200)
    }
    link.refuse(false)
    await caughtUp(pair)
    const replicaScan = await wholeTableScan(replica.node.dbPath, 'orders')
    const onReplica = await postJson(replica.base, `/db/${DATABASE_ID}/query`, { sql: SCAN_BY_CUSTOMER, params: ['x'] })
    const onPrimary = await postJson(primary.base, `/db/${DATABASE_ID}/query`, { sql: SCAN_BY_CUSTOMER, params: ['x'] })

    expect([onReplica.status, onPrimary.status]).toEqual([200, 200])
    expect(replicaScan.rowsRead).toBe(SEEDED_ORDERS + WRITES_WHILE_CUT)
    expect(primary.meter.billed(DATABASE_ID)).toEqual([
      ...Array.from({ length: WRITES_WHILE_CUT }, () => billedAs(INSERT_ORDER, false, inserted)),
      billedAs(SCAN_BY_CUSTOMER, false, await wholeTableScan(primary.node.dbPath, 'orders')),
    ])
    expect(replica.meter.billed(DATABASE_ID)).toEqual([billedAs(SCAN_BY_CUSTOMER, false, replicaScan)])
  })
})
