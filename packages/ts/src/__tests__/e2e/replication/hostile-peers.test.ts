import { describe, expect, it } from 'vitest'
import type { ReplicationBatch } from '../../../replication/index.js'
import type { ManagedNode } from '../lib/factory.js'
import { errorsOf, eventually, type Row, readRows, tableNames, tableRows } from '../lib/replication-scene.js'
import {
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
import { ddlChange, dialAsPeer, insertChange, listenAsPeer, type RawPeer, signedBatch, stampAt } from './peers.js'

const handle = sceneForEachTest()

const BATCH_LIMIT = 50
const DRIFT_LIMIT_MS = 5_000

interface HostileSource {
  source: RawPeer
  replica: ManagedNode
  nextSeq: () => bigint
  deliver: (batch: ReplicationBatch) => Promise<'applied' | string>
}

function identityOf(nodeId: string) {
  const { certs } = handle
  return { nodeId, cert: certs.certForNode(nodeId), caCertPath: certs.caCertPath }
}

async function replicaOfHostileSource(extraEndpoints: number[] = []): Promise<HostileSource> {
  const { scene } = handle
  const source = await listenAsPeer(scene, identityOf(PRIMARY), 'primary')
  const replica = await scene.replica(REPLICA, {
    port: source.port,
    config: {
      initialSync: false,
      maxBatchChanges: BATCH_LIMIT,
      maxClockDriftMs: DRIFT_LIMIT_MS,
      transportConfig: {
        localRole: 'replica',
        endpoints: [`localhost:${source.port}`, ...extraEndpoints.map(port => `localhost:${port}`)],
      },
    },
  })
  await source.connected(REPLICA)
  let seq = 0n
  const deliver = async (batch: ReplicationBatch): Promise<'applied' | string> => {
    const errorsBefore = errorsOf(replica, 'batch-received').length
    const appliedBefore = replica.engine.getAppliedSeq(PRIMARY)
    await source.transport.send(REPLICA, batch)
    await eventually(
      () =>
        errorsOf(replica, 'batch-received').length > errorsBefore ||
        replica.engine.getAppliedSeq(PRIMARY) >= batch.toSeq ||
        source.acks.some(({ message }) => message.batchId === batch.batchId),
      `the replica to answer batch ${batch.batchId}`,
    )
    const refusal = errorsOf(replica, 'batch-received').slice(errorsBefore)[0]
    if (refusal) return `${(refusal.error as { code?: string }).code}: ${refusal.error.message}`
    return replica.engine.getAppliedSeq(PRIMARY) > appliedBefore ? 'applied' : 'skipped'
  }
  const created = await deliver(signedBatch(PRIMARY, 1n, [ddlChange(ORDERS_SCHEMA, PRIMARY, stampAt(0, PRIMARY))]))
  expect(created).toBe('applied')
  seq = 1n
  return {
    source,
    replica,
    nextSeq: () => {
      seq += 1n
      return seq
    },
    deliver,
  }
}

function orders(rows: Row[], from: string, hlcOffsetMs = 0) {
  return rows.map((row, at) => insertChange('orders', row, from, stampAt(hlcOffsetMs, from, at)))
}

describe('a replica receiving batches from a peer that holds a valid certificate', () => {
  it('applies each signed batch once, and skips a replayed or overlapping one without duplicating rows', async () => {
    const { replica, nextSeq, deliver } = await replicaOfHostileSource()
    const first = signedBatch(PRIMARY, nextSeq(), orders([order(1, 'amara', 10)], PRIMARY))
    const overlapStart = nextSeq()
    nextSeq()
    const overlapping = signedBatch(PRIMARY, first.fromSeq, [
      ...first.changes,
      ...orders([order(2, 'kofi', 20), order(3, 'esi', 30)], PRIMARY),
    ])

    expect(await deliver(first)).toBe('applied')
    expect(await deliver(first)).toBe('skipped')
    expect(await deliver(overlapping)).toBe('applied')
    expect(overlapping.toSeq).toBe(overlapStart + 1n)
    expect(tableRows(replica.dbPath, 'orders')).toEqual([
      order(1, 'amara', 10),
      order(2, 'kofi', 20),
      order(3, 'esi', 30),
    ])
  })

  it('refuses a batch whose checksum does not match the spec canonical form of its changes, and applies nothing from it', async () => {
    const { replica, nextSeq, deliver } = await replicaOfHostileSource()
    const honest = signedBatch(PRIMARY, nextSeq(), orders([order(1, 'amara', 10)], PRIMARY))
    const altered = { ...honest, changes: orders([order(1, 'amara', 10_000)], PRIMARY) }
    const zeroed = { ...honest, checksum: '0'.repeat(64) }
    const uppercase = { ...honest, checksum: honest.checksum.toUpperCase() }

    for (const tampered of [altered, zeroed, uppercase]) {
      expect(await deliver(tampered)).toMatch(/^BATCH_VALIDATION_ERROR: Checksum mismatch/)
    }
    expect(tableRows(replica.dbPath, 'orders')).toEqual([])
    expect(await deliver(honest)).toBe('applied')
    expect(tableRows(replica.dbPath, 'orders')).toEqual([order(1, 'amara', 10)])
  })

  it('refuses schema changes outside the allowlist and data changes aimed at reserved or malformed tables', async () => {
    const { replica, nextSeq, deliver } = await replicaOfHostileSource()
    const statements = [
      'CREATE TABLE stolen AS SELECT * FROM orders',
      'DROP TABLE orders; DROP TABLE planted',
      'CREATE TABLE planted (id INTEGER PRIMARY KEY, junk BLOB DEFAULT (randomblob(16)))',
      "CREATE TABLE planted (id INTEGER PRIMARY KEY, path TEXT DEFAULT (readfile('/etc/passwd')))",
      'DROP TABLE _sirannon_changes',
      'DROP TABLE main._sirannon_peer_state',
      'CREATE INDEX planted_index ON _sirannon_changes (seq)',
      'CREATE TABLE sqlite_planted (id INTEGER)',
      "ATTACH DATABASE '/tmp/planted.db' AS planted",
      'ALTER TABLE orders RENAME TO planted',
      'CREATE TRIGGER planted AFTER INSERT ON orders BEGIN DELETE FROM orders; END',
    ]
    for (const statement of statements) {
      const outcome = await deliver(
        signedBatch(PRIMARY, nextSeq(), [ddlChange(statement, PRIMARY, stampAt(0, PRIMARY))]),
      )
      expect(outcome, statement).toMatch(/^BATCH_VALIDATION_ERROR: Unsafe or missing DDL/)
    }
    for (const table of ['_sirannon_peer_state', 'sqlite_master', 'orders"; DROP TABLE orders; --', '']) {
      const outcome = await deliver(
        signedBatch(PRIMARY, nextSeq(), [insertChange(table, { id: 1 }, PRIMARY, stampAt(0, PRIMARY))]),
      )
      expect(outcome, table).toMatch(/^BATCH_VALIDATION_ERROR: Invalid table name/)
    }
    const accepted = await deliver(
      signedBatch(PRIMARY, nextSeq(), [
        ddlChange(
          'CREATE TABLE hints (id INTEGER PRIMARY KEY, sqlite_hint TEXT, attached_at TEXT)',
          PRIMARY,
          stampAt(0, PRIMARY),
        ),
      ]),
    )

    expect(accepted).toBe('applied')
    expect(tableNames(replica.dbPath)).toEqual(['hints', 'orders'])
    expect(
      readRows(replica.dbPath, "SELECT count(*) AS n FROM sqlite_master WHERE type = 'trigger' AND name = 'planted'"),
    ).toEqual([{ n: 0 }])
  })

  it('accepts a batch of exactly its change limit and refuses one change more', async () => {
    const { replica, nextSeq, deliver } = await replicaOfHostileSource()
    const atLimit = Array.from({ length: BATCH_LIMIT }, (_, at) => order(at + 1, `at-limit-${at}`, at))
    const pastLimit = Array.from({ length: BATCH_LIMIT + 1 }, (_, at) => order(1_000 + at, `past-limit-${at}`, at))

    const tooMany = signedBatch(PRIMARY, nextSeq(), orders(pastLimit, PRIMARY))
    expect(await deliver(tooMany)).toBe(
      `BATCH_VALIDATION_ERROR: Batch too large: ${BATCH_LIMIT + 1} changes exceeds max ${BATCH_LIMIT}`,
    )
    expect(await deliver(signedBatch(PRIMARY, tooMany.fromSeq, orders(atLimit, PRIMARY)))).toBe('applied')
    expect(tableRows(replica.dbPath, 'orders')).toEqual(atLimit)
  })

  it('accepts a clock stamp inside its drift limit and refuses one past it in the future', async () => {
    const { replica, nextSeq, deliver } = await replicaOfHostileSource()
    const ahead = signedBatch(PRIMARY, nextSeq(), orders([order(2, 'from the future', 1)], PRIMARY, DRIFT_LIMIT_MS * 2))
    const inside = signedBatch(PRIMARY, nextSeq(), orders([order(1, 'inside', 1)], PRIMARY, DRIFT_LIMIT_MS / 2))

    expect(await deliver(ahead)).toMatch(/^BATCH_VALIDATION_ERROR: Clock drift too high/)
    expect(await deliver(inside)).toBe('applied')
    expect(tableRows(replica.dbPath, 'orders')).toEqual([order(1, 'inside', 1)])
  })

  it('refuses a batch naming another source, and every batch from a connected peer that is not a primary', async () => {
    const { scene } = handle
    const bystander = await listenAsPeer(scene, identityOf(SECOND_REPLICA), 'replica')
    const { replica, nextSeq, deliver } = await replicaOfHostileSource([bystander.port])
    await bystander.connected(REPLICA)
    const misnamed = signedBatch(SECOND_REPLICA, nextSeq(), orders([order(1, 'misnamed', 1)], SECOND_REPLICA))
    const errorsBefore = errorsOf(replica, 'batch-received').length

    expect(await deliver(misnamed)).toMatch(/^BATCH_VALIDATION_ERROR: Batch sourceNodeId/)
    await bystander.transport.send(
      REPLICA,
      signedBatch(SECOND_REPLICA, 1n, orders([order(2, 'from a replica', 1)], SECOND_REPLICA)),
    )
    await eventually(() => errorsOf(replica, 'batch-received').length > errorsBefore + 1, 'the second refusal')

    expect(errorsOf(replica, 'batch-received').at(-1)?.error.message).toBe(
      `Rejected batch from unauthorized peer: ${SECOND_REPLICA}`,
    )
    expect(replica.engine.getAppliedSeq(SECOND_REPLICA)).toBe(0n)
    expect(tableRows(replica.dbPath, 'orders')).toEqual([])
  })
})

describe('a primary receiving acknowledgements from a peer that holds a valid certificate', () => {
  it('ignores an acknowledgement or sync acknowledgement sent in the name of another replica', async () => {
    const { scene } = handle
    const primary = await scene.primary(PRIMARY, { schema: [ORDERS_SCHEMA], watch: ['orders'] })
    await primary.engine.executeBatch(
      INSERT_ORDER,
      Array.from({ length: 30 }, (_, at) => [at + 1, `seed-${at}`, at]),
    )
    const proxy = await scene.proxy(primary.port)
    const victim = await scene.replica(REPLICA, { port: proxy.port })
    await ready(victim)
    const [{ request_id: requestId }] = readRows(
      victim.dbPath,
      'SELECT request_id FROM _sirannon_sync_state WHERE request_id IS NOT NULL',
    ) as Array<{ request_id: string }>
    proxy.delay('to-client', 60_000)
    const spoofer = await dialAsPeer(scene, identityOf(INTRUDER), primary.port)
    await spoofer.connected(PRIMARY)

    await spoofer.transport.sendSyncAck(PRIMARY, {
      requestId,
      joinerNodeId: REPLICA,
      table: '__schema__',
      batchIndex: 0,
      success: false,
      error: 'forged refusal',
    })
    await spoofer.transport.sendAck(PRIMARY, { batchId: 'forged', ackedSeq: 1_000n, nodeId: REPLICA })
    await eventually(
      () =>
        errorsOf(primary, 'sync-ack-identity-mismatch').length > 0 &&
        errorsOf(primary, 'ack-identity-mismatch').length > 0,
      'the primary to refuse both forgeries',
    )
    await spoofer.transport.disconnect()
    await eventually(
      () => !primary.engine.status().peers.some(peer => peer.nodeId === INTRUDER && peer.connected),
      'the spoofer to leave',
    )
    const forgedWrite = await primary.engine
      .execute(INSERT_ORDER, [31, 'needs every replica', 1], { writeConcern: { level: 'all', timeoutMs: 300 } })
      .catch((error: unknown) => error)
    proxy.delay('to-client', 0)

    expect(errorsOf(primary, 'sync-ack-identity-mismatch')[0]?.error).toMatchObject({
      code: 'SYNC_ACK_NODE_ID_MISMATCH',
    })
    expect(errorsOf(primary, 'ack-identity-mismatch')[0]?.error).toMatchObject({ code: 'ACK_NODE_ID_MISMATCH' })
    expect(forgedWrite).toMatchObject({ code: 'WRITE_CONCERN_ERROR' })
    expect(errorsOf(victim, 'sync-refused')).toEqual([])
    await eventually(() => tableRows(victim.dbPath, 'orders').length === 31, 'the victim to hold every order')
    expect(tableRows(victim.dbPath, 'orders')).toEqual(tableRows(primary.dbPath, 'orders'))
    expect(primary.engine.status().peers.find(peer => peer.nodeId === REPLICA)?.lastAckedSeq).toBeLessThan(1_000n)
  })
})
