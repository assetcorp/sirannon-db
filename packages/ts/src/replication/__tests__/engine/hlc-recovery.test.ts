import { createHash } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { HLC } from '../../../core/sync/hlc.js'
import { ReplicationEngine } from '../../engine.js'
import { canonicaliseForChecksum } from '../../log.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import {
  createDbAndConn,
  createHarness,
  type EngineTestHarness,
  makeConfig,
  NODE_A,
  NODE_B,
  teardownHarness,
} from './helpers.js'

function buildBatchFromHlc(hlcVal: string, rowId: number, seq: bigint) {
  const changes = [
    {
      table: 'users',
      operation: 'insert' as const,
      rowId: String(rowId),
      primaryKey: { id: rowId },
      hlc: hlcVal,
      txId: `remote-tx-${seq}`,
      nodeId: NODE_B,
      newData: { id: rowId, name: `Remote-${rowId}` },
      oldData: null,
    },
  ]
  const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')
  return {
    sourceNodeId: NODE_B,
    batchId: `${NODE_B}-${seq}-${seq}`,
    fromSeq: seq,
    toSeq: seq,
    hlcRange: { min: hlcVal, max: hlcVal },
    changes,
    checksum,
  }
}

describe('ReplicationEngine HLC recovery on restart', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  it('starts an empty database without error and leaves HLC at the initial state', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engine.start()

    const first = engine.hlc.now()
    const decoded = HLC.decode(first)
    expect(decoded.wallMs).toBeGreaterThan(0)
    expect(decoded.nodeId).toBe(NODE_A)

    await engine.stop()
  })

  it('starts when replication tables exist but contain no rows', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const engineA = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineA.start()
    await engineA.stop()

    const usersCount = (await (await conn.prepare('SELECT COUNT(*) AS c FROM users')).get()) as { c: number }
    const changesCount = (await (await conn.prepare('SELECT COUNT(*) AS c FROM _sirannon_changes')).get()) as {
      c: number
    }
    const versionsCount = (await (await conn.prepare('SELECT COUNT(*) AS c FROM _sirannon_column_versions')).get()) as {
      c: number
    }
    expect(usersCount.c).toBe(0)
    expect(changesCount.c).toBe(0)
    expect(versionsCount.c).toBe(0)

    const engineB = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineB.start()

    const next = engineB.hlc.now()
    expect(HLC.decode(next).nodeId).toBe(NODE_A)

    await engineB.stop()
  })

  it('preserves local-write HLC monotonicity across restart', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const engineA = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineA.start()
    for (let i = 1; i <= 5; i++) {
      await engineA.execute(`INSERT INTO users (id, name) VALUES (${i}, 'u${i}')`)
    }
    const lastBeforeStop = engineA.hlc.now()
    await engineA.stop()

    const engineB = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineB.start()
    const firstAfterRestart = engineB.hlc.now()

    expect(HLC.compare(firstAfterRestart, lastBeforeStop)).toBeGreaterThan(0)

    await engineB.stop()
  })

  it('preserves applied-remote HLC monotonicity across restart', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const engineA = new ReplicationEngine(
      db,
      conn,
      makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica') }),
    )
    await engineA.start()
    harness.transport.addPeer(NODE_B, 'primary')

    const farFutureMs = Date.now() + 60_000
    const wallHex = farFutureMs.toString(16).padStart(12, '0')
    const remoteHlcVal = `${wallHex}-0000-${NODE_B}`

    await harness.transport.triggerBatchReceived(buildBatchFromHlc(remoteHlcVal, 9, 1n), NODE_B)

    const versionsStmt = await conn.prepare('SELECT MAX(hlc) AS m FROM _sirannon_column_versions')
    const versionsRow = (await versionsStmt.get()) as { m: string | null }
    expect(versionsRow.m).toBe(remoteHlcVal)

    await engineA.stop()

    const engineB = new ReplicationEngine(
      db,
      conn,
      makeConfig(harness.transport, { topology: new PrimaryReplicaTopology('replica') }),
    )
    await engineB.start()
    const firstAfterRestart = engineB.hlc.now()

    expect(HLC.compare(firstAfterRestart, remoteHlcVal)).toBeGreaterThan(0)

    await engineB.stop()
  })

  it('takes the max of local-write and applied-remote HLCs across restart', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const engineA = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineA.start()
    await engineA.execute("INSERT INTO users (id, name) VALUES (1, 'local-row')")
    const localHlcRow = (await (await conn.prepare('SELECT MAX(hlc) AS m FROM _sirannon_changes')).get()) as {
      m: string | null
    }
    const localHlc = localHlcRow.m
    expect(localHlc).not.toBeNull()
    if (localHlc === null) throw new Error('expected a persisted local HLC')
    await engineA.stop()

    const farFutureMs = Date.now() + 120_000
    const wallHex = farFutureMs.toString(16).padStart(12, '0')
    const remoteHlcVal = `${wallHex}-0000-${NODE_B}`
    const seedRemote = await conn.prepare(
      `INSERT INTO _sirannon_column_versions (table_name, row_id, column_name, hlc, node_id)
       VALUES (?, ?, ?, ?, ?)`,
    )
    await seedRemote.run('users', '42', 'name', remoteHlcVal, NODE_B)

    expect(HLC.compare(remoteHlcVal, localHlc)).toBeGreaterThan(0)

    const engineB = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineB.start()
    const firstAfterRestart = engineB.hlc.now()

    expect(HLC.compare(firstAfterRestart, remoteHlcVal)).toBeGreaterThan(0)
    expect(HLC.compare(firstAfterRestart, localHlc)).toBeGreaterThan(0)

    await engineB.stop()
  })

  it('recovers a future-stamped HLC even when wall-clock rewinds before restart', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const engineA = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engineA.start()

    const farFutureMs = Date.now() + 5 * 60_000
    const wallHex = farFutureMs.toString(16).padStart(12, '0')
    const farFutureHlc = `${wallHex}-0003-${NODE_A}`
    const insertStmt = await conn.prepare(
      `INSERT INTO _sirannon_column_versions (table_name, row_id, column_name, hlc, node_id)
       VALUES (?, ?, ?, ?, ?)`,
    )
    await insertStmt.run('users', '1', 'name', farFutureHlc, NODE_A)

    await engineA.stop()

    const originalNow = Date.now
    const rewindBaseMs = farFutureMs - 6 * 60_000
    const dateSpy = vi.spyOn(Date, 'now').mockImplementation(() => rewindBaseMs)

    try {
      const engineB = new ReplicationEngine(db, conn, makeConfig(harness.transport))
      await engineB.start()
      const firstAfterRestart = engineB.hlc.now()

      const decoded = HLC.decode(firstAfterRestart)
      expect(decoded.wallMs).toBeGreaterThanOrEqual(farFutureMs)
      expect(HLC.compare(firstAfterRestart, farFutureHlc)).toBeGreaterThan(0)

      await engineB.stop()
    } finally {
      dateSpy.mockRestore()
      expect(Date.now).toBe(originalNow)
    }
  })

  it('ignores empty-string HLC sentinels written by CDC triggers before stamping', async () => {
    const { db, conn } = await createDbAndConn(harness, 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const insertSentinel = await conn.prepare(
      `INSERT INTO _sirannon_changes (table_name, operation, row_id, node_id, tx_id, hlc)
       VALUES ('users', 'INSERT', '99', '', '', '')`,
    )
    await insertSentinel.run()

    const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport))
    await engine.start()

    const first = engine.hlc.now()
    expect(HLC.decode(first).wallMs).toBeGreaterThan(0)

    await engine.stop()
  })
})
