import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationConfig, ReplicationErrorEvent } from '../../types.js'
import { MockTransport, NODE_A, NODE_B } from './helpers.js'

describe('ReplicationEngine error events', () => {
  let tempDir: string
  let writerConn: SQLiteConnection
  let transport: MockTransport
  const openDbs: Database[] = []

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-err-'))
    transport = new MockTransport()
  })

  afterEach(async () => {
    for (const db of openDbs) {
      try {
        if (!db.closed) await db.close()
      } catch {
        /* cleanup */
      }
    }
    openDbs.length = 0

    if (writerConn) {
      try {
        await writerConn.close()
      } catch {
        /* cleanup */
      }
    }

    rmSync(tempDir, { recursive: true, force: true })
  })

  async function createDbAndConn(tableSql?: string): Promise<{ db: Database; conn: SQLiteConnection }> {
    const dbPath = join(tempDir, `test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    writerConn = conn

    if (tableSql) {
      await conn.exec(tableSql)
      const tracker = new ChangeTracker()
      const tableName = tableSql.match(/CREATE TABLE (\w+)/)?.[1]
      if (tableName) {
        await tracker.watch(conn, tableName)
      }
    }

    const db = await Database.create('test', dbPath, testDriver)
    openDbs.push(db)
    return { db, conn }
  }

  function makeConfig(overrides: Partial<ReplicationConfig> = {}): ReplicationConfig {
    return {
      nodeId: NODE_A,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      batchIntervalMs: 50,
      initialSync: false,
      ...overrides,
    }
  }

  it('emits error with correct fields when sender loop fails', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    transport.addPeer(NODE_B, 'replica')

    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    transport.sendShouldFail = true

    const errorEvents: ReplicationErrorEvent[] = []
    engine.on('replication-error', (event: ReplicationErrorEvent) => {
      errorEvents.push(event)
    })

    await new Promise(resolve => setTimeout(resolve, 200))

    const transportSendErrors = errorEvents.filter(e => e.operation === 'transport-send')
    expect(transportSendErrors.length).toBeGreaterThanOrEqual(1)

    const event = transportSendErrors[0]
    if (event) {
      expect(event.error).toBeInstanceOf(Error)
      expect(event.error.message).toBe('transport send failed')
      expect(event.operation).toBe('transport-send')
      expect(event.peerId).toBe(NODE_B)
      expect(event.recoverable).toBe(true)
    }

    transport.sendShouldFail = false
    await engine.stop()
  })

  it('includes correct operation and recoverable fields in error events', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const customError = new Error('custom transport failure')
    transport.sendError = customError

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    transport.addPeer(NODE_B, 'replica')
    await engine.execute("INSERT INTO items VALUES (1, 'x')")

    transport.sendShouldFail = true

    const errorEvents: ReplicationErrorEvent[] = []
    engine.on('replication-error', (event: ReplicationErrorEvent) => {
      errorEvents.push(event)
    })

    await new Promise(resolve => setTimeout(resolve, 200))

    const sendError = errorEvents.find(e => e.operation === 'transport-send')
    expect(sendError).toBeDefined()
    if (sendError) {
      expect(sendError.error).toBe(customError)
      expect(sendError.recoverable).toBe(true)
      expect(sendError.peerId).toBe(NODE_B)
    }

    transport.sendShouldFail = false
    await engine.stop()
  })

  it('continues operating after emitting errors', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    transport.addPeer(NODE_B, 'replica')
    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    transport.sendShouldFail = true

    const errorCount = { value: 0 }
    engine.on('replication-error', () => {
      errorCount.value += 1
    })

    await new Promise(resolve => setTimeout(resolve, 200))

    expect(errorCount.value).toBeGreaterThanOrEqual(1)

    transport.sendShouldFail = false

    const status = engine.status()
    expect(status.replicating).toBe(true)

    await engine.execute("INSERT INTO items VALUES (2, 'b')")

    await new Promise(resolve => setTimeout(resolve, 200))

    expect(transport.sentBatches.length).toBeGreaterThanOrEqual(1)

    await engine.stop()
  })

  it('emits error events for sync-related failures', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    const errorEvents: ReplicationErrorEvent[] = []
    engine.on('replication-error', (event: ReplicationErrorEvent) => {
      errorEvents.push(event)
    })

    transport.addPeer(NODE_B, 'replica')

    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    transport.sendShouldFail = true

    await new Promise(resolve => setTimeout(resolve, 200))

    const transportErrors = errorEvents.filter(e => e.operation === 'transport-send')
    expect(transportErrors.length).toBeGreaterThanOrEqual(1)

    const firstErr = transportErrors[0]
    if (firstErr) {
      expect(firstErr.recoverable).toBe(true)
      expect(firstErr.peerId).toBe(NODE_B)
    }

    transport.sendShouldFail = false
    await engine.stop()
  })
})
