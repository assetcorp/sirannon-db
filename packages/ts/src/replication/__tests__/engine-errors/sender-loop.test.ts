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
import { MockTransport, NODE_A } from './helpers.js'

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
      const tracker = new ChangeTracker({ replication: true })
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

  it('emits error on sender loop database failures', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    const errorEvents: ReplicationErrorEvent[] = []
    engine.on('replication-error', (event: ReplicationErrorEvent) => {
      errorEvents.push(event)
    })

    await conn.exec('DROP TABLE IF EXISTS _sirannon_changes')

    await new Promise(resolve => setTimeout(resolve, 300))

    const senderErrors = errorEvents.filter(e => e.operation === 'sender-loop')
    expect(senderErrors.length).toBeGreaterThanOrEqual(1)

    if (senderErrors[0]) {
      expect(senderErrors[0].recoverable).toBe(true)
      expect(senderErrors[0].error).toBeInstanceOf(Error)
    }

    await engine.stop()
  })
})
