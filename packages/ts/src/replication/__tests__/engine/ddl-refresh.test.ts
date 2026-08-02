import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import { ReplicationEngine } from '../../engine.js'
import { createHarness, type EngineTestHarness, makeConfig, teardownHarness } from './helpers.js'

describe('ReplicationEngine', () => {
  let harness: EngineTestHarness

  beforeEach(() => {
    harness = createHarness()
  })

  afterEach(async () => {
    await teardownHarness(harness)
  })

  describe('DDL trigger refresh', () => {
    it('captures new columns in CDC after ALTER TABLE ADD COLUMN', async () => {
      const dbPath = join(harness.tempDir, `ddl-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      harness.setWriterConn(conn)

      await conn.exec('CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)')
      const tracker = new ChangeTracker()
      await tracker.watch(conn, 'inventory')

      const db = await Database.create('ddl', dbPath, testDriver)
      harness.openDbs.push(db)

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport, { changeTracker: tracker }))
      await engine.start()

      await engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-001')")
      await engine.execute('ALTER TABLE inventory ADD COLUMN quantity INTEGER')
      await engine.execute('UPDATE inventory SET quantity = 42 WHERE id = 1')

      const stmt = await conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'UPDATE' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as { id: number; sku: string; quantity: number }
      expect(parsed.quantity).toBe(42)
      expect(parsed.sku).toBe('sku-001')

      await engine.stop()
    })

    it('captures new columns after DDL forwarded as a separate transaction', async () => {
      const dbPath = join(harness.tempDir, `ddl-fwd-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      harness.setWriterConn(conn)

      await conn.exec('CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)')
      const tracker = new ChangeTracker()
      await tracker.watch(conn, 'inventory')

      const db = await Database.create('ddl-fwd', dbPath, testDriver)
      harness.openDbs.push(db)

      const engine = new ReplicationEngine(db, conn, makeConfig(harness.transport, { changeTracker: tracker }))
      await engine.start()

      await engine.forwardStatements([{ sql: "INSERT INTO inventory (id, sku) VALUES (1, 'sku-001')" }])
      await engine.forwardStatements([{ sql: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER' }])
      await engine.forwardStatements([{ sql: 'UPDATE inventory SET quantity = 99 WHERE id = 1' }])

      const stmt = await conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'UPDATE' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as { quantity: number }
      expect(parsed.quantity).toBe(99)

      await engine.stop()
    })
  })
})
