import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { InMemoryTransport, MemoryBus } from '../../../transport/memory/index.js'
import { ReplicationEngine } from '../../engine.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type { ReplicationConfig } from '../../types.js'

const NODE_PRIMARY = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_REPLICA = 'bbbb0000bbbb0000bbbb0000bbbb0000'

interface NodeContext {
  db: Database
  conn: SQLiteConnection
  engine: ReplicationEngine
  transport: InMemoryTransport
  tracker: ChangeTracker
}

async function waitUntil(predicate: () => Promise<boolean>, timeoutMs = 4000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for predicate`)
}

describe('ReplicationEngine.forwardStatements', () => {
  let tempDir: string
  let bus: MemoryBus
  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-fwd-ddl-'))
    bus = new MemoryBus()
  })

  afterEach(async () => {
    for (const engine of runningEngines) {
      try {
        await engine.stop()
      } catch {
        /* best-effort */
      }
    }
    runningEngines.length = 0
    for (const db of openDbs) {
      try {
        if (!db.closed) await db.close()
      } catch {
        /* best-effort */
      }
    }
    openDbs.length = 0
    for (const conn of openConns) {
      try {
        await conn.close()
      } catch {
        /* best-effort */
      }
    }
    openConns.length = 0
    rmSync(tempDir, { recursive: true, force: true })
  })

  async function createNode(
    nodeId: string,
    role: 'primary' | 'replica',
    schemaSql: string,
    watchedTables: string[],
    overrides: Partial<ReplicationConfig> = {},
  ): Promise<NodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker()
    if (schemaSql) {
      await conn.exec(schemaSql)
    }
    for (const table of watchedTables) {
      await tracker.watch(conn, table)
    }

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = new InMemoryTransport(bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology(role),
      transport,
      batchIntervalMs: 25,
      batchSize: 200,
      initialSync: false,
      changeTracker: tracker,
      ...overrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, engine, transport, tracker }
  }

  describe('DDL and DML in a single forwardStatements batch', () => {
    it('captures the new column on the trailing INSERT and replicates it', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      const replica = await createNode(
        NODE_REPLICA,
        'replica',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )

      await primary.engine.start()
      await replica.engine.start()

      await primary.engine.forwardStatements([
        { sql: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER' },
        { sql: "INSERT INTO inventory (id, sku, quantity) VALUES (1, 'sku-001', 42)" },
      ])

      const stmt = await primary.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as { id: number; sku: string; quantity: number }
      expect(parsed.id).toBe(1)
      expect(parsed.sku).toBe('sku-001')
      expect(parsed.quantity).toBe(42)

      const targetSeq = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq, 8000)

      const colStmt = await replica.conn.prepare('PRAGMA table_info(inventory)')
      const cols = (await colStmt.all()) as Array<{ name: string }>
      expect(cols.map(c => c.name)).toContain('quantity')

      const replicaStmt = await replica.conn.prepare('SELECT id, sku, quantity FROM inventory WHERE id = 1')
      const replicaRow = (await replicaStmt.get()) as { id: number; sku: string; quantity: number } | undefined
      expect(replicaRow).toEqual({ id: 1, sku: 'sku-001', quantity: 42 })
    })

    it('rolls back DDL and emits no orphan CDC rows when the trailing statement fails', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL UNIQUE)',
        ['inventory'],
      )
      await primary.engine.start()

      await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-existing')")

      const seqBefore = primary.engine.getCurrentSeq()
      const colsBeforeStmt = await primary.conn.prepare('PRAGMA table_info(inventory)')
      const colsBefore = ((await colsBeforeStmt.all()) as Array<{ name: string }>).map(c => c.name)

      await expect(
        primary.engine.forwardStatements([
          { sql: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER' },
          { sql: "INSERT INTO inventory (id, sku, quantity) VALUES (2, 'sku-existing', 1)" },
        ]),
      ).rejects.toThrow()

      const colsAfterStmt = await primary.conn.prepare('PRAGMA table_info(inventory)')
      const colsAfter = ((await colsAfterStmt.all()) as Array<{ name: string }>).map(c => c.name)
      expect(colsAfter).toEqual(colsBefore)
      expect(colsAfter).not.toContain('quantity')

      const countStmt = await primary.conn.prepare(
        "SELECT COUNT(*) AS n FROM _sirannon_changes WHERE table_name = 'inventory' OR new_data LIKE '%quantity%'",
      )
      const { n } = (await countStmt.get()) as { n: number }
      expect(n).toBe(1)

      expect(primary.engine.getCurrentSeq()).toBe(seqBefore)
    })

    it('refreshes triggers between consecutive DDLs so DML against both new columns captures them', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      const replica = await createNode(
        NODE_REPLICA,
        'replica',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )

      await primary.engine.start()
      await replica.engine.start()

      await primary.engine.forwardStatements([
        { sql: 'ALTER TABLE inventory ADD COLUMN quantity INTEGER' },
        { sql: 'ALTER TABLE inventory ADD COLUMN status TEXT' },
        { sql: "INSERT INTO inventory (id, sku, quantity, status) VALUES (1, 'sku-001', 7, 'in-stock')" },
      ])

      const stmt = await primary.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as {
        id: number
        sku: string
        quantity: number
        status: string
      }
      expect(parsed.id).toBe(1)
      expect(parsed.sku).toBe('sku-001')
      expect(parsed.quantity).toBe(7)
      expect(parsed.status).toBe('in-stock')

      const targetSeq = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq, 8000)

      const replicaStmt = await replica.conn.prepare('SELECT id, sku, quantity, status FROM inventory WHERE id = 1')
      const replicaRow = (await replicaStmt.get()) as
        | { id: number; sku: string; quantity: number; status: string }
        | undefined
      expect(replicaRow).toEqual({ id: 1, sku: 'sku-001', quantity: 7, status: 'in-stock' })
    })

    it('does not raise or emit CDC rows when DDL+DML run against an unwatched table', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE watched_t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE unwatched_t (id INTEGER PRIMARY KEY, label TEXT NOT NULL);`,
        ['watched_t'],
      )
      await primary.engine.start()

      const ddlChangesBefore = (
        (await (
          await primary.conn.prepare("SELECT COUNT(*) AS n FROM _sirannon_changes WHERE operation = 'DDL'")
        ).get()) as { n: number }
      ).n

      await primary.engine.forwardStatements([
        { sql: 'ALTER TABLE unwatched_t ADD COLUMN extra TEXT' },
        { sql: "INSERT INTO unwatched_t (id, label, extra) VALUES (1, 'first', 'value')" },
      ])

      const ddlChangesAfter = (
        (await (
          await primary.conn.prepare("SELECT COUNT(*) AS n FROM _sirannon_changes WHERE operation = 'DDL'")
        ).get()) as { n: number }
      ).n
      expect(ddlChangesAfter).toBe(ddlChangesBefore + 1)

      const unwatchedCdc = (
        (await (
          await primary.conn.prepare("SELECT COUNT(*) AS n FROM _sirannon_changes WHERE table_name = 'unwatched_t'")
        ).get()) as { n: number }
      ).n
      expect(unwatchedCdc).toBe(0)

      const rowStmt = await primary.conn.prepare('SELECT id, label, extra FROM unwatched_t')
      const row = (await rowStmt.get()) as { id: number; label: string; extra: string } | undefined
      expect(row).toEqual({ id: 1, label: 'first', extra: 'value' })
    })
  })
})
