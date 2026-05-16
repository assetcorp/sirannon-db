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

describe('ReplicationEngine drop-table watched-entry cleanup', () => {
  let tempDir: string
  let bus: MemoryBus
  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-drop-cleanup-'))
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

    const tracker = new ChangeTracker({ replication: true })
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

  describe('engine.execute(DROP TABLE)', () => {
    it('removes the watched entry on the primary after the DDL commits', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      await primary.engine.start()

      expect(primary.tracker.watchedTables.has('inventory')).toBe(true)

      await primary.engine.execute('DROP TABLE inventory')

      expect(primary.tracker.watchedTables.has('inventory')).toBe(false)
    })

    it('leaves the watched entry intact when DROP TABLE is rolled back via embedded semicolon', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      await primary.engine.start()

      await expect(primary.engine.execute('DROP TABLE inventory;')).rejects.toThrow(
        'DDL statements containing semicolons',
      )

      expect(primary.tracker.watchedTables.has('inventory')).toBe(true)
    })

    it('is a no-op when DROP TABLE targets an unwatched table', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE watched_t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE unwatched_t (id INTEGER PRIMARY KEY, label TEXT NOT NULL);`,
        ['watched_t'],
      )
      await primary.engine.start()

      await primary.engine.execute('DROP TABLE unwatched_t')

      expect(primary.tracker.watchedTables.has('watched_t')).toBe(true)
      expect(primary.tracker.watchedTables.size).toBe(1)
    })
  })

  describe('engine.transaction(tx => DROP TABLE)', () => {
    it('removes the watched entry when the transaction commits', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute('DROP TABLE inventory')
      })

      expect(primary.tracker.watchedTables.has('inventory')).toBe(false)
    })

    it('keeps the watched entry when the transaction rolls back', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      await primary.engine.start()

      const intentional = new Error('rollback for test')
      await expect(
        primary.engine.transaction(async tx => {
          await tx.execute('DROP TABLE inventory')
          throw intentional
        }),
      ).rejects.toBe(intentional)

      expect(primary.tracker.watchedTables.has('inventory')).toBe(true)

      await primary.engine.execute("INSERT INTO inventory (id, sku) VALUES (1, 'sku-after-rollback')")

      const stmt = await primary.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as { sku: string }
      expect(parsed.sku).toBe('sku-after-rollback')
    })

    it('removes both entries when two DROP TABLE statements are issued in one transaction', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE alpha (id INTEGER PRIMARY KEY, a TEXT);
         CREATE TABLE beta (id INTEGER PRIMARY KEY, b TEXT);
         CREATE TABLE gamma (id INTEGER PRIMARY KEY, c TEXT);`,
        ['alpha', 'beta', 'gamma'],
      )
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute('DROP TABLE alpha')
        await tx.execute('DROP TABLE beta')
      })

      expect(primary.tracker.watchedTables.has('alpha')).toBe(false)
      expect(primary.tracker.watchedTables.has('beta')).toBe(false)
      expect(primary.tracker.watchedTables.has('gamma')).toBe(true)
    })

    it('removes the original entry after DROP+CREATE-with-different-schema and does not auto-watch the new table', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute('DROP TABLE inventory')
        await tx.execute('CREATE TABLE inventory (id INTEGER PRIMARY KEY, label TEXT, qty INTEGER)')
      })

      expect(primary.tracker.watchedTables.has('inventory')).toBe(false)

      const triggerStmt = await primary.conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '_sirannon_trg_inventory_%'",
      )
      const triggers = (await triggerStmt.all()) as Array<{ name: string }>
      expect(triggers).toEqual([])

      await primary.engine.execute("INSERT INTO inventory (id, label, qty) VALUES (1, 'a', 1)")
      const cdcStmt = await primary.conn.prepare(
        "SELECT COUNT(*) AS n FROM _sirannon_changes WHERE table_name = 'inventory'",
      )
      const { n } = (await cdcStmt.get()) as { n: number }
      expect(n).toBe(0)
    })

    it('preserves trigger refresh for surviving tables alongside a DROP', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL);
         CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL);`,
        ['inventory', 'accounts'],
      )
      await primary.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute('DROP TABLE inventory')
        await tx.execute('ALTER TABLE accounts ADD COLUMN balance INTEGER')
      })

      expect(primary.tracker.watchedTables.has('inventory')).toBe(false)
      expect(primary.tracker.watchedTables.has('accounts')).toBe(true)

      await primary.engine.execute("INSERT INTO accounts (id, holder, balance) VALUES (1, 'alice', 500)")
      const stmt = await primary.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'accounts' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      const parsed = JSON.parse(row?.new_data ?? '{}') as { id: number; holder: string; balance: number }
      expect(parsed.balance).toBe(500)
    })
  })

  describe('engine.forwardStatements(DROP TABLE)', () => {
    it('removes the watched entry on the primary when DROP TABLE is forwarded', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        'CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)',
        ['inventory'],
      )
      await primary.engine.start()

      await primary.engine.forwardStatements([{ sql: 'DROP TABLE inventory' }])

      expect(primary.tracker.watchedTables.has('inventory')).toBe(false)
    })

    it('removes only the dropped table when DROP and ADD COLUMN are forwarded together', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL);
         CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL);`,
        ['inventory', 'accounts'],
      )
      await primary.engine.start()

      await primary.engine.forwardStatements([
        { sql: 'ALTER TABLE accounts ADD COLUMN balance INTEGER' },
        { sql: 'DROP TABLE inventory' },
      ])

      expect(primary.tracker.watchedTables.has('inventory')).toBe(false)
      expect(primary.tracker.watchedTables.has('accounts')).toBe(true)
    })
  })

  describe('peer-applied DROP TABLE batch', () => {
    it('removes the watched entry on the replica after the batch commits', async () => {
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

      await primary.engine.execute('DROP TABLE inventory')
      const targetSeq = primary.engine.getCurrentSeq()

      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq, 8000)
      await waitUntil(async () => !replica.tracker.watchedTables.has('inventory'), 8000)

      expect(replica.tracker.watchedTables.has('inventory')).toBe(false)
    })
  })
})
