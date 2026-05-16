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

async function waitUntil(predicate: () => Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  throw new Error(`Timed out after ${timeoutMs}ms waiting for predicate`)
}

describe('ReplicationEngine inbound batch trigger refresh', () => {
  let tempDir: string
  let bus: MemoryBus
  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-inbound-ddl-'))
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

  describe('peer-replicated DDL followed by DML against the same watched table', () => {
    it('captures the new column on the replica when ALTER + INSERT arrive in the same batch', async () => {
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

      await primary.engine.transaction(async tx => {
        await tx.execute('ALTER TABLE inventory ADD COLUMN bar TEXT')
        await tx.execute("INSERT INTO inventory (id, sku, bar) VALUES (1, 'sku-001', 'value-bar')")
      })

      const targetSeq = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq)

      const stmt = await replica.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'inventory' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as { id: number; sku: string; bar: string }
      expect(parsed.id).toBe(1)
      expect(parsed.sku).toBe('sku-001')
      expect(parsed.bar).toBe('value-bar')
    })

    it('does not raise when a peer batch carries DDL against an unwatched table', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE watched_t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE unwatched_t (id INTEGER PRIMARY KEY, label TEXT NOT NULL);`,
        ['watched_t'],
      )
      const replica = await createNode(
        NODE_REPLICA,
        'replica',
        `CREATE TABLE watched_t (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE unwatched_t (id INTEGER PRIMARY KEY, label TEXT NOT NULL);`,
        ['watched_t'],
      )

      await primary.engine.start()
      await replica.engine.start()

      const errorEvents: Error[] = []
      replica.engine.on('replication-error', event => {
        errorEvents.push(event.error)
      })

      await primary.engine.execute('ALTER TABLE unwatched_t ADD COLUMN extra TEXT')
      const targetSeq = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq)

      const cdcStmt = await replica.conn.prepare(
        "SELECT COUNT(*) AS n FROM _sirannon_changes WHERE table_name = 'unwatched_t'",
      )
      const { n } = (await cdcStmt.get()) as { n: number }
      expect(n).toBe(0)

      expect(replica.tracker.watchedTables.has('watched_t')).toBe(true)
      expect(replica.tracker.watchedTables.has('unwatched_t')).toBe(false)

      const pragmaStmt = await replica.conn.prepare('PRAGMA table_info(unwatched_t)')
      const cols = ((await pragmaStmt.all()) as Array<{ name: string }>).map(c => c.name)
      expect(cols).toContain('extra')

      expect(errorEvents).toEqual([])
    })

    it('refreshes between consecutive ALTERs so a trailing DML captures every new column', async () => {
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

      await primary.engine.transaction(async tx => {
        await tx.execute('ALTER TABLE inventory ADD COLUMN quantity INTEGER')
        await tx.execute('ALTER TABLE inventory ADD COLUMN status TEXT')
        await tx.execute("INSERT INTO inventory (id, sku, quantity, status) VALUES (1, 'sku-001', 7, 'in-stock')")
      })

      const targetSeq = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq)

      const stmt = await replica.conn.prepare(
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
      expect(parsed).toEqual({ id: 1, sku: 'sku-001', quantity: 7, status: 'in-stock' })
    })

    it('prunes the dropped table while refreshing triggers for surviving tables in the same batch', async () => {
      const primary = await createNode(
        NODE_PRIMARY,
        'primary',
        `CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL);
         CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL);`,
        ['inventory', 'accounts'],
      )
      const replica = await createNode(
        NODE_REPLICA,
        'replica',
        `CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL);
         CREATE TABLE accounts (id INTEGER PRIMARY KEY, holder TEXT NOT NULL);`,
        ['inventory', 'accounts'],
      )

      await primary.engine.start()
      await replica.engine.start()

      await primary.engine.transaction(async tx => {
        await tx.execute('DROP TABLE inventory')
        await tx.execute('ALTER TABLE accounts ADD COLUMN balance INTEGER')
        await tx.execute("INSERT INTO accounts (id, holder, balance) VALUES (1, 'alice', 500)")
      })

      const targetSeq = primary.engine.getCurrentSeq()
      await waitUntil(async () => replica.engine.getAppliedSeq(NODE_PRIMARY) >= targetSeq)
      await waitUntil(async () => !replica.tracker.watchedTables.has('inventory'))

      expect(replica.tracker.watchedTables.has('inventory')).toBe(false)
      expect(replica.tracker.watchedTables.has('accounts')).toBe(true)

      const stmt = await replica.conn.prepare(
        "SELECT new_data FROM _sirannon_changes WHERE table_name = 'accounts' AND operation = 'INSERT' ORDER BY seq DESC LIMIT 1",
      )
      const row = (await stmt.get()) as { new_data: string } | undefined
      expect(row).toBeDefined()
      const parsed = JSON.parse(row?.new_data ?? '{}') as { id: number; holder: string; balance: number }
      expect(parsed).toEqual({ id: 1, holder: 'alice', balance: 500 })
    })
  })
})
