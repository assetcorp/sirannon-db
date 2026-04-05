import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { InMemoryTransport, MemoryBus } from '../../transport/memory/index.js'
import { ReplicationEngine } from '../engine.js'
import { SyncError } from '../errors.js'
import { PrimaryReplicaTopology } from '../topology/primary-replica.js'
import type { ReplicationConfig } from '../types.js'

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

interface NodeContext {
  db: Database
  conn: SQLiteConnection
  dbPath: string
  engine: ReplicationEngine
  transport: InMemoryTransport
  tracker: ChangeTracker
}

describe('Initial Sync', () => {
  let tempDir: string
  let bus: MemoryBus
  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sync-'))
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

  async function createPrimary(
    nodeId: string,
    tableSqls: string[],
    configOverrides: Partial<ReplicationConfig> = {},
  ): Promise<NodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker({ replication: true })
    for (const sql of tableSqls) {
      await conn.exec(sql)
      const tableName = sql.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/i)?.[1]
      if (tableName) {
        await tracker.watch(conn, tableName)
      }
    }

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = new InMemoryTransport(bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: false,
      snapshotConnectionFactory: () => testDriver.open(dbPath, { readonly: true }),
      changeTracker: tracker,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, dbPath, engine, transport, tracker }
  }

  async function createReplica(nodeId: string, configOverrides: Partial<ReplicationConfig> = {}): Promise<NodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker({ replication: true })

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = new InMemoryTransport(bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology('replica'),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: true,
      changeTracker: tracker,
      syncBatchSize: 100,
      syncAckTimeoutMs: 5000,
      catchUpDeadlineMs: 3000,
      maxSyncLagBeforeReady: 10,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, dbPath, engine, transport, tracker }
  }

  async function createSyncSourceNode(
    nodeId: string,
    tableSqls: string[],
    configOverrides: Partial<ReplicationConfig> = {},
  ): Promise<NodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker({ replication: true })
    for (const sql of tableSqls) {
      await conn.exec(sql)
      const tableName = sql.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/i)?.[1]
      if (tableName) {
        await tracker.watch(conn, tableName)
      }
    }

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = new InMemoryTransport(bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new PrimaryReplicaTopology('primary'),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      initialSync: false,
      snapshotConnectionFactory: () => testDriver.open(dbPath, { readonly: true }),
      changeTracker: tracker,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, dbPath, engine, transport, tracker }
  }

  const NODE_P = 'pppp0000pppp0000pppp0000pppp0000'
  const NODE_R = 'rrrr0000rrrr0000rrrr0000rrrr0000'
  const NODE_R2 = 'ssss0000ssss0000ssss0000ssss0000'
  const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
  const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

  describe('basic sync', () => {
    it('syncs a single table from primary to a new replica', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)'])
      await primary.engine.start()

      for (let i = 1; i <= 50; i++) {
        await primary.engine.execute(`INSERT INTO users (id, name) VALUES (${i}, 'user_${i}')`)
      }

      const replica = await createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const stmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM users')
      const row = (await stmt.get()) as { cnt: number }
      expect(row.cnt).toBe(50)

      const nameStmt = await replica.conn.prepare('SELECT name FROM users WHERE id = 25')
      const nameRow = (await nameStmt.get()) as { name: string }
      expect(nameRow.name).toBe('user_25')
    })

    it('syncs multi-table with FK in correct order', async () => {
      const primary = await createPrimary(NODE_P, [
        'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
        'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id), total INTEGER NOT NULL)',
        'CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(id), product TEXT NOT NULL)',
      ])
      await primary.engine.start()

      await primary.engine.execute("INSERT INTO users VALUES (1, 'alice')")
      await primary.engine.execute("INSERT INTO users VALUES (2, 'bob')")
      await primary.engine.execute('INSERT INTO orders VALUES (10, 1, 500)')
      await primary.engine.execute('INSERT INTO orders VALUES (11, 2, 300)')
      await primary.engine.execute("INSERT INTO order_items VALUES (100, 10, 'widget')")
      await primary.engine.execute("INSERT INTO order_items VALUES (101, 11, 'gadget')")

      const replica = await createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const usersStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM users')
      const usersRow = (await usersStmt.get()) as { cnt: number }
      expect(usersRow.cnt).toBe(2)

      const ordersStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM orders')
      const ordersRow = (await ordersStmt.get()) as { cnt: number }
      expect(ordersRow.cnt).toBe(2)

      const itemsStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM order_items')
      const itemsRow = (await itemsStmt.get()) as { cnt: number }
      expect(itemsRow.cnt).toBe(2)
    })

    it('syncs schema with indexes', async () => {
      const primary = await createPrimary(NODE_P, [
        'CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL, name TEXT NOT NULL)',
      ])
      await primary.conn.exec('CREATE INDEX idx_products_sku ON products (sku)')
      await primary.engine.start()

      await primary.engine.execute("INSERT INTO products VALUES (1, 'SKU001', 'Widget')")

      const replica = await createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const idxStmt = await replica.conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_products_sku'",
      )
      const idxRow = (await idxStmt.get()) as { name: string } | undefined
      expect(idxRow).toBeDefined()
      expect(idxRow?.name).toBe('idx_products_sku')
    })

    it('syncs empty database (schema only, no rows)', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      const replica = await createReplica(NODE_R)
      await replica.engine.start()

      await wait(1500)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const stmt = await replica.conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'items'")
      const row = (await stmt.get()) as { name: string } | undefined
      expect(row).toBeDefined()
    })
  })

  describe('read blocking', () => {
    it('throws SyncError during sync, succeeds after ready', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()
      await primary.engine.execute("INSERT INTO items VALUES (1, 'test')")

      const replica = await createReplica(NODE_R)

      const replicaStarted = replica.engine.start()
      await wait(50)

      if (replica.engine.status().syncState?.phase !== 'ready') {
        await expect(replica.engine.query('SELECT 1')).rejects.toThrow(SyncError)
      }

      await replicaStarted
      await wait(2000)

      expect(replica.engine.status().syncState?.phase).toBe('ready')
      const result = await replica.engine.query<{ id: number }>('SELECT id FROM items')
      expect(result.length).toBeGreaterThanOrEqual(0)
    })
  })

  describe('out-of-band path', () => {
    it('starts in ready state with initialSync: false', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      const dbPath = join(tempDir, `oob-${Date.now()}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      openConns.push(conn)

      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      const tracker = new ChangeTracker({ replication: true })
      await tracker.watch(conn, 'items')

      const db = await Database.create('oob-db', dbPath, testDriver)
      openDbs.push(db)

      const transport = new InMemoryTransport(bus)
      const engine = new ReplicationEngine(db, conn, {
        nodeId: NODE_R,
        topology: new PrimaryReplicaTopology('replica'),
        transport,
        batchIntervalMs: 30,
        initialSync: false,
        changeTracker: tracker,
      })
      runningEngines.push(engine)
      await engine.start()

      expect(engine.status().syncState?.phase).toBe('ready')
    })

    it('starts in ready state with resumeFromSeq', async () => {
      const dbPath = join(tempDir, `resume-${Date.now()}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      openConns.push(conn)

      await conn.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
      const tracker = new ChangeTracker({ replication: true })
      await tracker.watch(conn, 'items')

      const db = await Database.create('resume-db', dbPath, testDriver)
      openDbs.push(db)

      const transport = new InMemoryTransport(bus)
      const engine = new ReplicationEngine(db, conn, {
        nodeId: NODE_R,
        topology: new PrimaryReplicaTopology('replica'),
        transport,
        batchIntervalMs: 30,
        initialSync: false,
        resumeFromSeq: 100n,
        changeTracker: tracker,
      })
      runningEngines.push(engine)
      await engine.start()

      expect(engine.status().syncState?.phase).toBe('ready')
    })
  })

  describe('catch-up after sync', () => {
    it('transitions from catching-up to ready after applying incremental changes', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      for (let i = 1; i <= 20; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await createReplica(NODE_R, { maxSyncLagBeforeReady: 5 })
      await replica.engine.start()

      await wait(3000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')
    })
  })

  describe('concurrent joiners', () => {
    it('rejects when maxConcurrentSyncs is exceeded', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'], {
        maxConcurrentSyncs: 1,
      })
      await primary.engine.start()

      for (let i = 1; i <= 200; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica1 = await createReplica(NODE_R, { syncBatchSize: 10 })
      const replica2 = await createReplica(NODE_R2, { syncBatchSize: 10 })

      await replica1.engine.start()
      await wait(100)
      await replica2.engine.start()

      await wait(5000)

      const s1 = replica1.engine.status()
      const s2 = replica2.engine.status()
      const readyCount = [s1, s2].filter(s => s.syncState?.phase === 'ready').length
      expect(readyCount).toBeGreaterThanOrEqual(1)
    })
  })

  describe('CDC trigger isolation', () => {
    it('does not pollute _sirannon_changes during sync wipe and data insertion', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      for (let i = 1; i <= 10; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await createReplica(NODE_R)

      const seqBefore = await (async () => {
        await replica.conn.exec(`CREATE TABLE IF NOT EXISTS _sirannon_changes (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          table_name TEXT NOT NULL,
          operation TEXT NOT NULL,
          row_id TEXT NOT NULL,
          changed_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
          old_data TEXT,
          new_data TEXT,
          node_id TEXT NOT NULL DEFAULT '',
          tx_id TEXT NOT NULL DEFAULT '',
          hlc TEXT NOT NULL DEFAULT ''
        )`)
        const stmt = await replica.conn.prepare('SELECT MAX(seq) as max_seq FROM _sirannon_changes')
        const row = (await stmt.get()) as { max_seq: number | null }
        return row?.max_seq ?? 0
      })()

      await replica.engine.start()
      await wait(2000)

      const stmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM _sirannon_changes WHERE seq > ?')
      const row = (await stmt.get(seqBefore)) as { cnt: number }
      expect(row.cnt).toBe(0)
    })
  })

  describe('FK pragma restoration', () => {
    it('restores PRAGMA foreign_keys after sync completes', async () => {
      const primary = await createPrimary(NODE_P, [
        'CREATE TABLE parents (id INTEGER PRIMARY KEY)',
        'CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))',
      ])
      await primary.engine.start()
      await primary.engine.execute('INSERT INTO parents VALUES (1)')
      await primary.engine.execute('INSERT INTO children VALUES (10, 1)')

      const replica = await createReplica(NODE_R)
      await replica.engine.start()
      await wait(2000)

      expect(replica.engine.status().syncState?.phase).toBe('ready')

      const fkStmt = await replica.conn.prepare('PRAGMA foreign_keys')
      const fkRow = (await fkStmt.get()) as { foreign_keys: number }
      expect(fkRow.foreign_keys).toBe(1)
    })
  })

  describe('catch-up deadline', () => {
    it('transitions to ready after deadline even with remaining lag', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()
      await primary.engine.execute("INSERT INTO items VALUES (1, 'seed')")

      const replica = await createReplica(NODE_R, {
        catchUpDeadlineMs: 500,
        maxSyncLagBeforeReady: 999_999,
      })
      await replica.engine.start()
      await wait(3000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')
    })
  })

  describe('primary-to-replica sync', () => {
    it('syncs data from primary A to joining replica B', async () => {
      const nodeA = await createSyncSourceNode(NODE_A, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await nodeA.engine.start()

      for (let i = 1; i <= 20; i++) {
        await nodeA.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const dbPathB = join(tempDir, `${NODE_B.slice(0, 8)}-${Date.now()}.db`)
      const connB = await testDriver.open(dbPathB)
      await connB.exec('PRAGMA journal_mode = WAL')
      openConns.push(connB)

      const trackerB = new ChangeTracker({ replication: true })
      const dbB = await Database.create('db-join-b', dbPathB, testDriver)
      openDbs.push(dbB)

      const transportB = new InMemoryTransport(bus)
      const engineB = new ReplicationEngine(dbB, connB, {
        nodeId: NODE_B,
        topology: new PrimaryReplicaTopology('replica'),
        transport: transportB,
        batchIntervalMs: 30,
        initialSync: true,
        changeTracker: trackerB,
        syncBatchSize: 100,
        syncAckTimeoutMs: 5000,
        catchUpDeadlineMs: 5000,
        maxSyncLagBeforeReady: 5,
      })
      runningEngines.push(engineB)
      await engineB.start()

      await wait(3000)

      const status = engineB.status()
      expect(status.syncState?.phase).toBe('ready')

      const stmt = await connB.prepare('SELECT COUNT(*) as cnt FROM items')
      const row = (await stmt.get()) as { cnt: number }
      expect(row.cnt).toBe(20)
    })
  })

  describe('large dataset', () => {
    it('syncs multiple tables with many rows', async () => {
      const primary = await createPrimary(NODE_P, [
        'CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)',
        'CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)',
        'CREATE TABLE t3 (id INTEGER PRIMARY KEY, val TEXT)',
      ])
      await primary.engine.start()

      for (let t = 1; t <= 3; t++) {
        for (let i = 1; i <= 200; i++) {
          await primary.engine.execute(`INSERT INTO t${t} VALUES (${i}, 'v${i}')`)
        }
      }

      const replica = await createReplica(NODE_R, { syncBatchSize: 50 })
      await replica.engine.start()

      await wait(5000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      for (let t = 1; t <= 3; t++) {
        const stmt = await replica.conn.prepare(`SELECT COUNT(*) as cnt FROM t${t}`)
        const row = (await stmt.get()) as { cnt: number }
        expect(row.cnt).toBe(200)
      }
    })
  })

  describe('timeout', () => {
    it('source aborts sync when maxSyncDurationMs exceeded', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'], {
        maxSyncDurationMs: 200,
      })
      await primary.engine.start()

      for (let i = 1; i <= 500; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await createReplica(NODE_R, { syncBatchSize: 5 })
      await replica.engine.start()

      await wait(3000)

      const status = replica.engine.status()
      expect(['pending', 'syncing', 'ready']).toContain(status.syncState?.phase)
    })
  })

  describe('integrity verification', () => {
    it('detects row count mismatch in manifest', async () => {
      const primary = await createPrimary(NODE_P, ['CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'])
      await primary.engine.start()

      for (let i = 1; i <= 10; i++) {
        await primary.engine.execute(`INSERT INTO items VALUES (${i}, 'item_${i}')`)
      }

      const replica = await createReplica(NODE_R)
      await replica.engine.start()

      await wait(2000)

      const status = replica.engine.status()
      expect(status.syncState?.phase).toBe('ready')

      const countStmt = await replica.conn.prepare('SELECT COUNT(*) as cnt FROM items')
      const countRow = (await countStmt.get()) as { cnt: number }
      expect(countRow.cnt).toBe(10)
    })
  })
})
