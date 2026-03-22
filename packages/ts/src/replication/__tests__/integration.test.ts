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
import { TopologyError } from '../errors.js'
import { MultiPrimaryTopology } from '../topology/multi-primary.js'
import { PrimaryReplicaTopology } from '../topology/primary-replica.js'
import type { ReplicationConfig } from '../types.js'

const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'
const NODE_C = 'cccc0000cccc0000cccc0000cccc0000'

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

interface NodeContext {
  db: Database
  conn: SQLiteConnection
  engine: ReplicationEngine
  transport: InMemoryTransport
}

describe('Replication Integration', () => {
  let tempDir: string
  let bus: MemoryBus
  const openDbs: Database[] = []
  const openConns: SQLiteConnection[] = []
  const runningEngines: ReplicationEngine[] = []

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integ-'))
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

  async function createNode(nodeId: string, configOverrides: Partial<ReplicationConfig> = {}): Promise<NodeContext> {
    const dbPath = join(tempDir, `${nodeId.slice(0, 8)}-${Date.now()}.db`)
    const conn = await testDriver.open(dbPath)
    await conn.exec('PRAGMA journal_mode = WAL')
    openConns.push(conn)

    const tracker = new ChangeTracker({ replication: true })
    await conn.exec(`
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				name TEXT NOT NULL,
				value INTEGER NOT NULL DEFAULT 0
			)
		`)
    await tracker.watch(conn, 'items')

    const db = await Database.create(`db-${nodeId.slice(0, 8)}`, dbPath, testDriver)
    openDbs.push(db)

    const transport = new InMemoryTransport(bus)
    const config: ReplicationConfig = {
      nodeId,
      topology: new MultiPrimaryTopology(),
      transport,
      batchIntervalMs: 30,
      batchSize: 100,
      ...configOverrides,
    }

    const engine = new ReplicationEngine(db, conn, config)
    runningEngines.push(engine)

    return { db, conn, engine, transport }
  }

  async function queryItems(conn: SQLiteConnection): Promise<Array<{ id: number; name: string; value: number }>> {
    const stmt = await conn.prepare('SELECT id, name, value FROM items ORDER BY id')
    return stmt.all() as Promise<Array<{ id: number; name: string; value: number }>>
  }

  async function queryItem(
    conn: SQLiteConnection,
    id: number,
  ): Promise<{ id: number; name: string; value: number } | undefined> {
    const stmt = await conn.prepare('SELECT id, name, value FROM items WHERE id = ?')
    return stmt.get(id) as Promise<{ id: number; name: string; value: number } | undefined>
  }

  describe('two-node replication via sender loop', () => {
    it('replicates data from writer to peer through InMemoryTransport', async () => {
      const nodeA = await createNode(NODE_A)
      const nodeB = await createNode(NODE_B)

      await nodeA.engine.start()
      await nodeB.engine.start()

      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'widget', 100)")
      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (2, 'gadget', 200)")

      await wait(300)

      const itemsB = await queryItems(nodeB.conn)
      expect(itemsB).toHaveLength(2)
      expect(itemsB[0].name).toBe('widget')
      expect(itemsB[0].value).toBe(100)
      expect(itemsB[1].name).toBe('gadget')
      expect(itemsB[1].value).toBe(200)
    })
  })

  describe('write forwarding via transport', () => {
    it('forwards a single write from replica transport to primary engine', async () => {
      const primary = await createNode(NODE_A, {
        topology: new MultiPrimaryTopology(),
      })
      const replica = await createNode(NODE_B, {
        topology: new PrimaryReplicaTopology('replica'),
        writeForwarding: true,
      })

      await primary.engine.start()
      await replica.engine.start()

      const result = await replica.transport.forward(NODE_A, {
        requestId: 'fwd-test-1',
        statements: [{ sql: "INSERT INTO items (id, name, value) VALUES (1, 'forwarded', 42)" }],
      })

      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)

      const itemOnPrimary = await queryItem(primary.conn, 1)
      expect(itemOnPrimary).toBeDefined()
      expect(itemOnPrimary?.name).toBe('forwarded')
      expect(itemOnPrimary?.value).toBe(42)
    })
  })

  describe('statement forwarding via transport', () => {
    it('forwards multiple statements atomically through the transport layer', async () => {
      const primary = await createNode(NODE_A, {
        topology: new MultiPrimaryTopology(),
      })
      const replica = await createNode(NODE_B, {
        topology: new PrimaryReplicaTopology('replica'),
      })

      await primary.engine.start()
      await replica.engine.start()

      const result = await replica.transport.forward(NODE_A, {
        requestId: 'fwd-batch-1',
        statements: [
          { sql: "INSERT INTO items (id, name, value) VALUES (1, 'alpha', 10)" },
          { sql: "INSERT INTO items (id, name, value) VALUES (2, 'beta', 20)" },
          { sql: "INSERT INTO items (id, name, value) VALUES (3, 'gamma', 30)" },
        ],
      })

      expect(result.results).toHaveLength(3)
      for (const r of result.results) {
        expect(r.changes).toBe(1)
      }

      const itemsOnPrimary = await queryItems(primary.conn)
      expect(itemsOnPrimary).toHaveLength(3)
      expect(itemsOnPrimary.map(i => i.name)).toEqual(['alpha', 'beta', 'gamma'])
    })
  })

  describe('transaction forwarding rejection', () => {
    it('throws TopologyError when replica attempts a local transaction', async () => {
      const primary = await createNode(NODE_A, {
        topology: new PrimaryReplicaTopology('primary'),
      })
      const replica = await createNode(NODE_B, {
        topology: new PrimaryReplicaTopology('replica'),
        writeForwarding: true,
      })

      await primary.engine.start()
      await replica.engine.start()

      await expect(
        replica.engine.transaction(async tx => {
          await tx.execute("INSERT INTO items (id, name, value) VALUES (1, 'should-fail', 0)")
        }),
      ).rejects.toThrow(TopologyError)
    })
  })

  describe('two-node multi-primary with LWW', () => {
    it('resolves concurrent writes consistently on both nodes', async () => {
      const nodeA = await createNode(NODE_A)
      const nodeB = await createNode(NODE_B)

      await nodeA.engine.start()
      await nodeB.engine.start()

      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'seed-a', 0)")
      await wait(300)

      await wait(10)
      await nodeA.engine.execute("UPDATE items SET name = 'updated-a', value = 111 WHERE id = 1")
      await wait(10)
      await nodeB.engine.execute("UPDATE items SET name = 'updated-b', value = 222 WHERE id = 1")

      await wait(500)

      const itemA = await queryItem(nodeA.conn, 1)
      const itemB = await queryItem(nodeB.conn, 1)
      expect(itemA).toBeDefined()
      expect(itemB).toBeDefined()
      expect(itemA?.name).toBe(itemB?.name)
      expect(itemA?.value).toBe(itemB?.value)
    })
  })

  describe('three-node majority writeConcern', () => {
    it('resolves after majority of peers ACK', async () => {
      const nodeA = await createNode(NODE_A)
      const nodeB = await createNode(NODE_B)
      const nodeC = await createNode(NODE_C)

      await nodeA.engine.start()
      await nodeB.engine.start()
      await nodeC.engine.start()

      await wait(50)

      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'majority-write', 999)", undefined, {
        writeConcern: { level: 'majority', timeoutMs: 5000 },
      })

      const status = nodeA.engine.status()
      expect(status.replicating).toBe(true)
    })
  })

  describe('idempotency', () => {
    it('does not duplicate rows when the sender loop retries delivery windows', async () => {
      const nodeA = await createNode(NODE_A)
      const nodeB = await createNode(NODE_B)

      await nodeA.engine.start()
      await nodeB.engine.start()

      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'unique', 42)")

      await wait(300)

      const countBefore = await queryItems(nodeB.conn)
      expect(countBefore).toHaveLength(1)

      await wait(300)

      const countAfter = await queryItems(nodeB.conn)
      expect(countAfter).toHaveLength(1)
      expect(countAfter[0].name).toBe('unique')
    })
  })

  describe('transport disconnect and reconnect', () => {
    it('syncs new writes made after reconnection', async () => {
      const nodeA = await createNode(NODE_A)
      const nodeB = await createNode(NODE_B)

      await nodeA.engine.start()
      await nodeB.engine.start()

      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (1, 'before-disconnect', 10)")
      await wait(300)

      const itemsBefore = await queryItems(nodeB.conn)
      expect(itemsBefore).toHaveLength(1)
      expect(itemsBefore[0].name).toBe('before-disconnect')

      await nodeB.transport.disconnect()
      await wait(50)

      const itemsDuring = await queryItems(nodeB.conn)
      expect(itemsDuring).toHaveLength(1)

      await nodeB.transport.connect(NODE_B, {})

      await nodeA.engine.execute("INSERT INTO items (id, name, value) VALUES (3, 'after-reconnect', 30)")
      await wait(400)

      const itemsAfter = await queryItems(nodeB.conn)
      expect(itemsAfter).toHaveLength(2)
      const names = itemsAfter.map(i => i.name)
      expect(names).toContain('before-disconnect')
      expect(names).toContain('after-reconnect')
    })
  })
})
