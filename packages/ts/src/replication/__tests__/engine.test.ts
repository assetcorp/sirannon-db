import { createHash } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { ReplicationEngine } from '../engine.js'
import { BatchValidationError, ReplicationError, TopologyError } from '../errors.js'
import { HLC } from '../hlc.js'
import { canonicaliseForChecksum } from '../log.js'
import { PrimaryReplicaTopology } from '../topology/primary-replica.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  ReplicationAck,
  ReplicationBatch,
  ReplicationConfig,
  ReplicationErrorEvent,
  ReplicationTransport,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  TransportConfig,
} from '../types.js'

class MockTransport implements ReplicationTransport {
  private batchHandler: ((batch: ReplicationBatch, from: string) => Promise<void>) | null = null
  private ackHandler: ((ack: ReplicationAck, from: string) => void) | null = null
  private forwardHandler: ((req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>) | null =
    null
  private peerConnectedHandler: ((peer: NodeInfo) => void) | null = null
  private peerDisconnectedHandler: ((peerId: string) => void) | null = null

  private readonly _peers = new Map<string, NodeInfo>()
  readonly sentBatches: Array<{ peerId: string; batch: ReplicationBatch }> = []
  readonly sentAcks: Array<{ peerId: string; ack: ReplicationAck }> = []
  connected = false

  async connect(_localNodeId: string, _config: TransportConfig): Promise<void> {
    this.connected = true
  }
  async disconnect(): Promise<void> {
    this.connected = false
  }
  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.sentBatches.push({ peerId, batch })
  }
  async broadcast(_batch: ReplicationBatch): Promise<void> {}
  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.sentAcks.push({ peerId, ack })
  }
  async forward(_peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    return { results: [{ changes: 1, lastInsertRowId: 1 }], requestId: request.requestId }
  }

  onBatchReceived(handler: (batch: ReplicationBatch, from: string) => Promise<void>): void {
    this.batchHandler = handler
  }
  onAckReceived(handler: (ack: ReplicationAck, from: string) => void): void {
    this.ackHandler = handler
  }
  onForwardReceived(handler: (req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>): void {
    this.forwardHandler = handler
  }
  async requestSync(_peerId: string, _request: SyncRequest): Promise<void> {}
  async sendSyncBatch(_peerId: string, _batch: SyncBatch): Promise<void> {}
  async sendSyncComplete(_peerId: string, _complete: SyncComplete): Promise<void> {}
  async sendSyncAck(_peerId: string, _ack: SyncAck): Promise<void> {}
  onSyncRequested(_handler: (request: SyncRequest, fromPeerId: string) => Promise<void>): void {}
  onSyncBatchReceived(_handler: (batch: SyncBatch, fromPeerId: string) => Promise<void>): void {}
  onSyncCompleteReceived(_handler: (complete: SyncComplete, fromPeerId: string) => Promise<void>): void {}
  onSyncAckReceived(_handler: (ack: SyncAck, fromPeerId: string) => void): void {}
  onPeerConnected(handler: (peer: NodeInfo) => void): void {
    this.peerConnectedHandler = handler
  }
  onPeerDisconnected(handler: (peerId: string) => void): void {
    this.peerDisconnectedHandler = handler
  }

  peers(): ReadonlyMap<string, NodeInfo> {
    return this._peers
  }

  addPeer(id: string, role: 'primary' | 'replica' = 'replica'): void {
    this._peers.set(id, {
      id,
      role,
      joinedAt: Date.now(),
      lastSeenAt: Date.now(),
      lastAckedSeq: 0n,
    })
    if (this.peerConnectedHandler) {
      this.peerConnectedHandler(this._peers.get(id) as NodeInfo)
    }
  }

  removePeer(id: string): void {
    this._peers.delete(id)
    if (this.peerDisconnectedHandler) {
      this.peerDisconnectedHandler(id)
    }
  }

  triggerBatchReceived(batch: ReplicationBatch, from: string): Promise<void> {
    if (this.batchHandler) {
      return this.batchHandler(batch, from)
    }
    return Promise.resolve()
  }

  triggerAckReceived(ack: ReplicationAck, from: string): void {
    if (this.ackHandler) {
      this.ackHandler(ack, from)
    }
  }

  triggerForwardReceived(request: ForwardedTransaction, from: string): Promise<ForwardedTransactionResult> {
    if (this.forwardHandler) {
      return this.forwardHandler(request, from)
    }
    return Promise.reject(new Error('No forward handler registered'))
  }
}

const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

describe('ReplicationEngine', () => {
  let tempDir: string
  let writerConn: SQLiteConnection
  let transport: MockTransport
  const openDbs: Database[] = []

  beforeEach(async () => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-repl-'))
    transport = new MockTransport()
  })

  afterEach(async () => {
    for (const db of openDbs) {
      try {
        if (!db.closed) await db.close()
      } catch {
        /* best-effort */
      }
    }
    openDbs.length = 0

    if (writerConn) {
      try {
        await writerConn.close()
      } catch {
        /* best-effort */
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

  describe('lifecycle', () => {
    it('starts and stops cleanly', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      const status = engine.status()
      expect(status.nodeId).toBe(NODE_A)
      expect(status.replicating).toBe(true)

      await engine.stop()
      expect(engine.status().replicating).toBe(false)
    })
  })

  describe('write routing', () => {
    it('throws TopologyError for writes on non-writable nodes without forwarding', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig({
          topology: new PrimaryReplicaTopology('replica'),
          writeForwarding: false,
        }),
      )
      await engine.start()

      await expect(engine.execute("INSERT INTO users VALUES (1, 'x')")).rejects.toThrow(TopologyError)

      await engine.stop()
    })

    it('forwards writes when writeForwarding is enabled', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      transport.addPeer('primary1', 'primary')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig({
          topology: new PrimaryReplicaTopology('replica'),
          writeForwarding: true,
        }),
      )
      await engine.start()

      const result = await engine.execute("INSERT INTO users VALUES (1, 'x')")
      expect(result.changes).toBe(1)

      await engine.stop()
    })

    it('throws TopologyError for transaction on non-writable node', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig({
          topology: new PrimaryReplicaTopology('replica'),
        }),
      )
      await engine.start()

      await expect(
        engine.transaction(async tx => {
          await tx.execute("INSERT INTO users VALUES (1, 'x')")
        }),
      ).rejects.toThrow(TopologyError)

      await engine.stop()
    })
  })

  describe('batch receiving', () => {
    it('applies incoming batches and sends ACK', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig({ topology: new PrimaryReplicaTopology('replica') }))
      await engine.start()
      transport.addPeer(NODE_B, 'primary')

      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()
      const changes = [
        {
          table: 'users',
          operation: 'insert' as const,
          rowId: '100',
          primaryKey: { id: 100 },
          hlc: hlcVal,
          txId: 'remote-tx',
          nodeId: NODE_B,
          newData: { id: 100, name: 'Remote' },
          oldData: null,
        },
      ]
      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      await transport.triggerBatchReceived(
        {
          sourceNodeId: NODE_B,
          batchId: `${NODE_B}-1-1`,
          fromSeq: 1n,
          toSeq: 1n,
          hlcRange: { min: hlcVal, max: hlcVal },
          changes,
          checksum,
        },
        NODE_B,
      )

      const stmt = await conn.prepare('SELECT name FROM users WHERE id = 100')
      const row = (await stmt.get()) as { name: string } | undefined
      expect(row?.name).toBe('Remote')

      expect(transport.sentAcks.length).toBeGreaterThan(0)

      await engine.stop()
    })

    it('rejects batches with excessive clock drift', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig({ topology: new PrimaryReplicaTopology('replica'), maxClockDriftMs: 1000 }),
      )
      await engine.start()
      transport.addPeer(NODE_B, 'primary')

      const farFutureMs = Date.now() + 100_000
      const wallHex = farFutureMs.toString(16).padStart(12, '0')
      const hlcVal = `${wallHex}-0000-${NODE_B}`

      const changes = [
        {
          table: 'users',
          operation: 'insert' as const,
          rowId: '200',
          primaryKey: { id: 200 },
          hlc: hlcVal,
          txId: 'drift-tx',
          nodeId: NODE_B,
          newData: { id: 200, name: 'Drifted' },
          oldData: null,
        },
      ]
      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      const errorEvents: ReplicationErrorEvent[] = []
      engine.on('replication-error', (event: ReplicationErrorEvent) => {
        errorEvents.push(event)
      })

      await transport.triggerBatchReceived(
        {
          sourceNodeId: NODE_B,
          batchId: `${NODE_B}-50-50`,
          fromSeq: 50n,
          toSeq: 50n,
          hlcRange: { min: hlcVal, max: hlcVal },
          changes,
          checksum,
        },
        NODE_B,
      )

      expect(errorEvents.length).toBe(1)
      expect(errorEvents[0].error).toBeInstanceOf(BatchValidationError)
      expect(errorEvents[0].operation).toBe('batch-received')
      expect(errorEvents[0].peerId).toBe(NODE_B)
      expect(errorEvents[0].recoverable).toBe(true)

      await engine.stop()
    })
  })

  describe('ACK tracking', () => {
    it('updates peer tracker on ACK received', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      transport.addPeer(NODE_B)

      transport.triggerAckReceived({ batchId: 'test-batch', ackedSeq: 5n, nodeId: NODE_B }, NODE_B)

      const status = engine.status()
      const peer = status.peers.find(p => p.nodeId === NODE_B)
      expect(peer?.lastAckedSeq).toBe(5n)

      await engine.stop()
    })
  })

  describe('forwardStatements', () => {
    it('executes locally when the node is writable', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      const result = await engine.forwardStatements([{ sql: "INSERT INTO products (id, name) VALUES (1, 'Widget')" }])

      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)

      await engine.stop()
    })
  })

  describe('forward authorization', () => {
    it('rejects forward from unknown peer', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      await expect(
        transport.triggerForwardReceived(
          { requestId: 'fwd-1', statements: [{ sql: "INSERT INTO items VALUES (1, 'x')" }] },
          'unknown-peer-id',
        ),
      ).rejects.toThrow(ReplicationError)

      await engine.stop()
    })

    it('accepts forward from known peer', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()
      transport.addPeer(NODE_B)

      const result = await transport.triggerForwardReceived(
        { requestId: 'fwd-2', statements: [{ sql: "INSERT INTO items VALUES (1, 'test')" }] },
        NODE_B,
      )

      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)

      await engine.stop()
    })

    it('forward handler not registered on replica', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')

      const engine = new ReplicationEngine(db, conn, makeConfig({ topology: new PrimaryReplicaTopology('replica') }))
      await engine.start()

      await expect(
        transport.triggerForwardReceived(
          { requestId: 'fwd-3', statements: [{ sql: "INSERT INTO items VALUES (1, 'x')" }] },
          NODE_B,
        ),
      ).rejects.toThrow('No forward handler registered')

      await engine.stop()
    })
  })

  describe('observability', () => {
    it('returns 0 from getCurrentSeq before start', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig())

      expect(engine.getCurrentSeq()).toBe(0n)
    })

    it('returns 0 from getAppliedSeq for unknown peers', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      expect(engine.getAppliedSeq('unknown-peer')).toBe(0n)
      expect(engine.getAppliedSeq(NODE_B)).toBe(0n)

      await engine.stop()
    })

    it('advances getCurrentSeq after a local write', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      const before = engine.getCurrentSeq()
      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
      const after = engine.getCurrentSeq()

      expect(after).toBeGreaterThan(before)

      await engine.stop()
    })

    it('reports the same seq for the same write across repeated reads', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      await engine.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
      const first = engine.getCurrentSeq()
      const second = engine.getCurrentSeq()
      const third = engine.getCurrentSeq()

      expect(first).toBe(second)
      expect(second).toBe(third)

      await engine.stop()
    })

    it('updates getAppliedSeq after a remote batch is applied', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig({ topology: new PrimaryReplicaTopology('replica') }))
      await engine.start()
      transport.addPeer(NODE_B, 'primary')

      expect(engine.getAppliedSeq(NODE_B)).toBe(0n)

      const hlcB = new HLC(NODE_B)
      const hlcVal = hlcB.now()
      const changes = [
        {
          table: 'users',
          operation: 'insert' as const,
          rowId: '7',
          primaryKey: { id: 7 },
          hlc: hlcVal,
          txId: 'remote-tx',
          nodeId: NODE_B,
          newData: { id: 7, name: 'Remote' },
          oldData: null,
        },
      ]
      const checksum = createHash('sha256').update(canonicaliseForChecksum(changes)).digest('hex')

      await transport.triggerBatchReceived(
        {
          sourceNodeId: NODE_B,
          batchId: `${NODE_B}-1-1`,
          fromSeq: 1n,
          toSeq: 1n,
          hlcRange: { min: hlcVal, max: hlcVal },
          changes,
          checksum,
        },
        NODE_B,
      )

      expect(engine.getAppliedSeq(NODE_B)).toBe(1n)
      expect(engine.getAppliedSeq('different-peer')).toBe(0n)

      await engine.stop()
    })

    it('does not regress getCurrentSeq if reads are interleaved with writes', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()

      const seqs: bigint[] = []
      for (let i = 1; i <= 5; i++) {
        await engine.execute(`INSERT INTO users (id, name) VALUES (${i}, 'u${i}')`)
        seqs.push(engine.getCurrentSeq())
      }

      for (let i = 1; i < seqs.length; i++) {
        expect(seqs[i]).toBeGreaterThanOrEqual(seqs[i - 1])
      }
      expect(seqs[seqs.length - 1]).toBeGreaterThan(0n)

      await engine.stop()
    })

    it('preserves getCurrentSeq across restart by reading from durable state', async () => {
      const { db, conn } = await createDbAndConn('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

      const engineA = new ReplicationEngine(db, conn, makeConfig())
      await engineA.start()
      await engineA.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")
      const seqA = engineA.getCurrentSeq()
      await engineA.stop()

      expect(seqA).toBeGreaterThan(0n)

      const engineB = new ReplicationEngine(db, conn, makeConfig())
      await engineB.start()
      const seqB = engineB.getCurrentSeq()
      await engineB.stop()

      expect(seqB).toBe(seqA)
    })
  })

  describe('DDL trigger refresh', () => {
    it('captures new columns in CDC after ALTER TABLE ADD COLUMN', async () => {
      const dbPath = join(tempDir, `ddl-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      writerConn = conn

      await conn.exec('CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)')
      const tracker = new ChangeTracker({ replication: true })
      await tracker.watch(conn, 'inventory')

      const db = await Database.create('ddl', dbPath, testDriver)
      openDbs.push(db)

      const engine = new ReplicationEngine(db, conn, makeConfig({ changeTracker: tracker }))
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
      const dbPath = join(tempDir, `ddl-fwd-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)
      const conn = await testDriver.open(dbPath)
      await conn.exec('PRAGMA journal_mode = WAL')
      writerConn = conn

      await conn.exec('CREATE TABLE inventory (id INTEGER PRIMARY KEY, sku TEXT NOT NULL)')
      const tracker = new ChangeTracker({ replication: true })
      await tracker.watch(conn, 'inventory')

      const db = await Database.create('ddl-fwd', dbPath, testDriver)
      openDbs.push(db)

      const engine = new ReplicationEngine(db, conn, makeConfig({ changeTracker: tracker }))
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
