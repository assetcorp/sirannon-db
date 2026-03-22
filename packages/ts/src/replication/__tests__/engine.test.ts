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
import { MultiPrimaryTopology } from '../topology/multi-primary.js'
import { PrimaryReplicaTopology } from '../topology/primary-replica.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  RaftMessage,
  ReplicationAck,
  ReplicationBatch,
  ReplicationConfig,
  ReplicationTransport,
  TransportConfig,
} from '../types.js'

class MockTransport implements ReplicationTransport {
  private batchHandler: ((batch: ReplicationBatch, from: string) => Promise<void>) | null = null
  private ackHandler: ((ack: ReplicationAck, from: string) => void) | null = null
  private forwardHandler:
    | ((req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>)
    | null = null
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
  async sendRaftMessage(_peerId: string, _message: RaftMessage): Promise<void> {}
  async broadcastRaftMessage(_message: RaftMessage): Promise<void> {}
  onRaftMessage(_handler: (message: RaftMessage, fromPeerId: string) => void): void {}
  onPeerConnected(handler: (peer: NodeInfo) => void): void {
    this.peerConnectedHandler = handler
  }
  onPeerDisconnected(handler: (peerId: string) => void): void {
    this.peerDisconnectedHandler = handler
  }

  peers(): ReadonlyMap<string, NodeInfo> {
    return this._peers
  }

  addPeer(id: string, role: 'primary' | 'replica' | 'peer' = 'peer'): void {
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
      topology: new MultiPrimaryTopology(),
      transport,
      batchIntervalMs: 50,
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

      const engine = new ReplicationEngine(db, conn, makeConfig())
      await engine.start()
      transport.addPeer(NODE_B)

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
      const checksum = createHash('sha256').update(JSON.stringify(changes)).digest('hex')

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

      const engine = new ReplicationEngine(db, conn, makeConfig({ maxClockDriftMs: 1000 }))
      await engine.start()

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
      const checksum = createHash('sha256').update(JSON.stringify(changes)).digest('hex')

      await expect(
        transport.triggerBatchReceived(
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
        ),
      ).rejects.toThrow(BatchValidationError)

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

      const engine = new ReplicationEngine(
        db,
        conn,
        makeConfig({ topology: new PrimaryReplicaTopology('replica') }),
      )
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
})
