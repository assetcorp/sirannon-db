import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { testDriver } from '../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../core/cdc/change-tracker.js'
import { Database } from '../../core/database.js'
import type { SQLiteConnection } from '../../core/driver/types.js'
import { ReplicationEngine } from '../engine.js'
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
  private ackHandler: ((ack: ReplicationAck, from: string) => void) | null = null
  private peerConnectedHandler: ((peer: NodeInfo) => void) | null = null
  private peerDisconnectedHandler: ((peerId: string) => void) | null = null

  private readonly _peers = new Map<string, NodeInfo>()
  readonly sentBatches: Array<{ peerId: string; batch: ReplicationBatch }> = []
  connected = false
  sendShouldFail = false
  sendError = new Error('transport send failed')

  async connect(_localNodeId: string, _config: TransportConfig): Promise<void> {
    this.connected = true
  }
  async disconnect(): Promise<void> {
    this.connected = false
  }
  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    if (this.sendShouldFail) {
      throw this.sendError
    }
    this.sentBatches.push({ peerId, batch })
  }
  async broadcast(_batch: ReplicationBatch): Promise<void> {}
  async sendAck(_peerId: string, _ack: ReplicationAck): Promise<void> {}
  async forward(_peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    return { results: [{ changes: 1, lastInsertRowId: 1 }], requestId: request.requestId }
  }

  onBatchReceived(_handler: (batch: ReplicationBatch, from: string) => Promise<void>): void {}
  onAckReceived(handler: (ack: ReplicationAck, from: string) => void): void {
    this.ackHandler = handler
  }
  onForwardReceived(_handler: (req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>): void {}
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
      const peer = this._peers.get(id)
      if (peer) {
        this.peerConnectedHandler(peer)
      }
    }
  }

  removePeer(id: string): void {
    this._peers.delete(id)
    if (this.peerDisconnectedHandler) {
      this.peerDisconnectedHandler(id)
    }
  }

  triggerAckReceived(ack: ReplicationAck, from: string): void {
    if (this.ackHandler) {
      this.ackHandler(ack, from)
    }
  }
}

const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

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

  it('works without crashing when no error listener is registered', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    transport.addPeer(NODE_B, 'replica')
    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    transport.sendShouldFail = true

    await new Promise(resolve => setTimeout(resolve, 200))

    const status = engine.status()
    expect(status.replicating).toBe(true)

    transport.sendShouldFail = false
    await engine.stop()
  })

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

  it('does not crash the engine when an error listener throws', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    transport.addPeer(NODE_B, 'replica')
    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    transport.sendShouldFail = true

    engine.on('replication-error', () => {
      throw new Error('listener blew up')
    })

    await new Promise(resolve => setTimeout(resolve, 200))

    const status = engine.status()
    expect(status.replicating).toBe(true)

    transport.sendShouldFail = false
    await engine.stop()
  })

  it('emits events on replication-error, not the built-in error event', async () => {
    const { db, conn } = await createDbAndConn('CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)')

    const engine = new ReplicationEngine(db, conn, makeConfig())
    await engine.start()

    transport.addPeer(NODE_B, 'replica')
    await engine.execute("INSERT INTO items VALUES (1, 'a')")

    transport.sendShouldFail = true

    const builtInErrors: Error[] = []
    engine.on('error', (err: Error) => {
      builtInErrors.push(err)
    })

    const replicationErrors: ReplicationErrorEvent[] = []
    engine.on('replication-error', (event: ReplicationErrorEvent) => {
      replicationErrors.push(event)
    })

    await new Promise(resolve => setTimeout(resolve, 200))

    expect(builtInErrors.length).toBe(0)
    expect(replicationErrors.length).toBeGreaterThanOrEqual(1)

    transport.sendShouldFail = false
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
