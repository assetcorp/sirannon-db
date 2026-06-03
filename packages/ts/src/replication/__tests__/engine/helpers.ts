import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { testDriver } from '../../../core/__tests__/helpers/test-driver.js'
import { ChangeTracker } from '../../../core/cdc/change-tracker.js'
import { Database } from '../../../core/database.js'
import type { SQLiteConnection } from '../../../core/driver/types.js'
import { PrimaryReplicaTopology } from '../../topology/primary-replica.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  ReplicationAck,
  ReplicationBatch,
  ReplicationConfig,
  ReplicationTransport,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  TransportConfig,
} from '../../types.js'

export class MockTransport implements ReplicationTransport {
  private batchHandler: ((batch: ReplicationBatch, from: string) => Promise<void>) | null = null
  private ackHandler: ((ack: ReplicationAck, from: string) => void) | null = null
  private syncRequestHandler: ((request: SyncRequest, fromPeerId: string) => Promise<void>) | null = null
  private syncBatchHandler: ((batch: SyncBatch, fromPeerId: string) => Promise<void>) | null = null
  private syncCompleteHandler: ((complete: SyncComplete, fromPeerId: string) => Promise<void>) | null = null
  private syncAckHandler: ((ack: SyncAck, fromPeerId: string) => void) | null = null
  private forwardHandler: ((req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>) | null =
    null
  private peerConnectedHandler: ((peer: NodeInfo) => void) | null = null
  private peerDisconnectedHandler: ((peerId: string) => void) | null = null

  private readonly _peers = new Map<string, NodeInfo>()
  readonly sentBatches: Array<{ peerId: string; batch: ReplicationBatch }> = []
  readonly sentAcks: Array<{ peerId: string; ack: ReplicationAck }> = []
  readonly sentSyncRequests: Array<{ peerId: string; request: SyncRequest }> = []
  readonly sentSyncBatches: Array<{ peerId: string; batch: SyncBatch }> = []
  readonly sentSyncCompletes: Array<{ peerId: string; complete: SyncComplete }> = []
  readonly sentSyncAcks: Array<{ peerId: string; ack: SyncAck }> = []
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
  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.sentSyncRequests.push({ peerId, request })
  }
  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.sentSyncBatches.push({ peerId, batch })
  }
  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.sentSyncCompletes.push({ peerId, complete })
  }
  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.sentSyncAcks.push({ peerId, ack })
  }
  onSyncRequested(handler: (request: SyncRequest, fromPeerId: string) => Promise<void>): void {
    this.syncRequestHandler = handler
  }
  onSyncBatchReceived(handler: (batch: SyncBatch, fromPeerId: string) => Promise<void>): void {
    this.syncBatchHandler = handler
  }
  onSyncCompleteReceived(handler: (complete: SyncComplete, fromPeerId: string) => Promise<void>): void {
    this.syncCompleteHandler = handler
  }
  onSyncAckReceived(handler: (ack: SyncAck, fromPeerId: string) => void): void {
    this.syncAckHandler = handler
  }
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

  triggerSyncRequested(request: SyncRequest, from: string): Promise<void> {
    if (this.syncRequestHandler) {
      return this.syncRequestHandler(request, from)
    }
    return Promise.reject(new Error('No sync request handler registered'))
  }

  triggerSyncBatchReceived(batch: SyncBatch, from: string): Promise<void> {
    if (this.syncBatchHandler) {
      return this.syncBatchHandler(batch, from)
    }
    return Promise.reject(new Error('No sync batch handler registered'))
  }

  triggerSyncCompleteReceived(complete: SyncComplete, from: string): Promise<void> {
    if (this.syncCompleteHandler) {
      return this.syncCompleteHandler(complete, from)
    }
    return Promise.reject(new Error('No sync complete handler registered'))
  }

  triggerSyncAckReceived(ack: SyncAck, from: string): void {
    if (this.syncAckHandler) {
      this.syncAckHandler(ack, from)
    }
  }

  triggerForwardReceived(request: ForwardedTransaction, from: string): Promise<ForwardedTransactionResult> {
    if (this.forwardHandler) {
      return this.forwardHandler(request, from)
    }
    return Promise.reject(new Error('No forward handler registered'))
  }
}

export const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
export const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'

export interface EngineTestHarness {
  tempDir: string
  transport: MockTransport
  openDbs: Database[]
  getWriterConn(): SQLiteConnection | undefined
  setWriterConn(conn: SQLiteConnection): void
}

export function createHarness(): EngineTestHarness {
  let writerConn: SQLiteConnection | undefined
  return {
    tempDir: mkdtempSync(join(tmpdir(), 'sirannon-repl-')),
    transport: new MockTransport(),
    openDbs: [],
    getWriterConn: () => writerConn,
    setWriterConn: (conn: SQLiteConnection) => {
      writerConn = conn
    },
  }
}

export async function teardownHarness(harness: EngineTestHarness): Promise<void> {
  for (const db of harness.openDbs) {
    try {
      if (!db.closed) await db.close()
    } catch {
      /* best-effort */
    }
  }
  harness.openDbs.length = 0

  const writerConn = harness.getWriterConn()
  if (writerConn) {
    try {
      await writerConn.close()
    } catch {
      /* best-effort */
    }
  }

  rmSync(harness.tempDir, { recursive: true, force: true })
}

export async function createDbAndConn(
  harness: EngineTestHarness,
  tableSql?: string,
): Promise<{ db: Database; conn: SQLiteConnection }> {
  const dbPath = join(harness.tempDir, `test-${Date.now()}-${Math.random().toString(36).slice(2)}.db`)

  const conn = await testDriver.open(dbPath)
  await conn.exec('PRAGMA journal_mode = WAL')
  harness.setWriterConn(conn)

  if (tableSql) {
    await conn.exec(tableSql)
    const tracker = new ChangeTracker({ replication: true })
    const tableName = tableSql.match(/CREATE TABLE (\w+)/)?.[1]
    if (tableName) {
      await tracker.watch(conn, tableName)
    }
  }

  const db = await Database.create('test', dbPath, testDriver)
  harness.openDbs.push(db)
  return { db, conn }
}

export function makeConfig(transport: MockTransport, overrides: Partial<ReplicationConfig> = {}): ReplicationConfig {
  return {
    nodeId: NODE_A,
    topology: new PrimaryReplicaTopology('primary'),
    transport,
    batchIntervalMs: 50,
    initialSync: false,
    ...overrides,
  }
}
