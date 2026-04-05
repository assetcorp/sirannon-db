import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { generate } from 'selfsigned'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import type {
  ForwardedTransaction,
  NodeInfo,
  ReplicationAck,
  ReplicationBatch,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
} from '../../../replication/types.js'
import { fromColumnValue, GrpcReplicationTransport, toColumnValue } from '../index.js'

function createBatch(overrides: Partial<ReplicationBatch> = {}): ReplicationBatch {
  return {
    sourceNodeId: 'primary-node',
    batchId: `batch-${Date.now()}`,
    fromSeq: 1n,
    toSeq: 5n,
    hlcRange: { min: '2024-01-01T00:00:00.000Z-0000-primary', max: '2024-01-01T00:00:01.000Z-0000-primary' },
    changes: [
      {
        table: 'users',
        operation: 'insert',
        rowId: 'user-1',
        primaryKey: { id: 1 },
        hlc: '2024-01-01T00:00:00.000Z-0000-primary',
        txId: 'tx-1',
        nodeId: 'primary-node',
        newData: { id: 1, name: 'Alice', email: 'alice@example.com' },
        oldData: null,
      },
    ],
    checksum: 'abc123',
    ...overrides,
  }
}

function createAck(overrides: Partial<ReplicationAck> = {}): ReplicationAck {
  return {
    batchId: 'batch-1',
    ackedSeq: 5n,
    nodeId: 'replica-node',
    ...overrides,
  }
}

function waitFor(conditionFn: () => boolean, timeoutMs = 5000, intervalMs = 50): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const start = Date.now()
    const check = () => {
      if (conditionFn()) {
        resolve()
        return
      }
      if (Date.now() - start > timeoutMs) {
        reject(new Error('waitFor timed out'))
        return
      }
      setTimeout(check, intervalMs)
    }
    check()
  })
}

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    for (const t of transports) {
      try {
        await t.disconnect()
      } catch {
        /* already disconnected */
      }
    }
    transports.length = 0
  })

  async function setupPrimaryReplica(
    primaryOpts: ConstructorParameters<typeof GrpcReplicationTransport>[0] = {},
    replicaOpts: ConstructorParameters<typeof GrpcReplicationTransport>[0] = {},
  ) {
    const primary = new GrpcReplicationTransport({
      insecure: true,
      port: 0,
      ...primaryOpts,
    })
    transports.push(primary)

    await primary.connect('primary-node', { localRole: 'primary' })
    const port = primary.getPort()
    expect(port).toBeGreaterThan(0)

    const replica = new GrpcReplicationTransport({
      insecure: true,
      ...replicaOpts,
    })
    transports.push(replica)

    await replica.connect('replica-node', {
      localRole: 'replica',
      endpoints: [`127.0.0.1:${port}`],
    })

    await waitFor(() => primary.peers().size > 0 && replica.peers().size > 0)

    return { primary, replica, port }
  }

  describe('peer connection and hello exchange', () => {
    it('establishes bidirectional peer connection via hello messages', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      expect(primary.peers().size).toBe(1)
      expect(replica.peers().size).toBe(1)

      const primaryPeer = primary.peers().get('replica-node')
      expect(primaryPeer).toBeDefined()
      expect(primaryPeer?.role).toBe('replica')

      const replicaPeer = replica.peers().get('primary-node')
      expect(replicaPeer).toBeDefined()
      expect(replicaPeer?.role).toBe('primary')
    })

    it('fires onPeerConnected handlers on both sides', async () => {
      const primaryConnected: NodeInfo[] = []
      const replicaConnected: NodeInfo[] = []

      const primary = new GrpcReplicationTransport({ insecure: true, port: 0 })
      transports.push(primary)
      primary.onPeerConnected(peer => primaryConnected.push(peer))

      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const replica = new GrpcReplicationTransport({ insecure: true })
      transports.push(replica)
      replica.onPeerConnected(peer => replicaConnected.push(peer))

      await replica.connect('replica-node', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      await waitFor(() => primaryConnected.length > 0 && replicaConnected.length > 0)

      expect(primaryConnected[0]?.id).toBe('replica-node')
      expect(replicaConnected[0]?.id).toBe('primary-node')
    })
  })

  describe('batch send and receive', () => {
    it('sends a batch from primary and receives it on replica', async () => {
      const { primary } = await setupPrimaryReplica()

      const receivedBatches: { batch: ReplicationBatch; from: string }[] = []

      const replica = transports[1]
      if (!replica) throw new Error('replica not created')
      replica.onBatchReceived(async (batch, from) => {
        receivedBatches.push({ batch, from })
      })

      const batch = createBatch()
      await primary.send('replica-node', batch)

      await waitFor(() => receivedBatches.length > 0)

      const received = receivedBatches[0]
      if (!received) throw new Error('no batch received')
      expect(received.from).toBe('primary-node')
      expect(received.batch.batchId).toBe(batch.batchId)
      expect(received.batch.fromSeq).toBe(1n)
      expect(received.batch.toSeq).toBe(5n)
      expect(received.batch.checksum).toBe('abc123')
      expect(received.batch.changes.length).toBe(1)
    })

    it('preserves bigint values through protobuf serialisation', async () => {
      const { primary } = await setupPrimaryReplica()

      const receivedBatches: ReplicationBatch[] = []
      const replica = transports[1]
      if (!replica) throw new Error('replica not created')
      replica.onBatchReceived(async batch => {
        receivedBatches.push(batch)
      })

      const batch = createBatch({
        fromSeq: 9007199254740993n,
        toSeq: 9007199254740999n,
      })
      await primary.send('replica-node', batch)

      await waitFor(() => receivedBatches.length > 0)

      const received = receivedBatches[0]
      if (!received) throw new Error('no batch received')
      expect(typeof received.fromSeq).toBe('bigint')
      expect(received.fromSeq).toBe(9007199254740993n)
      expect(received.toSeq).toBe(9007199254740999n)
    })
  })

  describe('ack exchange', () => {
    it('sends ack from replica and receives it on primary', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      const receivedAcks: { ack: ReplicationAck; from: string }[] = []
      primary.onAckReceived((ack, from) => {
        receivedAcks.push({ ack, from })
      })

      const ack = createAck()
      await replica.sendAck('primary-node', ack)

      await waitFor(() => receivedAcks.length > 0)

      const received = receivedAcks[0]
      if (!received) throw new Error('no ack received')
      expect(received.from).toBe('replica-node')
      expect(received.ack.batchId).toBe('batch-1')
      expect(received.ack.ackedSeq).toBe(5n)
      expect(typeof received.ack.ackedSeq).toBe('bigint')
    })
  })

  describe('forward request/response', () => {
    it('forwards a write request from replica to primary with deadline', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      primary.onForwardReceived(async (request, _from) => {
        return {
          requestId: request.requestId,
          results: request.statements.map(() => ({
            changes: 1,
            lastInsertRowId: 42,
          })),
        }
      })

      const request: ForwardedTransaction = {
        requestId: 'fwd-1',
        statements: [{ sql: 'INSERT INTO users (name) VALUES (:name)', params: { ':name': 'Bob' } }],
      }

      const result = await replica.forward('primary-node', request)
      expect(result.requestId).toBe('fwd-1')
      expect(result.results.length).toBe(1)
      expect(result.results[0]?.changes).toBe(1)
      expect(result.results[0]?.lastInsertRowId).toBe(42)
    })

    it('propagates forward errors via the error field', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      primary.onForwardReceived(async () => {
        throw new Error('constraint violation: UNIQUE')
      })

      const request: ForwardedTransaction = {
        requestId: 'fwd-err',
        statements: [{ sql: 'INSERT INTO users (id) VALUES (1)' }],
      }

      await expect(replica.forward('primary-node', request)).rejects.toThrow(
        'Forward RPC error: constraint violation: UNIQUE',
      )
    })
  })

  describe('disconnect behaviour', () => {
    it('notifies peers on disconnect', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      const disconnectedPeers: string[] = []
      primary.onPeerDisconnected(peerId => {
        disconnectedPeers.push(peerId)
      })

      await replica.disconnect()

      await waitFor(() => disconnectedPeers.length > 0, 10_000)

      expect(disconnectedPeers).toContain('replica-node')
    })

    it('throws TransportError when sending after disconnect', async () => {
      const { primary } = await setupPrimaryReplica()
      await primary.disconnect()

      await expect(primary.send('replica-node', createBatch())).rejects.toThrow('Transport is not connected')
    })

    it('throws when connecting twice without disconnecting', async () => {
      const transport = new GrpcReplicationTransport({ insecure: true, port: 0 })
      transports.push(transport)
      await transport.connect('node-1', { localRole: 'primary' })

      await expect(transport.connect('node-1', { localRole: 'primary' })).rejects.toThrow(
        'Transport is already connected',
      )
    })
  })

  describe('large payload handling', () => {
    it('transfers 100 changes with 1KB data each', async () => {
      const { primary } = await setupPrimaryReplica()

      const receivedBatches: ReplicationBatch[] = []
      const replica = transports[1]
      if (!replica) throw new Error('replica not created')
      replica.onBatchReceived(async batch => {
        receivedBatches.push(batch)
      })

      const largeData = 'x'.repeat(1024)
      const changes = Array.from({ length: 100 }, (_, i) => ({
        table: 'documents',
        operation: 'insert' as const,
        rowId: `doc-${i}`,
        primaryKey: { id: i },
        hlc: `2024-01-01T00:00:00.${String(i).padStart(3, '0')}Z-0000-primary`,
        txId: `tx-${i}`,
        nodeId: 'primary-node',
        newData: { id: i, content: largeData, index: i },
        oldData: null,
      }))

      const batch = createBatch({
        changes,
        fromSeq: 1n,
        toSeq: 100n,
      })

      await primary.send('replica-node', batch)

      await waitFor(() => receivedBatches.length > 0, 10_000)

      const received = receivedBatches[0]
      if (!received) throw new Error('no batch received')
      expect(received.changes.length).toBe(100)
      const firstChange = received.changes[0]
      if (!firstChange) throw new Error('no change')
      expect((firstChange.newData as Record<string, unknown>)?.content).toBe(largeData)
    })
  })

  describe('insecure mode', () => {
    it('connects in insecure mode without TLS configuration', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      expect(primary.peers().size).toBe(1)
      expect(replica.peers().size).toBe(1)
    })
  })

  describe('stream-per-peer invariant', () => {
    it('replaces old stream when a new connection opens for the same peer', async () => {
      const primary = new GrpcReplicationTransport({ insecure: true, port: 0 })
      transports.push(primary)

      const connectedPeers: string[] = []
      const disconnectedPeers: string[] = []
      primary.onPeerConnected(peer => connectedPeers.push(peer.id))
      primary.onPeerDisconnected(peerId => disconnectedPeers.push(peerId))

      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const replica1 = new GrpcReplicationTransport({ insecure: true })
      transports.push(replica1)
      await replica1.connect('replica-node', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      await waitFor(() => connectedPeers.length >= 1)
      expect(connectedPeers).toContain('replica-node')

      await replica1.disconnect()
      await waitFor(() => disconnectedPeers.length >= 1, 10_000)

      const replica2 = new GrpcReplicationTransport({ insecure: true })
      transports.push(replica2)
      await replica2.connect('replica-node', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      await waitFor(() => connectedPeers.length >= 2)
      expect(primary.peers().has('replica-node')).toBe(true)
    })
  })

  describe('ColumnValue round-trip', () => {
    it('converts null correctly', () => {
      const cv = toColumnValue(null)
      expect(cv.nullValue).toBe(true)
      expect(fromColumnValue(cv)).toBeNull()
    })

    it('converts undefined correctly', () => {
      const cv = toColumnValue(undefined)
      expect(cv.nullValue).toBe(true)
      expect(fromColumnValue(cv)).toBeNull()
    })

    it('converts string correctly', () => {
      const cv = toColumnValue('hello world')
      expect(cv.stringValue).toBe('hello world')
      expect(fromColumnValue(cv)).toBe('hello world')
    })

    it('converts integer correctly', () => {
      const cv = toColumnValue(42)
      expect(cv.intValue).toBe(42n)
      expect(fromColumnValue(cv)).toBe(42n)
    })

    it('converts float correctly', () => {
      const cv = toColumnValue(3.14)
      expect(cv.floatValue).toBe(3.14)
      expect(fromColumnValue(cv)).toBe(3.14)
    })

    it('converts bigint correctly', () => {
      const cv = toColumnValue(9007199254740993n)
      expect(cv.intValue).toBe(9007199254740993n)
      expect(fromColumnValue(cv)).toBe(9007199254740993n)
    })

    it('converts boolean true correctly', () => {
      const cv = toColumnValue(true)
      expect(cv.boolValue).toBe(true)
      expect(fromColumnValue(cv)).toBe(true)
    })

    it('converts boolean false correctly', () => {
      const cv = toColumnValue(false)
      expect(cv.boolValue).toBe(false)
      expect(fromColumnValue(cv)).toBe(false)
    })

    it('converts Uint8Array correctly', () => {
      const data = new Uint8Array([0x01, 0x02, 0x03, 0xff])
      const cv = toColumnValue(data)
      expect(cv.blobValue).toBeDefined()
      const result = fromColumnValue(cv)
      expect(Buffer.isBuffer(result)).toBe(true)
      expect(Buffer.compare(result as Buffer, Buffer.from(data))).toBe(0)
    })

    it('converts Buffer correctly', () => {
      const data = Buffer.from([0xde, 0xad, 0xbe, 0xef])
      const cv = toColumnValue(data)
      expect(cv.blobValue).toBeDefined()
      const result = fromColumnValue(cv)
      expect(Buffer.isBuffer(result)).toBe(true)
      expect(Buffer.compare(result as Buffer, data)).toBe(0)
    })

    it('throws TypeError for unsupported types', () => {
      expect(() => toColumnValue(new Date())).toThrow(TypeError)
      expect(() => toColumnValue(Symbol('test'))).toThrow(TypeError)
      expect(() => toColumnValue({ nested: true })).toThrow(TypeError)
      expect(() => toColumnValue([1, 2, 3])).toThrow(TypeError)
    })
  })

  describe('broadcast', () => {
    it('broadcasts a batch to all connected peers', async () => {
      const primary = new GrpcReplicationTransport({ insecure: true, port: 0 })
      transports.push(primary)
      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const receivedOnR1: ReplicationBatch[] = []
      const receivedOnR2: ReplicationBatch[] = []

      const r1 = new GrpcReplicationTransport({ insecure: true })
      transports.push(r1)
      r1.onBatchReceived(async batch => {
        receivedOnR1.push(batch)
      })
      await r1.connect('replica-1', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      const r2 = new GrpcReplicationTransport({ insecure: true })
      transports.push(r2)
      r2.onBatchReceived(async batch => {
        receivedOnR2.push(batch)
      })
      await r2.connect('replica-2', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      await waitFor(() => primary.peers().size >= 2)

      const batch = createBatch()
      await primary.broadcast(batch)

      await waitFor(() => receivedOnR1.length > 0 && receivedOnR2.length > 0)

      expect(receivedOnR1[0]?.batchId).toBe(batch.batchId)
      expect(receivedOnR2[0]?.batchId).toBe(batch.batchId)
    })
  })

  describe('mTLS authentication', () => {
    let certDir: string
    let caCertPath: string
    let serverCertPath: string
    let serverKeyPath: string
    let clientCertPath: string
    let clientKeyPath: string
    let wrongCertPath: string
    let wrongKeyPath: string

    beforeAll(async () => {
      certDir = mkdtempSync(join(tmpdir(), 'sirannon-grpc-tls-'))
      const tomorrow = new Date(Date.now() + 86_400_000)

      const caAttrs = [{ name: 'commonName', value: 'Sirannon Test CA' }]
      const caResult = await generate(caAttrs, {
        keySize: 2048,
        notAfterDate: tomorrow,
        extensions: [
          { name: 'basicConstraints', cA: true },
          { name: 'keyUsage', keyCertSign: true, cRLSign: true },
        ],
      })

      caCertPath = join(certDir, 'ca.pem')
      writeFileSync(caCertPath, caResult.cert)

      const serverAttrs = [{ name: 'commonName', value: 'primary-node' }]
      const serverResult = await generate(serverAttrs, {
        keySize: 2048,
        notAfterDate: tomorrow,
        extensions: [
          {
            name: 'subjectAltName',
            altNames: [
              { type: 2, value: 'localhost' },
              { type: 7, ip: '127.0.0.1' },
            ],
          },
        ],
        ca: { key: caResult.private, cert: caResult.cert },
      })

      serverCertPath = join(certDir, 'server.pem')
      serverKeyPath = join(certDir, 'server-key.pem')
      writeFileSync(serverCertPath, serverResult.cert)
      writeFileSync(serverKeyPath, serverResult.private)

      const clientAttrs = [{ name: 'commonName', value: 'replica-node' }]
      const clientResult = await generate(clientAttrs, {
        keySize: 2048,
        notAfterDate: tomorrow,
        extensions: [
          {
            name: 'subjectAltName',
            altNames: [
              { type: 2, value: 'localhost' },
              { type: 7, ip: '127.0.0.1' },
            ],
          },
        ],
        ca: { key: caResult.private, cert: caResult.cert },
      })

      clientCertPath = join(certDir, 'client.pem')
      clientKeyPath = join(certDir, 'client-key.pem')
      writeFileSync(clientCertPath, clientResult.cert)
      writeFileSync(clientKeyPath, clientResult.private)

      const wrongResult = await generate([{ name: 'commonName', value: 'wrong-node' }], {
        keySize: 2048,
        notAfterDate: tomorrow,
      })
      wrongCertPath = join(certDir, 'wrong.pem')
      wrongKeyPath = join(certDir, 'wrong-key.pem')
      writeFileSync(wrongCertPath, wrongResult.cert)
      writeFileSync(wrongKeyPath, wrongResult.private)
    })

    afterAll(() => {
      try {
        rmSync(certDir, { recursive: true, force: true })
      } catch {
        /* best-effort cleanup */
      }
    })

    it('connects with valid mTLS certificates', async () => {
      const primary = new GrpcReplicationTransport({
        port: 0,
        tlsCert: serverCertPath,
        tlsKey: serverKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(primary)

      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const replica = new GrpcReplicationTransport({
        tlsCert: clientCertPath,
        tlsKey: clientKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(replica)

      await replica.connect('replica-node', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      await waitFor(() => primary.peers().size > 0 && replica.peers().size > 0, 10_000)

      expect(primary.peers().has('replica-node')).toBe(true)
      expect(replica.peers().has('primary-node')).toBe(true)
    })

    it('rejects connection with wrong certificate', async () => {
      const primary = new GrpcReplicationTransport({
        port: 0,
        tlsCert: serverCertPath,
        tlsKey: serverKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(primary)

      await primary.connect('primary-node', { localRole: 'primary' })
      const port = primary.getPort()

      const wrongReplica = new GrpcReplicationTransport({
        tlsCert: wrongCertPath,
        tlsKey: wrongKeyPath,
        tlsCaCert: caCertPath,
      })
      transports.push(wrongReplica)

      await wrongReplica.connect('wrong-replica', {
        localRole: 'replica',
        endpoints: [`127.0.0.1:${port}`],
      })

      await new Promise(resolve => setTimeout(resolve, 2000))

      expect(primary.peers().has('wrong-replica')).toBe(false)
    })
  })

  describe('sync operations', () => {
    it('sends sync request and receives sync batch and complete', async () => {
      const { primary, replica } = await setupPrimaryReplica()

      const receivedSyncRequests: { req: SyncRequest; from: string }[] = []
      const receivedSyncBatches: { batch: SyncBatch; from: string }[] = []
      const receivedSyncCompletes: { complete: SyncComplete; from: string }[] = []
      const receivedSyncAcks: { ack: SyncAck; from: string }[] = []

      primary.onSyncRequested(async (req, from) => {
        receivedSyncRequests.push({ req, from })

        await primary.sendSyncBatch('replica-node', {
          requestId: req.requestId,
          table: 'users',
          batchIndex: 0,
          rows: [{ id: 1, name: 'Alice' }],
          schema: ['id', 'name'],
          checksum: 'sync-check-1',
          isLastBatchForTable: true,
        })

        await primary.sendSyncComplete('replica-node', {
          requestId: req.requestId,
          snapshotSeq: 100n,
          manifests: [{ table: 'users', rowCount: 1, pkHash: 'hash-1' }],
        })
      })

      replica.onSyncBatchReceived(async (batch, from) => {
        receivedSyncBatches.push({ batch, from })
      })

      replica.onSyncCompleteReceived(async (complete, from) => {
        receivedSyncCompletes.push({ complete, from })
      })

      primary.onSyncAckReceived((ack, from) => {
        receivedSyncAcks.push({ ack, from })
      })

      await replica.requestSync('primary-node', {
        requestId: 'sync-1',
        joinerNodeId: 'replica-node',
        completedTables: [],
      })

      await waitFor(() => receivedSyncRequests.length > 0, 5000)
      await waitFor(() => receivedSyncBatches.length > 0 && receivedSyncCompletes.length > 0, 5000)

      expect(receivedSyncRequests[0]?.req.requestId).toBe('sync-1')
      expect(receivedSyncBatches[0]?.batch.table).toBe('users')
      expect(receivedSyncBatches[0]?.batch.rows[0]).toEqual({ id: 1n, name: 'Alice' })
      expect(receivedSyncCompletes[0]?.complete.snapshotSeq).toBe(100n)

      await replica.sendSyncAck('primary-node', {
        requestId: 'sync-1',
        joinerNodeId: 'replica-node',
        table: 'users',
        batchIndex: 0,
        success: true,
      })

      await waitFor(() => receivedSyncAcks.length > 0)
      expect(receivedSyncAcks[0]?.ack.requestId).toBe('sync-1')
      expect(receivedSyncAcks[0]?.ack.success).toBe(true)
    })
  })
})
