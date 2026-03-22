import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  RaftMessage,
  ReplicationAck,
  ReplicationBatch,
} from '../../../replication/types.js'
import { WebSocketReplicationTransport } from '../index.js'

function makeBatch(overrides?: Partial<ReplicationBatch>): ReplicationBatch {
  return {
    sourceNodeId: 'node-a',
    batchId: 'batch-1',
    fromSeq: 1n,
    toSeq: 5n,
    hlcRange: { min: '1-0-node-a', max: '5-0-node-a' },
    changes: [],
    checksum: 'abc123',
    ...overrides,
  }
}

function makeAck(overrides?: Partial<ReplicationAck>): ReplicationAck {
  return {
    batchId: 'batch-1',
    ackedSeq: 5n,
    nodeId: 'node-b',
    ...overrides,
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

describe('WebSocketReplicationTransport', () => {
  let transportA: WebSocketReplicationTransport
  let transportB: WebSocketReplicationTransport

  beforeEach(async () => {
    transportA = new WebSocketReplicationTransport({
      port: 0,
      reconnectInitialDelay: 50,
      reconnectMaxDelay: 200,
    })
    transportB = new WebSocketReplicationTransport({
      port: 0,
      reconnectInitialDelay: 50,
      reconnectMaxDelay: 200,
    })

    await transportA.connect('node-a', {})
  })

  afterEach(async () => {
    await transportB.disconnect()
    await transportA.disconnect()
  })

  async function connectBtoA(): Promise<void> {
    const portA = transportA.listeningPort
    await transportB.connect('node-b', {
      endpoints: [`http://127.0.0.1:${portA}`],
    })
    await sleep(200)
  }

  describe('peer connection and hello exchange', () => {
    it('establishes bidirectional peer connection', async () => {
      await connectBtoA()

      expect(transportA.peers().size).toBe(1)
      expect(transportA.peers().has('node-b')).toBe(true)
    })

    it('notifies on peer connect via handler', async () => {
      const connected: string[] = []
      transportA.onPeerConnected(peer => {
        connected.push(peer.id)
      })

      await connectBtoA()

      expect(connected).toContain('node-b')
    })
  })

  describe('batch exchange', () => {
    it('sends and receives batches between peers', async () => {
      await connectBtoA()

      const received: { batch: ReplicationBatch; from: string }[] = []
      transportA.onBatchReceived(async (batch, fromPeerId) => {
        received.push({ batch, from: fromPeerId })
      })

      const batch = makeBatch({ sourceNodeId: 'node-b', batchId: 'ws-batch-1' })
      await transportB.send('node-a', batch)
      await sleep(100)

      expect(received).toHaveLength(1)
      expect(received[0].batch.batchId).toBe('ws-batch-1')
      expect(received[0].from).toBe('node-b')
    })

    it('preserves bigint values through serialization', async () => {
      await connectBtoA()

      const received: ReplicationBatch[] = []
      transportA.onBatchReceived(async batch => {
        received.push(batch)
      })

      await transportB.send('node-a', makeBatch({ fromSeq: 999999n, toSeq: 1000000n }))
      await sleep(100)

      expect(received).toHaveLength(1)
      expect(received[0].fromSeq).toBe(999999n)
      expect(received[0].toSeq).toBe(1000000n)
    })
  })

  describe('ack exchange', () => {
    it('sends and receives acks', async () => {
      await connectBtoA()

      const received: { ack: ReplicationAck; from: string }[] = []
      transportA.onAckReceived((ack, fromPeerId) => {
        received.push({ ack, from: fromPeerId })
      })

      await transportB.sendAck('node-a', makeAck({ ackedSeq: 42n }))
      await sleep(100)

      expect(received).toHaveLength(1)
      expect(received[0].ack.ackedSeq).toBe(42n)
      expect(received[0].from).toBe('node-b')
    })
  })

  describe('forward request/response', () => {
    it('forwards and receives results', async () => {
      await connectBtoA()

      transportA.onForwardReceived(async (request, _fromPeerId) => {
        return {
          results: request.statements.map(() => ({ changes: 1, lastInsertRowId: 99 })),
          requestId: request.requestId,
        } satisfies ForwardedTransactionResult
      })

      const request: ForwardedTransaction = {
        requestId: 'ws-fwd-1',
        statements: [{ sql: 'INSERT INTO t VALUES (1)' }],
      }

      const result = await transportB.forward('node-a', request)
      expect(result.requestId).toBe('ws-fwd-1')
      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)
    })
  })

  describe('raft messages', () => {
    it('sends and receives raft messages', async () => {
      await connectBtoA()

      const received: { msg: RaftMessage; from: string }[] = []
      transportA.onRaftMessage((message, fromPeerId) => {
        received.push({ msg: message, from: fromPeerId })
      })

      const raftMsg: RaftMessage = { type: 'request_vote', term: 5, candidateId: 'node-b' }
      await transportB.sendRaftMessage('node-a', raftMsg)
      await sleep(100)

      expect(received).toHaveLength(1)
      expect(received[0].msg.type).toBe('request_vote')
      expect(received[0].msg.term).toBe(5)
      expect(received[0].from).toBe('node-b')
    })
  })

  describe('malformed message handling', () => {
    it('drops malformed messages without crashing', async () => {
      await connectBtoA()

      const received: ReplicationBatch[] = []
      transportA.onBatchReceived(async batch => {
        received.push(batch)
      })

      const portA = transportA.listeningPort
      const ws = new WebSocket(`ws://127.0.0.1:${portA}/replication`)

      await new Promise<void>(resolve => {
        ws.onopen = () => {
          ws.send(JSON.stringify({ type: 'hello', payload: { nodeId: 'bad-node', role: 'peer' } }))
          setTimeout(() => {
            ws.send('this is not json at all')
            ws.send(JSON.stringify({ type: 'batch', payload: 'not-a-real-batch' }))
            ws.send(JSON.stringify({ noTypeField: true }))
            resolve()
          }, 50)
        }
      })

      await sleep(100)

      expect(received).toHaveLength(0)

      ws.close()
      await sleep(50)
    })
  })

  describe('disconnect behavior', () => {
    it('throws when sending after disconnect', async () => {
      await connectBtoA()
      await transportB.disconnect()
      await expect(transportB.send('node-a', makeBatch())).rejects.toThrow('Transport is not connected')
    })

    it('notifies peers on disconnect', async () => {
      await connectBtoA()

      const disconnected: string[] = []
      transportA.onPeerDisconnected(peerId => {
        disconnected.push(peerId)
      })

      await transportB.disconnect()
      await sleep(500)

      expect(disconnected).toContain('node-b')
    })
  })

  describe('large payload handling', () => {
    it('handles large batches within max payload', async () => {
      await connectBtoA()

      const received: ReplicationBatch[] = []
      transportA.onBatchReceived(async batch => {
        received.push(batch)
      })

      const largeChanges = Array.from({ length: 100 }, (_, i) => ({
        table: 'users',
        operation: 'insert' as const,
        rowId: `row-${i}`,
        primaryKey: { id: i },
        hlc: `${i}-0-node-b`,
        txId: `tx-${i}`,
        nodeId: 'node-b',
        newData: { id: i, name: `user-${i}`, data: 'x'.repeat(1000) },
        oldData: null,
      }))

      const batch = makeBatch({ changes: largeChanges, batchId: 'large-batch' })
      await transportB.send('node-a', batch)
      await sleep(200)

      expect(received).toHaveLength(1)
      expect(received[0].changes).toHaveLength(100)
    })
  })

  describe('authentication', () => {
    let authTransportA: WebSocketReplicationTransport
    let authTransportB: WebSocketReplicationTransport

    afterEach(async () => {
      await authTransportB?.disconnect()
      await authTransportA?.disconnect()
    })

    it('rejects peer with wrong authToken', async () => {
      authTransportA = new WebSocketReplicationTransport({
        port: 0,
        authToken: 'correct-secret-token',
      })
      authTransportB = new WebSocketReplicationTransport({
        port: 0,
        authToken: 'wrong-secret-token',
      })

      await authTransportA.connect('auth-node-a', {})
      const portA = authTransportA.listeningPort

      await authTransportB.connect('auth-node-b', {
        endpoints: [`http://127.0.0.1:${portA}`],
      })

      await sleep(300)

      expect(authTransportA.peers().size).toBe(0)
    })

    it('accepts peer with correct authToken', async () => {
      authTransportA = new WebSocketReplicationTransport({
        port: 0,
        authToken: 'shared-secret-token',
      })
      authTransportB = new WebSocketReplicationTransport({
        port: 0,
        authToken: 'shared-secret-token',
      })

      await authTransportA.connect('auth-node-a', {})
      const portA = authTransportA.listeningPort

      await authTransportB.connect('auth-node-b', {
        endpoints: [`http://127.0.0.1:${portA}`],
      })

      await sleep(300)

      expect(authTransportA.peers().size).toBe(1)
      expect(authTransportA.peers().has('auth-node-b')).toBe(true)
    })

    it('allows connection when no authToken configured', async () => {
      authTransportA = new WebSocketReplicationTransport({ port: 0 })
      authTransportB = new WebSocketReplicationTransport({ port: 0 })

      await authTransportA.connect('auth-node-a', {})
      const portA = authTransportA.listeningPort

      await authTransportB.connect('auth-node-b', {
        endpoints: [`http://127.0.0.1:${portA}`],
      })

      await sleep(300)

      expect(authTransportA.peers().size).toBe(1)
    })

    it('rejects peer when server has authToken but client sends none', async () => {
      authTransportA = new WebSocketReplicationTransport({
        port: 0,
        authToken: 'server-only-token',
      })
      authTransportB = new WebSocketReplicationTransport({ port: 0 })

      await authTransportA.connect('auth-node-a', {})
      const portA = authTransportA.listeningPort

      await authTransportB.connect('auth-node-b', {
        endpoints: [`http://127.0.0.1:${portA}`],
      })

      await sleep(300)

      expect(authTransportA.peers().size).toBe(0)
    })
  })
})
