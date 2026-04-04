import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  ReplicationAck,
  ReplicationBatch,
} from '../../../replication/types.js'
import { InMemoryTransport, MemoryBus } from '../index.js'

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

function flushMicrotasks(): Promise<void> {
  return new Promise(resolve => {
    queueMicrotask(resolve)
  })
}

describe('MemoryBus', () => {
  it('tracks transport count', () => {
    const bus = new MemoryBus()
    const t1 = new InMemoryTransport(bus)
    const t2 = new InMemoryTransport(bus)

    expect(bus.size).toBe(0)
    bus.join('a', t1)
    expect(bus.size).toBe(1)
    bus.join('b', t2)
    expect(bus.size).toBe(2)
    bus.leave('a')
    expect(bus.size).toBe(1)
  })

  it('returns undefined for unknown peer', () => {
    const bus = new MemoryBus()
    expect(bus.getTransport('unknown')).toBeUndefined()
  })
})

describe('InMemoryTransport', () => {
  let bus: MemoryBus
  let transportA: InMemoryTransport
  let transportB: InMemoryTransport

  beforeEach(async () => {
    bus = new MemoryBus()
    transportA = new InMemoryTransport(bus)
    transportB = new InMemoryTransport(bus)
    await transportA.connect('node-a', {})
    await transportB.connect('node-b', {})
  })

  afterEach(async () => {
    await transportA.disconnect()
    await transportB.disconnect()
  })

  describe('batch exchange', () => {
    it('sends and receives a batch between two peers', async () => {
      const received: { batch: ReplicationBatch; from: string }[] = []
      transportB.onBatchReceived(async (batch, fromPeerId) => {
        received.push({ batch, from: fromPeerId })
      })

      const batch = makeBatch()
      await transportA.send('node-b', batch)
      await flushMicrotasks()

      expect(received).toHaveLength(1)
      expect(received[0].batch.batchId).toBe('batch-1')
      expect(received[0].from).toBe('node-a')
    })

    it('broadcasts to all connected peers', async () => {
      const transportC = new InMemoryTransport(bus)
      await transportC.connect('node-c', {})

      const receivedB: ReplicationBatch[] = []
      const receivedC: ReplicationBatch[] = []

      transportB.onBatchReceived(async batch => {
        receivedB.push(batch)
      })
      transportC.onBatchReceived(async batch => {
        receivedC.push(batch)
      })

      await transportA.broadcast(makeBatch())
      await flushMicrotasks()

      expect(receivedB).toHaveLength(1)
      expect(receivedC).toHaveLength(1)

      await transportC.disconnect()
    })
  })

  describe('ack exchange', () => {
    it('sends and receives an ack', async () => {
      const received: { ack: ReplicationAck; from: string }[] = []
      transportA.onAckReceived((ack, fromPeerId) => {
        received.push({ ack, from: fromPeerId })
      })

      await transportB.sendAck('node-a', makeAck())
      await flushMicrotasks()

      expect(received).toHaveLength(1)
      expect(received[0].ack.batchId).toBe('batch-1')
      expect(received[0].from).toBe('node-b')
    })
  })

  describe('forward request/response', () => {
    it('forwards a transaction and receives the result', async () => {
      transportB.onForwardReceived(async (request, _fromPeerId) => {
        return {
          results: request.statements.map(() => ({ changes: 1, lastInsertRowId: 42 })),
          requestId: request.requestId,
        } satisfies ForwardedTransactionResult
      })

      const request: ForwardedTransaction = {
        requestId: 'fwd-1',
        statements: [{ sql: 'INSERT INTO t VALUES (1)' }],
      }

      const result = await transportA.forward('node-b', request)
      expect(result.requestId).toBe('fwd-1')
      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)
    })

    it('throws when no forward handler is registered', async () => {
      const request: ForwardedTransaction = {
        requestId: 'fwd-2',
        statements: [{ sql: 'SELECT 1' }],
      }

      await expect(transportA.forward('node-b', request)).rejects.toThrow('No forward handler registered')
    })
  })

  describe('peer lifecycle', () => {
    it('notifies on peer connect', async () => {
      const bus2 = new MemoryBus()
      const t1 = new InMemoryTransport(bus2)
      await t1.connect('peer-1', {})

      const connectedPeers: string[] = []
      t1.onPeerConnected(peer => {
        connectedPeers.push(peer.id)
      })

      const t2 = new InMemoryTransport(bus2)
      await t2.connect('peer-2', {})

      expect(connectedPeers).toContain('peer-2')

      await t1.disconnect()
      await t2.disconnect()
    })

    it('notifies on peer disconnect', async () => {
      const disconnectedPeers: string[] = []
      transportA.onPeerDisconnected(peerId => {
        disconnectedPeers.push(peerId)
      })

      await transportB.disconnect()

      expect(disconnectedPeers).toContain('node-b')
    })

    it('returns peers map', () => {
      const peers = transportA.peers()
      expect(peers.size).toBe(1)
      expect(peers.has('node-b')).toBe(true)
    })
  })

  describe('async delivery via queueMicrotask', () => {
    it('delivers batch via microtask after send resolves', async () => {
      const deliveryOrder: string[] = []
      transportB.onBatchReceived(async () => {
        deliveryOrder.push('batch-received')
      })

      await transportA.send('node-b', makeBatch())
      deliveryOrder.push('after-send')

      await flushMicrotasks()
      deliveryOrder.push('after-flush')

      expect(deliveryOrder).toContain('batch-received')
      expect(deliveryOrder).toContain('after-flush')
    })

    it('delivers ack via microtask after sendAck resolves', async () => {
      const deliveryOrder: string[] = []
      transportA.onAckReceived(() => {
        deliveryOrder.push('ack-received')
      })

      await transportB.sendAck('node-a', makeAck())
      deliveryOrder.push('after-send-ack')

      await flushMicrotasks()
      deliveryOrder.push('after-flush')

      expect(deliveryOrder).toContain('ack-received')
      expect(deliveryOrder).toContain('after-flush')
    })
  })

  describe('disconnect behavior', () => {
    it('throws when sending after disconnect', async () => {
      await transportA.disconnect()
      await expect(transportA.send('node-b', makeBatch())).rejects.toThrow('Transport is not connected')
    })

    it('throws when broadcasting after disconnect', async () => {
      await transportA.disconnect()
      await expect(transportA.broadcast(makeBatch())).rejects.toThrow('Transport is not connected')
    })

    it('throws when sending ack after disconnect', async () => {
      await transportA.disconnect()
      await expect(transportA.sendAck('node-b', makeAck())).rejects.toThrow('Transport is not connected')
    })

    it('throws when forwarding after disconnect', async () => {
      await transportA.disconnect()
      await expect(transportA.forward('node-b', { requestId: 'x', statements: [] })).rejects.toThrow(
        'Transport is not connected',
      )
    })

    it('throws when sending to a disconnected peer', async () => {
      await transportB.disconnect()
      await expect(transportA.send('node-b', makeBatch())).rejects.toThrow("Peer 'node-b' is not connected")
    })

    it('handles double disconnect gracefully', async () => {
      await transportA.disconnect()
      await transportA.disconnect()
    })
  })

  describe('bus isolation', () => {
    it('two separate buses do not interfere', async () => {
      const bus2 = new MemoryBus()
      const isolated1 = new InMemoryTransport(bus2)
      const isolated2 = new InMemoryTransport(bus2)
      await isolated1.connect('iso-1', {})
      await isolated2.connect('iso-2', {})

      const receivedOnBus1: ReplicationBatch[] = []
      const receivedOnBus2: ReplicationBatch[] = []

      transportB.onBatchReceived(async batch => {
        receivedOnBus1.push(batch)
      })
      isolated2.onBatchReceived(async batch => {
        receivedOnBus2.push(batch)
      })

      await transportA.send('node-b', makeBatch({ batchId: 'bus1-batch' }))
      await isolated1.send('iso-2', makeBatch({ batchId: 'bus2-batch' }))
      await flushMicrotasks()

      expect(receivedOnBus1).toHaveLength(1)
      expect(receivedOnBus1[0].batchId).toBe('bus1-batch')
      expect(receivedOnBus2).toHaveLength(1)
      expect(receivedOnBus2[0].batchId).toBe('bus2-batch')

      await isolated1.disconnect()
      await isolated2.disconnect()
    })
  })

  describe('connection errors', () => {
    it('throws when connecting a transport that is already connected', async () => {
      await expect(transportA.connect('node-a', {})).rejects.toThrow('Transport is already connected')
    })
  })
})
