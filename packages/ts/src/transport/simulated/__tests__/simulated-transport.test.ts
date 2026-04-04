import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReplicationAck, ReplicationBatch } from '../../../replication/types.js'
import { FaultPolicy } from '../fault-policy.js'
import { SimulatedNetwork } from '../index.js'
import { SeededPRNG } from '../prng.js'
import { DeterministicScheduler } from '../scheduler.js'

const T0 = 1_000_000

function makeBatch(sourceNodeId: string, seq = 1n): ReplicationBatch {
  return {
    sourceNodeId,
    batchId: `${sourceNodeId}-${seq}-${seq}`,
    fromSeq: seq,
    toSeq: seq,
    hlcRange: { min: '000000000001-0000-node', max: '000000000001-0000-node' },
    changes: [],
    checksum: 'abc123',
  }
}

function makeAck(nodeId: string, seq = 1n): ReplicationAck {
  return { batchId: `${nodeId}-${seq}-${seq}`, ackedSeq: seq, nodeId }
}

describe('SimulatedTransport', () => {
  let scheduler: DeterministicScheduler
  let network: SimulatedNetwork
  let prng: SeededPRNG

  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(T0)
    prng = new SeededPRNG(42)
    scheduler = new DeterministicScheduler(T0)
    network = new SimulatedNetwork(scheduler, new FaultPolicy({}, prng))
  })

  afterEach(() => {
    scheduler.dispose()
    vi.useRealTimers()
  })

  describe('connect and disconnect', () => {
    it('connects and discovers existing peers', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()

      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      expect(tA.peers().size).toBe(1)
      expect(tA.peers().has('nodeB')).toBe(true)
      expect(tB.peers().size).toBe(1)
      expect(tB.peers().has('nodeA')).toBe(true)
    })

    it('fires peerConnected handlers on both sides', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()

      const connected: string[] = []
      tA.onPeerConnected(p => connected.push(`A sees ${p.id}`))
      tB.onPeerConnected(p => connected.push(`B sees ${p.id}`))

      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      expect(connected).toContain('A sees nodeB')
      expect(connected).toContain('B sees nodeA')
    })

    it('fires peerDisconnected on disconnect', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()

      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const disconnected: string[] = []
      tA.onPeerDisconnected(id => disconnected.push(id))

      await tB.disconnect()
      expect(disconnected).toContain('nodeB')
      expect(tA.peers().size).toBe(0)
    })

    it('throws on double connect', async () => {
      const t = network.createTransport()
      await t.connect('x', {})
      await expect(t.connect('x', {})).rejects.toThrow('already connected')
    })

    it('throws on send when not connected', async () => {
      const t = network.createTransport()
      await expect(t.send('peer', makeBatch('x'))).rejects.toThrow('not connected')
    })
  })

  describe('batch exchange', () => {
    it('sends and receives a batch through the scheduler', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const received: ReplicationBatch[] = []
      tB.onBatchReceived(async (batch, _from) => {
        received.push(batch)
      })

      const batch = makeBatch('nodeA')
      await tA.send('nodeB', batch)

      expect(received).toHaveLength(0)
      await scheduler.runUntilQuiet()
      expect(received).toHaveLength(1)
      expect(received[0].sourceNodeId).toBe('nodeA')
    })

    it('broadcasts to all connected peers', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      const tC = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})
      await tC.connect('nodeC', {})

      const receivedB: string[] = []
      const receivedC: string[] = []
      tB.onBatchReceived(async batch => receivedB.push(batch.batchId))
      tC.onBatchReceived(async batch => receivedC.push(batch.batchId))

      await tA.broadcast(makeBatch('nodeA'))
      await scheduler.runUntilQuiet()

      expect(receivedB).toHaveLength(1)
      expect(receivedC).toHaveLength(1)
    })

    it('throws when sending to disconnected peer', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})
      await tB.disconnect()

      await expect(tA.send('nodeB', makeBatch('nodeA'))).rejects.toThrow('not connected')
    })

    it('validates batch structure', async () => {
      const t = network.createTransport()
      await t.connect('x', {})
      await expect(t.send('x', { bad: true } as unknown as ReplicationBatch)).rejects.toThrow('Invalid batch structure')
    })
  })

  describe('ack exchange', () => {
    it('sends and receives acks', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const acks: ReplicationAck[] = []
      tA.onAckReceived(ack => acks.push(ack))

      await tB.sendAck('nodeA', makeAck('nodeB'))
      await scheduler.runUntilQuiet()

      expect(acks).toHaveLength(1)
      expect(acks[0].nodeId).toBe('nodeB')
    })
  })

  describe('forward requests', () => {
    it('forwards a request and receives a response', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      tB.onForwardReceived(async req => ({
        results: [{ changes: 1, lastInsertRowId: 42 }],
        requestId: req.requestId,
      }))

      const resultPromise = tA.forward('nodeB', {
        statements: [{ sql: "INSERT INTO items VALUES (1, 'test', 100)" }],
        requestId: 'fwd-1',
      })

      await scheduler.runUntilQuiet()
      const result = await resultPromise

      expect(result.results).toHaveLength(1)
      expect(result.results[0].changes).toBe(1)
      expect(result.requestId).toBe('fwd-1')
    })

    it('throws when forwarding to disconnected peer', async () => {
      const tA = network.createTransport()
      await tA.connect('nodeA', {})

      await expect(tA.forward('nodeB', { statements: [], requestId: 'r' })).rejects.toThrow('not connected')
    })
  })

  describe('fault injection', () => {
    it('drops messages when drop rate is 1.0', async () => {
      const dropPolicy = new FaultPolicy({ dropRate: 1.0 }, new SeededPRNG(1))
      const dropNetwork = new SimulatedNetwork(scheduler, dropPolicy)
      const tA = dropNetwork.createTransport()
      const tB = dropNetwork.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const received: ReplicationBatch[] = []
      tB.onBatchReceived(async batch => received.push(batch))

      await tA.send('nodeB', makeBatch('nodeA'))
      await scheduler.runUntilQuiet()

      expect(received).toHaveLength(0)
    })

    it('drops messages between partitioned nodes', async () => {
      network.policy.addPartition('nodeA', 'nodeB')

      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const received: ReplicationBatch[] = []
      tB.onBatchReceived(async batch => received.push(batch))

      await tA.send('nodeB', makeBatch('nodeA'))
      await scheduler.runUntilQuiet()

      expect(received).toHaveLength(0)
    })

    it('delivers messages after partition is healed', async () => {
      network.policy.addPartition('nodeA', 'nodeB')

      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const received: ReplicationBatch[] = []
      tB.onBatchReceived(async batch => received.push(batch))

      await tA.send('nodeB', makeBatch('nodeA', 1n))
      await scheduler.runUntilQuiet()
      expect(received).toHaveLength(0)

      network.policy.removePartition('nodeA', 'nodeB')
      await tA.send('nodeB', makeBatch('nodeA', 2n))
      await scheduler.runUntilQuiet()
      expect(received).toHaveLength(1)
    })
  })

  describe('error swallowing', () => {
    it('swallows batch handler errors without crashing the scheduler', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      tB.onBatchReceived(async () => {
        throw new Error('handler exploded')
      })

      await tA.send('nodeB', makeBatch('nodeA'))
      await scheduler.runUntilQuiet()
    })
  })

  describe('event delivery to disconnected target', () => {
    it('silently drops events for peers that disconnected after enqueue', async () => {
      const tA = network.createTransport()
      const tB = network.createTransport()
      await tA.connect('nodeA', {})
      await tB.connect('nodeB', {})

      const received: ReplicationBatch[] = []
      tB.onBatchReceived(async batch => received.push(batch))

      await tA.send('nodeB', makeBatch('nodeA'))
      await tB.disconnect()
      await scheduler.runUntilQuiet()

      expect(received).toHaveLength(0)
    })
  })
})
