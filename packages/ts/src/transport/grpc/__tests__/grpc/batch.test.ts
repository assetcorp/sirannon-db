import { afterEach, describe, expect, it } from 'vitest'
import type { ReplicationBatch } from '../../../../replication/types.js'
import { GrpcReplicationTransport } from '../../index.js'
import { createBatch, setupPrimaryReplica, teardown, waitFor } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('batch send and receive', () => {
    it('sends a batch from primary and receives it on replica', async () => {
      const { primary } = await setupPrimaryReplica(transports)

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
      const { primary } = await setupPrimaryReplica(transports)

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

  describe('large payload handling', () => {
    it('transfers 100 changes with 1KB data each', async () => {
      const { primary } = await setupPrimaryReplica(transports)

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
})
