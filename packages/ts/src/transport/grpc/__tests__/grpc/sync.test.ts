import { afterEach, describe, expect, it } from 'vitest'
import type { SyncAck, SyncBatch, SyncComplete, SyncRequest } from '../../../../replication/types.js'
import type { GrpcReplicationTransport } from '../../index.js'
import { setupPrimaryReplica, teardown, waitFor } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('sync operations', () => {
    it('sends sync request and receives sync batch and complete', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

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
          totalTables: 3,
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
      expect(receivedSyncBatches[0]?.batch.totalTables).toBe(3)
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
