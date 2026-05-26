import { afterEach, describe, expect, it } from 'vitest'
import type { ReplicationAck } from '../../../../replication/types.js'
import type { GrpcReplicationTransport } from '../../index.js'
import { createAck, setupPrimaryReplica, teardown, waitFor } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('ack exchange', () => {
    it('sends ack from replica and receives it on primary', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

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
})
