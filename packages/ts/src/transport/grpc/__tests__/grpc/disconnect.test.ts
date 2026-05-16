import { afterEach, describe, expect, it } from 'vitest'
import { GrpcReplicationTransport } from '../../index.js'
import { createBatch, setupPrimaryReplica, teardown, waitFor } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('disconnect behaviour', () => {
    it('notifies peers on disconnect', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

      const disconnectedPeers: string[] = []
      primary.onPeerDisconnected(peerId => {
        disconnectedPeers.push(peerId)
      })

      await replica.disconnect()

      await waitFor(() => disconnectedPeers.length > 0, 10_000)

      expect(disconnectedPeers).toContain('replica-node')
    })

    it('throws TransportError when sending after disconnect', async () => {
      const { primary } = await setupPrimaryReplica(transports)
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
})
