import { afterEach, describe, expect, it } from 'vitest'
import type { NodeInfo } from '../../../../replication/types.js'
import { GrpcReplicationTransport } from '../../index.js'
import { setupPrimaryReplica, teardown, waitFor } from './_helpers.js'

describe('GrpcReplicationTransport', () => {
  const transports: GrpcReplicationTransport[] = []

  afterEach(async () => {
    await teardown(transports)
  })

  describe('peer connection and hello exchange', () => {
    it('establishes bidirectional peer connection via hello messages', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

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

  describe('insecure mode', () => {
    it('connects in insecure mode without TLS configuration', async () => {
      const { primary, replica } = await setupPrimaryReplica(transports)

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
})
