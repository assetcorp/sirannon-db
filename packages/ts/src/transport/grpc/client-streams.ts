import type { ClientOptions } from '@grpc/grpc-js'
import type { TopologyRole } from '../../replication/types.js'
import {
  fromAckPayload,
  fromBatchPayload,
  fromSyncAckPayload,
  fromSyncBatchPayload,
  fromSyncCompletePayload,
  fromSyncRequestPayload,
} from './codec.js'
import { ReplicationClient, type ReplicationMessage, type SyncMessage } from './generated/replication.js'
import { buildChannelCreds } from './grpc-credentials.js'
import { type ClientPeerEntry, registerPeer, removePeer } from './peer-streams.js'
import type { GrpcReplicationTransport } from './transport.js'

export async function connectToEndpoint(t: GrpcReplicationTransport, endpoint: string): Promise<void> {
  const channelCreds = buildChannelCreds(t.options)
  const clientOpts: Partial<ClientOptions> = {}
  const client = new ReplicationClient(endpoint, channelCreds, clientOpts)

  const replicateStream = client.replicate()
  const syncStream = client.sync()

  const entry: ClientPeerEntry = {
    client,
    replicateStream,
    syncStream,
  }

  replicateStream.write({
    hello: { nodeId: t.localNodeId, role: t.localRole },
  })
  syncStream.write({
    hello: { nodeId: t.localNodeId, role: t.localRole },
  })

  let replicatePeerId: string | null = null

  replicateStream.on('data', (msg: ReplicationMessage) => {
    if (replicatePeerId === null) {
      if (!msg.hello) {
        replicateStream.cancel()
        return
      }
      replicatePeerId = msg.hello.nodeId
      const peerRole = msg.hello.role as TopologyRole
      t.clientPeerStreams.set(replicatePeerId, entry)
      registerPeer(t.connectedPeers, t.peerConnectedHandler, replicatePeerId, peerRole)
      return
    }

    if (msg.batch) {
      t.batchHandler?.(fromBatchPayload(msg.batch), replicatePeerId).catch(() => {})
    } else if (msg.ack) {
      t.ackHandler?.(fromAckPayload(msg.ack), replicatePeerId)
    }
  })

  replicateStream.on('end', () => {
    detachReplicate(t, replicatePeerId)
  })

  replicateStream.on('error', () => {
    detachReplicate(t, replicatePeerId)
  })

  let syncPeerId: string | null = null

  syncStream.on('data', (msg: SyncMessage) => {
    if (syncPeerId === null) {
      if (!msg.hello) {
        syncStream.cancel()
        return
      }
      syncPeerId = msg.hello.nodeId
      return
    }

    if (msg.syncRequest) {
      t.syncRequestHandler?.(fromSyncRequestPayload(msg.syncRequest), syncPeerId).catch(() => {})
    } else if (msg.syncBatch) {
      t.syncBatchHandler?.(fromSyncBatchPayload(msg.syncBatch), syncPeerId).catch(() => {})
    } else if (msg.syncComplete) {
      t.syncCompleteHandler?.(fromSyncCompletePayload(msg.syncComplete), syncPeerId).catch(() => {})
    } else if (msg.syncAck) {
      t.syncAckHandler?.(fromSyncAckPayload(msg.syncAck), syncPeerId)
    }
  })

  syncStream.on('end', () => {
    detachSync(t, syncPeerId)
  })

  syncStream.on('error', () => {
    detachSync(t, syncPeerId)
  })
}

function detachReplicate(t: GrpcReplicationTransport, peerId: string | null): void {
  if (!peerId) return
  const e = t.clientPeerStreams.get(peerId)
  if (!e) return
  e.replicateStream = null
  if (!e.syncStream) {
    t.clientPeerStreams.delete(peerId)
    removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
  }
}

function detachSync(t: GrpcReplicationTransport, peerId: string | null): void {
  if (!peerId) return
  const e = t.clientPeerStreams.get(peerId)
  if (!e) return
  e.syncStream = null
  if (!e.replicateStream) {
    t.clientPeerStreams.delete(peerId)
    removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
  }
}
