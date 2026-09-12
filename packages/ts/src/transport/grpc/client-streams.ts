import type { ClientDuplexStream, ClientOptions } from '@grpc/grpc-js'
import type { TopologyRole } from '../../replication/types.js'
import {
  claimEndpoint,
  noteEndpointReachable,
  registerEndpointAbort,
  releaseEndpoint,
  scheduleEndpointRedial,
} from './client-reconnect.js'
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

function cancelQuietly(stream: Pick<ClientDuplexStream<never, never>, 'cancel'>): void {
  try {
    stream.cancel()
  } catch {}
}

export function connectToEndpoint(t: GrpcReplicationTransport, endpoint: string): void {
  if (!t.connected) return
  if (!claimEndpoint(t, endpoint)) return

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

  let replicatePeerId: string | null = null
  let syncPeerId: string | null = null
  let closed = false

  const teardown = (): void => {
    if (closed) return
    closed = true
    releaseEndpoint(t, endpoint)

    const peerId = replicatePeerId ?? syncPeerId
    if (peerId !== null && t.clientPeerStreams.get(peerId) === entry) {
      t.clientPeerStreams.delete(peerId)
      removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
    }
    entry.replicateStream = null
    entry.syncStream = null

    cancelQuietly(replicateStream)
    cancelQuietly(syncStream)
    client.close()

    scheduleEndpointRedial(t, endpoint, () => {
      connectToEndpoint(t, endpoint)
    })
  }

  registerEndpointAbort(t, endpoint, teardown)

  const hello = {
    nodeId: t.localNodeId,
    role: t.localRole,
    groupId: t.localGroupId ?? '',
    primaryTerm: t.localPrimaryTerm ?? 0n,
    protocolVersion: t.localProtocolVersion ?? '',
  }
  replicateStream.write({ hello })
  syncStream.write({ hello })

  replicateStream.on('data', (msg: ReplicationMessage) => {
    if (replicatePeerId === null) {
      if (!msg.hello) {
        teardown()
        return
      }
      replicatePeerId = msg.hello.nodeId
      const peerRole = msg.hello.role as TopologyRole
      t.clientPeerStreams.set(replicatePeerId, entry)
      noteEndpointReachable(t, endpoint)
      registerPeer(t.connectedPeers, t.peerConnectedHandler, replicatePeerId, peerRole, {
        groupId: msg.hello.groupId || undefined,
        primaryTerm: msg.hello.primaryTerm === 0n ? undefined : msg.hello.primaryTerm,
        protocolVersion: msg.hello.protocolVersion || undefined,
      })
      return
    }

    if (msg.batch) {
      t.batchHandler?.(fromBatchPayload(msg.batch), replicatePeerId).catch(() => {})
    } else if (msg.ack) {
      t.ackHandler?.(fromAckPayload(msg.ack), replicatePeerId)
    }
  })

  replicateStream.on('end', teardown)
  replicateStream.on('error', teardown)

  syncStream.on('data', (msg: SyncMessage) => {
    if (syncPeerId === null) {
      if (!msg.hello) {
        teardown()
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

  syncStream.on('end', teardown)
  syncStream.on('error', teardown)
}
