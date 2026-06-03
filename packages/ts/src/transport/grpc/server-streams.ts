import { Server, type ServerDuplexStream, status } from '@grpc/grpc-js'
import { HealthImplementation } from 'grpc-health-check'
import { TransportError } from '../../replication/errors.js'
import type { TopologyRole } from '../../replication/types.js'
import {
  fromAckPayload,
  fromBatchPayload,
  fromForwardRequest,
  fromSyncAckPayload,
  fromSyncBatchPayload,
  fromSyncCompletePayload,
  fromSyncRequestPayload,
} from './codec.js'
import type {
  ForwardRequest as ProtoForwardRequest,
  ForwardResponse as ProtoForwardResponse,
  ReplicationMessage,
  SyncMessage,
} from './generated/replication.js'
import { ReplicationService } from './generated/replication.js'
import { buildServerCreds } from './grpc-credentials.js'
import { SERVICE_NAME } from './options.js'
import type { PeerStreamEntry } from './peer-streams.js'
import { registerPeer, removePeer } from './peer-streams.js'
import type { GrpcReplicationTransport } from './transport.js'

export async function startServer(t: GrpcReplicationTransport): Promise<void> {
  const server = new Server()
  t.server = server

  const healthImpl = new HealthImplementation({
    [SERVICE_NAME]: 'SERVING',
    '': 'SERVING',
  })
  healthImpl.addToServer(server)
  t.healthImpl = healthImpl

  server.addService(ReplicationService, {
    replicate: (call: ServerDuplexStream<ReplicationMessage, ReplicationMessage>) => {
      handleServerReplicateStream(t, call)
    },
    sync: (call: ServerDuplexStream<SyncMessage, SyncMessage>) => {
      handleServerSyncStream(t, call)
    },
    forward: (
      call: { request: ProtoForwardRequest; getPeer(): string; getAuthContext(): unknown },
      callback: (err: { code: number; message: string } | null, response?: ProtoForwardResponse) => void,
    ) => {
      handleForwardCall(t, call, callback)
    },
  })

  const host = t.options.host ?? '0.0.0.0'
  const port = t.options.port ?? 0
  const bindAddress = `${host}:${port}`

  const serverCreds = buildServerCreds(t.options)

  await new Promise<void>((resolve, reject) => {
    server.bindAsync(bindAddress, serverCreds, (err, assignedPort) => {
      if (err) {
        reject(new TransportError(`Failed to bind gRPC server: ${err.message}`))
        return
      }
      t.boundPort = assignedPort
      resolve()
    })
  })
}

export function handleServerReplicateStream(
  t: GrpcReplicationTransport,
  call: ServerDuplexStream<ReplicationMessage, ReplicationMessage>,
): void {
  let peerId: string | null = null
  let helloSent = false

  call.on('data', (msg: ReplicationMessage) => {
    if (peerId === null) {
      if (!msg.hello) {
        call.destroy(new Error('First message must be Hello'))
        return
      }

      if (!t.validateTlsIdentity(call, msg.hello.nodeId)) {
        call.destroy(new Error(`TLS certificate CN does not match claimed nodeId '${msg.hello.nodeId}'`))
        return
      }

      peerId = msg.hello.nodeId
      const peerRole = msg.hello.role as TopologyRole

      const existing = t.serverPeerStreams.get(peerId)
      if (existing?.replicateStream) {
        existing.replicateStream.end()
      }

      const ps: PeerStreamEntry = t.serverPeerStreams.get(peerId) ?? {
        replicateStream: null,
        syncStream: null,
      }
      ps.replicateStream = call
      t.serverPeerStreams.set(peerId, ps)

      if (!helloSent) {
        call.write({
          hello: {
            nodeId: t.localNodeId,
            role: t.localRole,
            groupId: t.localGroupId ?? '',
            primaryTerm: t.localPrimaryTerm ?? 0n,
            protocolVersion: t.localProtocolVersion ?? '',
          },
        })
        helloSent = true
      }

      registerPeer(t.connectedPeers, t.peerConnectedHandler, peerId, peerRole, {
        groupId: msg.hello.groupId || undefined,
        primaryTerm: msg.hello.primaryTerm === 0n ? undefined : msg.hello.primaryTerm,
        protocolVersion: msg.hello.protocolVersion || undefined,
      })
      return
    }

    if (msg.batch) {
      const batch = fromBatchPayload(msg.batch)
      t.batchHandler?.(batch, peerId).catch(() => {})
    } else if (msg.ack) {
      const ack = fromAckPayload(msg.ack)
      t.ackHandler?.(ack, peerId)
    }
  })

  call.on('end', () => {
    call.end()
    if (peerId) {
      const ps = t.serverPeerStreams.get(peerId)
      if (ps?.replicateStream === call) {
        ps.replicateStream = null
      }
      if (!ps?.syncStream) {
        removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
      }
    }
  })

  call.on('error', () => {
    if (peerId) {
      const ps = t.serverPeerStreams.get(peerId)
      if (ps?.replicateStream === call) {
        ps.replicateStream = null
      }
      if (!ps?.syncStream) {
        removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
      }
    }
  })
}

export function handleServerSyncStream(
  t: GrpcReplicationTransport,
  call: ServerDuplexStream<SyncMessage, SyncMessage>,
): void {
  let peerId: string | null = null
  let helloSent = false

  call.on('data', (msg: SyncMessage) => {
    if (peerId === null) {
      if (!msg.hello) {
        call.destroy(new Error('First message must be Hello'))
        return
      }

      if (!t.validateTlsIdentity(call, msg.hello.nodeId)) {
        call.destroy(new Error(`TLS certificate CN does not match claimed nodeId '${msg.hello.nodeId}'`))
        return
      }

      peerId = msg.hello.nodeId
      const peerRole = msg.hello.role as TopologyRole

      const existing = t.serverPeerStreams.get(peerId)
      if (existing?.syncStream) {
        existing.syncStream.end()
      }

      const ps: PeerStreamEntry = t.serverPeerStreams.get(peerId) ?? {
        replicateStream: null,
        syncStream: null,
      }
      ps.syncStream = call
      t.serverPeerStreams.set(peerId, ps)

      if (!helloSent) {
        call.write({
          hello: {
            nodeId: t.localNodeId,
            role: t.localRole,
            groupId: t.localGroupId ?? '',
            primaryTerm: t.localPrimaryTerm ?? 0n,
            protocolVersion: t.localProtocolVersion ?? '',
          },
        })
        helloSent = true
      }

      registerPeer(t.connectedPeers, t.peerConnectedHandler, peerId, peerRole, {
        groupId: msg.hello.groupId || undefined,
        primaryTerm: msg.hello.primaryTerm === 0n ? undefined : msg.hello.primaryTerm,
        protocolVersion: msg.hello.protocolVersion || undefined,
      })
      return
    }

    if (msg.syncRequest) {
      t.syncRequestHandler?.(fromSyncRequestPayload(msg.syncRequest), peerId).catch(() => {})
    } else if (msg.syncBatch) {
      t.syncBatchHandler?.(fromSyncBatchPayload(msg.syncBatch), peerId).catch(() => {})
    } else if (msg.syncComplete) {
      t.syncCompleteHandler?.(fromSyncCompletePayload(msg.syncComplete), peerId).catch(() => {})
    } else if (msg.syncAck) {
      t.syncAckHandler?.(fromSyncAckPayload(msg.syncAck), peerId)
    }
  })

  call.on('end', () => {
    call.end()
    if (peerId) {
      const ps = t.serverPeerStreams.get(peerId)
      if (ps?.syncStream === call) {
        ps.syncStream = null
      }
      if (!ps?.replicateStream) {
        removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
      }
    }
  })

  call.on('error', () => {
    if (peerId) {
      const ps = t.serverPeerStreams.get(peerId)
      if (ps?.syncStream === call) {
        ps.syncStream = null
      }
      if (!ps?.replicateStream) {
        removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
      }
    }
  })
}

export function handleForwardCall(
  t: GrpcReplicationTransport,
  call: { request: ProtoForwardRequest; getPeer(): string; getAuthContext(): unknown },
  callback: (err: { code: number; message: string } | null, response?: ProtoForwardResponse) => void,
): void {
  if (!t.forwardHandler) {
    callback({
      code: status.UNIMPLEMENTED,
      message: 'Forward handler not registered',
    })
    return
  }

  const peerId = t.resolveForwardPeerId(call)
  if (!peerId) {
    callback({
      code: status.UNAUTHENTICATED,
      message: 'Forward request from unidentified peer',
    })
    return
  }

  const appReq = fromForwardRequest(call.request)
  t.forwardHandler(appReq, peerId)
    .then(result => {
      callback(null, {
        requestId: result.requestId,
        results: result.results.map(r => ({
          changes: r.changes,
          lastInsertRowId: BigInt(typeof r.lastInsertRowId === 'string' ? r.lastInsertRowId : r.lastInsertRowId),
        })),
        error: '',
        groupId: result.groupId ?? '',
        primaryTerm: result.primaryTerm ?? 0n,
      })
    })
    .catch((err: Error) => {
      callback(null, {
        requestId: call.request.requestId,
        results: [],
        error: err.message,
        groupId: call.request.groupId ?? '',
        primaryTerm: call.request.primaryTerm ?? 0n,
      })
    })
}
