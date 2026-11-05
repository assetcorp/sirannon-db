import { Server, type ServerDuplexStream, status } from '@grpc/grpc-js'
import { HealthImplementation } from 'grpc-health-check'
import { SirannonError } from '../../core/errors.js'
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
  Hello,
  ForwardRequest as ProtoForwardRequest,
  ForwardResponse as ProtoForwardResponse,
  ReplicationMessage,
  SyncMessage,
} from './generated/replication.js'
import { ReplicationService } from './generated/replication.js'
import { buildServerCreds } from './grpc-credentials.js'
import { localHello, peerHelloFields } from './hello.js'
import { SERVICE_NAME } from './options.js'
import type { PeerStreamEntry } from './peer-streams.js'
import { registerPeer, removePeer, restartPeerConnection } from './peer-streams.js'
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

interface RefusableStream {
  getAuthContext(): unknown
  emit(event: 'error', error: Error): boolean
}

function refuseStream(call: RefusableStream, code: status, details: string): false {
  call.emit('error', Object.assign(new Error(details), { code, details }))
  return false
}

function acceptsHello<T extends { hello?: Hello }>(
  t: GrpcReplicationTransport,
  call: RefusableStream,
  msg: T,
): msg is T & { hello: Hello } {
  if (!msg.hello) return refuseStream(call, status.INVALID_ARGUMENT, 'First message must be Hello')
  if (!t.validateTlsIdentity(call, msg.hello.nodeId)) {
    return refuseStream(
      call,
      status.UNAUTHENTICATED,
      `TLS certificate CN does not match claimed nodeId '${msg.hello.nodeId}'`,
    )
  }
  return true
}

export function handleServerReplicateStream(
  t: GrpcReplicationTransport,
  call: ServerDuplexStream<ReplicationMessage, ReplicationMessage>,
): void {
  let peerId: string | null = null
  let helloSent = false

  call.on('data', (msg: ReplicationMessage) => {
    if (peerId === null) {
      if (!acceptsHello(t, call, msg)) return

      peerId = msg.hello.nodeId
      const peerRole = msg.hello.role as TopologyRole

      const existing = t.serverPeerStreams.get(peerId)
      const replaced = existing?.replicateStream ?? null
      replaced?.end()

      const ps: PeerStreamEntry = existing ?? {
        replicateStream: null,
        syncStream: null,
      }
      ps.replicateStream = call
      t.serverPeerStreams.set(peerId, ps)
      if (replaced) restartPeerConnection(t.connectedPeers, t.peerDisconnectedHandler, peerId)

      if (!helloSent) {
        call.write({ hello: localHello(t) })
        helloSent = true
      }

      registerPeer(t.connectedPeers, t.peerConnectedHandler, peerId, peerRole, peerHelloFields(msg.hello))
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
    forgetServerStream(t, peerId, call, 'replicateStream')
  })

  call.on('error', () => {
    forgetServerStream(t, peerId, call, 'replicateStream')
  })
}

function forgetServerStream(
  t: GrpcReplicationTransport,
  peerId: string | null,
  call: PeerStreamEntry['replicateStream'] | PeerStreamEntry['syncStream'],
  kind: keyof PeerStreamEntry,
): void {
  if (peerId === null) return
  const ps = t.serverPeerStreams.get(peerId)
  if (!ps || ps[kind] !== call) return
  ps[kind] = null
  const other = kind === 'replicateStream' ? ps.syncStream : ps.replicateStream
  if (!other) removePeer(t.connectedPeers, t.serverPeerStreams, t.peerDisconnectedHandler, peerId)
}

export function handleServerSyncStream(
  t: GrpcReplicationTransport,
  call: ServerDuplexStream<SyncMessage, SyncMessage>,
): void {
  let peerId: string | null = null
  let helloSent = false

  call.on('data', (msg: SyncMessage) => {
    if (peerId === null) {
      if (!acceptsHello(t, call, msg)) return

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
        call.write({ hello: localHello(t) })
        helloSent = true
      }

      registerPeer(t.connectedPeers, t.peerConnectedHandler, peerId, peerRole, peerHelloFields(msg.hello))
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
    forgetServerStream(t, peerId, call, 'syncStream')
  })

  call.on('error', () => {
    forgetServerStream(t, peerId, call, 'syncStream')
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

  let appReq: ReturnType<typeof fromForwardRequest>
  try {
    appReq = fromForwardRequest(call.request)
  } catch (err: unknown) {
    callback({
      code: status.INVALID_ARGUMENT,
      message: err instanceof Error ? err.message : String(err),
    })
    return
  }
  t.forwardHandler(appReq, peerId)
    .then(result => {
      callback(null, {
        requestId: result.requestId,
        results: result.results.map(r => ({
          changes: r.changes,
          lastInsertRowId: BigInt(typeof r.lastInsertRowId === 'string' ? r.lastInsertRowId : r.lastInsertRowId),
        })),
        error: '',
        errorCode: '',
        groupId: result.groupId ?? '',
        primaryTerm: result.primaryTerm ?? 0n,
      })
    })
    .catch((err: Error) => {
      callback(null, {
        requestId: call.request.requestId,
        results: [],
        error: err.message,
        errorCode: err instanceof SirannonError ? err.code : '',
        groupId: call.request.groupId ?? '',
        primaryTerm: call.request.primaryTerm ?? 0n,
      })
    })
}
