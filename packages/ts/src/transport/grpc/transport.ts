import {
  type ClientDuplexStream,
  Metadata,
  type Server,
  type ServerDuplexStream,
  type ServiceError,
} from '@grpc/grpc-js'
import type { HealthImplementation } from 'grpc-health-check'
import { TransportError } from '../../replication/errors.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  ReplicationAck,
  ReplicationBatch,
  ReplicationTransport,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  TopologyRole,
  TransportConfig,
} from '../../replication/types.js'
import { connectToEndpoint } from './client-streams.js'
import {
  toAckPayload,
  toBatchPayload,
  toForwardRequest,
  toSyncAckPayload,
  toSyncBatchPayload,
  toSyncCompletePayload,
  toSyncRequestPayload,
} from './codec.js'
import type {
  ForwardResponse as ProtoForwardResponse,
  ReplicationMessage,
  SyncMessage,
} from './generated/replication.js'
import { DEFAULT_FORWARD_DEADLINE_MS, type GrpcReplicationOptions, SERVICE_NAME } from './options.js'
import type {
  AckHandler,
  BatchHandler,
  ClientPeerEntry,
  ForwardHandler,
  PeerConnectedHandler,
  PeerDisconnectedHandler,
  PeerStreamEntry,
  SyncAckHandler,
  SyncBatchHandler,
  SyncCompleteHandler,
  SyncRequestHandler,
} from './peer-streams.js'
import { startServer } from './server-streams.js'
import { writeWithBackpressure } from './stream-util.js'

export class GrpcReplicationTransport implements ReplicationTransport {
  readonly options: GrpcReplicationOptions
  localNodeId = ''
  localRole: TopologyRole = 'replica'
  connected = false
  server: Server | null = null
  boundPort = 0
  healthImpl: HealthImplementation | null = null

  readonly connectedPeers = new Map<string, NodeInfo>()
  readonly serverPeerStreams = new Map<string, PeerStreamEntry>()
  readonly clientPeerStreams = new Map<string, ClientPeerEntry>()

  batchHandler: BatchHandler | null = null
  ackHandler: AckHandler | null = null
  forwardHandler: ForwardHandler | null = null
  peerConnectedHandler: PeerConnectedHandler | null = null
  peerDisconnectedHandler: PeerDisconnectedHandler | null = null
  syncRequestHandler: SyncRequestHandler | null = null
  syncBatchHandler: SyncBatchHandler | null = null
  syncCompleteHandler: SyncCompleteHandler | null = null
  syncAckHandler: SyncAckHandler | null = null

  constructor(options: GrpcReplicationOptions = {}) {
    this.options = options
  }

  getPort(): number {
    return this.boundPort
  }

  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }
    if (!localNodeId || typeof localNodeId !== 'string') {
      throw new TransportError('localNodeId must be a non-empty string')
    }

    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'
    this.connected = true

    if (this.localRole === 'primary') {
      await startServer(this)
    }

    if (config.endpoints && config.endpoints.length > 0) {
      for (const endpoint of config.endpoints) {
        await connectToEndpoint(this, endpoint)
      }
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return
    this.connected = false

    for (const [peerId, entry] of this.clientPeerStreams) {
      entry.replicateStream?.cancel()
      entry.syncStream?.cancel()
      entry.client.close()
      this.clientPeerStreams.delete(peerId)
    }

    for (const [peerId, ps] of this.serverPeerStreams) {
      ps.replicateStream?.end()
      ps.syncStream?.end()
      this.serverPeerStreams.delete(peerId)
    }

    const peerIds = [...this.connectedPeers.keys()]
    this.connectedPeers.clear()
    for (const peerId of peerIds) {
      this.peerDisconnectedHandler?.(peerId)
    }

    if (this.healthImpl) {
      this.healthImpl.setStatus(SERVICE_NAME, 'NOT_SERVING')
    }

    if (this.server) {
      await new Promise<void>(resolve => {
        const srv = this.server
        if (!srv) {
          resolve()
          return
        }
        srv.tryShutdown(err => {
          if (err) {
            srv.forceShutdown()
          }
          resolve()
        })
      })
      this.server = null
    }
  }

  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const stream = this.getReplicateWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: ReplicationMessage = { batch: toBatchPayload(batch) }
    await writeWithBackpressure(stream, msg)
  }

  async broadcast(batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const msg: ReplicationMessage = { batch: toBatchPayload(batch) }
    const promises: Promise<void>[] = []
    for (const [peerId] of this.connectedPeers) {
      const stream = this.getReplicateWriteStream(peerId)
      if (stream) {
        promises.push(writeWithBackpressure(stream, msg))
      }
    }
    await Promise.all(promises)
  }

  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    const stream = this.getReplicateWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: ReplicationMessage = { ack: toAckPayload(ack) }
    await writeWithBackpressure(stream, msg)
  }

  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()
    const clientEntry = this.clientPeerStreams.get(peerId)
    if (!clientEntry) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }

    const protoReq = toForwardRequest(request)
    const deadlineMs = this.options.forwardDeadlineMs ?? DEFAULT_FORWARD_DEADLINE_MS
    const deadline = new Date(Date.now() + deadlineMs)

    return new Promise<ForwardedTransactionResult>((resolve, reject) => {
      clientEntry.client.forward(
        protoReq,
        new Metadata(),
        { deadline },
        (err: ServiceError | null, response: ProtoForwardResponse | undefined) => {
          if (err) {
            reject(new TransportError(`Forward RPC failed: ${err.message}`))
            return
          }
          if (!response) {
            reject(new TransportError('Forward RPC returned empty response'))
            return
          }
          if (response.error) {
            reject(new TransportError(`Forward RPC error: ${response.error}`))
            return
          }
          resolve({
            requestId: response.requestId,
            results: response.results.map(r => ({
              changes: r.changes,
              lastInsertRowId: Number(r.lastInsertRowId),
            })),
          })
        },
      )
    })
  }

  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncRequest: toSyncRequestPayload(request) }
    await writeWithBackpressure(stream, msg)
  }

  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncBatch: toSyncBatchPayload(batch) }
    await writeWithBackpressure(stream, msg)
  }

  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncComplete: toSyncCompletePayload(complete) }
    await writeWithBackpressure(stream, msg)
  }

  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncAck: toSyncAckPayload(ack) }
    await writeWithBackpressure(stream, msg)
  }

  onBatchReceived(handler: BatchHandler): void {
    this.batchHandler = handler
  }

  onAckReceived(handler: AckHandler): void {
    this.ackHandler = handler
  }

  onForwardReceived(handler: ForwardHandler): void {
    this.forwardHandler = handler
  }

  onSyncRequested(handler: SyncRequestHandler): void {
    this.syncRequestHandler = handler
  }

  onSyncBatchReceived(handler: SyncBatchHandler): void {
    this.syncBatchHandler = handler
  }

  onSyncCompleteReceived(handler: SyncCompleteHandler): void {
    this.syncCompleteHandler = handler
  }

  onSyncAckReceived(handler: SyncAckHandler): void {
    this.syncAckHandler = handler
  }

  onPeerConnected(handler: PeerConnectedHandler): void {
    this.peerConnectedHandler = handler
  }

  onPeerDisconnected(handler: PeerDisconnectedHandler): void {
    this.peerDisconnectedHandler = handler
  }

  peers(): ReadonlyMap<string, NodeInfo> {
    return this.connectedPeers
  }

  extractTlsCN(call: { getAuthContext(): unknown }): string | null {
    if (this.options.insecure) return null
    if (!this.options.tlsCaCert) return null

    const authCtx = call.getAuthContext() as { sslPeerCertificate?: { subject?: { CN?: string | string[] } } } | null
    if (!authCtx) return null
    const peerCert = authCtx.sslPeerCertificate
    if (!peerCert) return null
    const certCN = peerCert.subject?.CN
    if (!certCN) return null

    if (Array.isArray(certCN)) return certCN[0] ?? null
    return certCN
  }

  validateTlsIdentity(call: { getAuthContext(): unknown }, claimedNodeId: string): boolean {
    if (this.options.insecure) return true
    if (!this.options.tlsCaCert) return true

    const cn = this.extractTlsCN(call)
    return cn === claimedNodeId
  }

  resolveForwardPeerId(call: { getPeer(): string; getAuthContext(): unknown }): string | null {
    const cn = this.extractTlsCN(call)
    if (cn) {
      return this.connectedPeers.has(cn) ? cn : null
    }

    if (!this.options.insecure) return null

    const peerAddr = call.getPeer()
    for (const [, entry] of this.serverPeerStreams) {
      if (entry.replicateStream?.getPeer() === peerAddr) {
        const streamPeerId = this.findPeerIdForStream(entry)
        if (streamPeerId) return streamPeerId
      }
    }
    return null
  }

  private findPeerIdForStream(entry: PeerStreamEntry): string | null {
    for (const [peerId, ps] of this.serverPeerStreams) {
      if (ps === entry) return peerId
    }
    return null
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }

  private getReplicateWriteStream(
    peerId: string,
  ):
    | ServerDuplexStream<ReplicationMessage, ReplicationMessage>
    | ClientDuplexStream<ReplicationMessage, ReplicationMessage>
    | null {
    const serverStream = this.serverPeerStreams.get(peerId)
    if (serverStream?.replicateStream) return serverStream.replicateStream

    const clientEntry = this.clientPeerStreams.get(peerId)
    if (clientEntry?.replicateStream) return clientEntry.replicateStream

    return null
  }

  private getSyncWriteStream(
    peerId: string,
  ): ServerDuplexStream<SyncMessage, SyncMessage> | ClientDuplexStream<SyncMessage, SyncMessage> | null {
    const serverStream = this.serverPeerStreams.get(peerId)
    if (serverStream?.syncStream) return serverStream.syncStream

    const clientEntry = this.clientPeerStreams.get(peerId)
    if (clientEntry?.syncStream) return clientEntry.syncStream

    return null
  }
}
