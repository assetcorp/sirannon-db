import type { Server } from '@grpc/grpc-js'
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
import { stopEndpointDialling } from './client-reconnect.js'
import { connectToEndpoint } from './client-streams.js'
import {
  toAckPayload,
  toBatchPayload,
  toSyncAckPayload,
  toSyncBatchPayload,
  toSyncCompletePayload,
  toSyncRequestPayload,
} from './codec.js'
import { forwardOverRpc } from './forward-rpc.js'
import type { ReplicationMessage, SyncMessage } from './generated/replication.js'
import { DEFAULT_FORWARD_DEADLINE_MS, type GrpcReplicationOptions, SERVICE_NAME } from './options.js'
import {
  type AckHandler,
  type BatchHandler,
  type ClientPeerEntry,
  type ForwardHandler,
  type PeerConnectedHandler,
  type PeerDisconnectedHandler,
  type PeerStreamEntry,
  peerIdForServerStream,
  replicateWriteStream,
  type SyncAckHandler,
  type SyncBatchHandler,
  type SyncCompleteHandler,
  type SyncRequestHandler,
  syncWriteStream,
} from './peer-streams.js'
import { startServer } from './server-streams.js'
import { writeWithBackpressure } from './stream-util.js'

/**
 * @public
 *
 * Replicates between nodes over gRPC with mutual TLS, which is the transport production clusters use.
 */
export class GrpcReplicationTransport implements ReplicationTransport {
  /** @internal */
  readonly options: GrpcReplicationOptions
  /** @internal */
  localNodeId = ''
  /** @internal */
  localRole: TopologyRole = 'replica'
  /** @internal */
  localGroupId: string | undefined
  /** @internal */
  localPrimaryTerm: bigint | undefined
  /** @internal */
  localProtocolVersion: string | undefined
  /** @internal */
  connected = false
  /** @internal */
  server: Server | null = null
  /** @internal */
  boundPort = 0
  /** @internal */
  healthImpl: HealthImplementation | null = null

  /** @internal */
  readonly connectedPeers = new Map<string, NodeInfo>()
  /** @internal */
  readonly serverPeerStreams = new Map<string, PeerStreamEntry>()
  /** @internal */
  readonly clientPeerStreams = new Map<string, ClientPeerEntry>()

  /** @internal */
  batchHandler: BatchHandler | null = null
  /** @internal */
  ackHandler: AckHandler | null = null
  /** @internal */
  forwardHandler: ForwardHandler | null = null
  /** @internal */
  peerConnectedHandler: PeerConnectedHandler | null = null
  /** @internal */
  peerDisconnectedHandler: PeerDisconnectedHandler | null = null
  /** @internal */
  syncRequestHandler: SyncRequestHandler | null = null
  /** @internal */
  syncBatchHandler: SyncBatchHandler | null = null
  /** @internal */
  syncCompleteHandler: SyncCompleteHandler | null = null
  /** @internal */
  syncAckHandler: SyncAckHandler | null = null

  constructor(options: GrpcReplicationOptions = {}) {
    this.options = options
  }

  /** Returns the port the gRPC server bound to, which is the resolved port when you asked for 0. */
  getPort(): number {
    return this.boundPort
  }

  /** Connects to the configured peers and announces this node. */
  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }
    if (!localNodeId || typeof localNodeId !== 'string') {
      throw new TransportError('localNodeId must be a non-empty string')
    }

    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'
    this.localGroupId = config.groupId
    this.localPrimaryTerm = config.primaryTerm
    this.localProtocolVersion = config.protocolVersion
    this.connected = true

    if (this.localRole === 'primary' || config.groupId) {
      await startServer(this)
    }

    if (config.endpoints && config.endpoints.length > 0) {
      for (const endpoint of config.endpoints) {
        connectToEndpoint(this, endpoint)
      }
    }
  }

  /** Closes every peer connection. */
  async disconnect(): Promise<void> {
    if (!this.connected) return
    this.connected = false
    stopEndpointDialling(this)

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

  /** Sends one batch of changes to one peer. */
  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const stream = this.getReplicateWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: ReplicationMessage = { batch: toBatchPayload(batch) }
    await writeWithBackpressure(stream, msg)
  }

  /** Sends one batch of changes to every connected peer. */
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

  /** Confirms to a peer that this node applied one of its batches. */
  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    const stream = this.getReplicateWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: ReplicationMessage = { ack: toAckPayload(ack) }
    await writeWithBackpressure(stream, msg)
  }

  /** Sends a write to the primary and waits for its result. */
  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()
    const clientEntry = this.clientPeerStreams.get(peerId)
    if (!clientEntry) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    return forwardOverRpc(clientEntry, request, this.options.forwardDeadlineMs ?? DEFAULT_FORWARD_DEADLINE_MS)
  }

  /** Asks a peer to stream a full copy of the database. */
  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncRequest: toSyncRequestPayload(request) }
    await writeWithBackpressure(stream, msg)
  }

  /** Sends one page of first-sync table data. */
  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncBatch: toSyncBatchPayload(batch) }
    await writeWithBackpressure(stream, msg)
  }

  /** Tells a joining node that first sync has finished, and sends the manifests to verify it. */
  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncComplete: toSyncCompletePayload(complete) }
    await writeWithBackpressure(stream, msg)
  }

  /** Confirms to the source that a joining node stored one first-sync page. */
  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.ensureConnected()
    const stream = this.getSyncWriteStream(peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const msg: SyncMessage = { syncAck: toSyncAckPayload(ack) }
    await writeWithBackpressure(stream, msg)
  }

  /** Registers the handler that applies incoming change batches. */
  onBatchReceived(handler: BatchHandler): void {
    this.batchHandler = handler
  }

  /** Registers the handler that records incoming acknowledgements. */
  onAckReceived(handler: AckHandler): void {
    this.ackHandler = handler
  }

  /** Registers the handler that runs a write a replica forwarded. */
  onForwardReceived(handler: ForwardHandler): void {
    this.forwardHandler = handler
  }

  /** Registers the handler that serves a first-sync request. */
  onSyncRequested(handler: SyncRequestHandler): void {
    this.syncRequestHandler = handler
  }

  /** Registers the handler that stores an incoming first-sync page. */
  onSyncBatchReceived(handler: SyncBatchHandler): void {
    this.syncBatchHandler = handler
  }

  /** Registers the handler that finishes first sync and verifies the manifests. */
  onSyncCompleteReceived(handler: SyncCompleteHandler): void {
    this.syncCompleteHandler = handler
  }

  /** Registers the handler that records first-sync page acknowledgements. */
  onSyncAckReceived(handler: SyncAckHandler): void {
    this.syncAckHandler = handler
  }

  /** Registers the handler that runs when a peer connects. */
  onPeerConnected(handler: PeerConnectedHandler): void {
    this.peerConnectedHandler = handler
  }

  /** Registers the handler that runs when a peer disconnects. */
  onPeerDisconnected(handler: PeerDisconnectedHandler): void {
    this.peerDisconnectedHandler = handler
  }

  /** Returns every connected peer, keyed by identifier. */
  peers(): ReadonlyMap<string, NodeInfo> {
    return this.connectedPeers
  }

  private extractTlsCN(call: { getAuthContext(): unknown }): string | null {
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

  /** @internal */
  validateTlsIdentity(call: { getAuthContext(): unknown }, claimedNodeId: string): boolean {
    if (this.options.insecure) return true
    if (!this.options.tlsCaCert) return true

    const cn = this.extractTlsCN(call)
    return cn === claimedNodeId
  }

  /** @internal */
  resolveForwardPeerId(call: { getPeer(): string; getAuthContext(): unknown }): string | null {
    const cn = this.extractTlsCN(call)
    if (cn) {
      return this.connectedPeers.has(cn) ? cn : null
    }

    if (!this.options.insecure) return null

    const peerAddr = call.getPeer()
    for (const [, entry] of this.serverPeerStreams) {
      if (entry.replicateStream?.getPeer() === peerAddr) {
        const streamPeerId = peerIdForServerStream(this.serverPeerStreams, entry)
        if (streamPeerId) return streamPeerId
      }
    }
    return null
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }

  private getReplicateWriteStream(peerId: string) {
    return replicateWriteStream(this.serverPeerStreams, this.clientPeerStreams, peerId)
  }

  private getSyncWriteStream(peerId: string) {
    return syncWriteStream(this.serverPeerStreams, this.clientPeerStreams, peerId)
  }
}
