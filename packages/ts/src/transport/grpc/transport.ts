import type { Server } from '@grpc/grpc-js'
import type { HealthImplementation } from 'grpc-health-check'
import type { SirannonError } from '../../core/errors.js'
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
import { replaceWantedEndpoints, stopEndpointDialling } from './client-reconnect.js'
import {
  toAckPayload,
  toBatchPayload,
  toSyncAckPayload,
  toSyncBatchPayload,
  toSyncCompletePayload,
  toSyncRequestPayload,
} from './codec.js'
import type { ReplicationMessage, SyncMessage } from './generated/replication.js'
import { type GrpcRuntime, loadGrpcRuntime } from './loader.js'
import { DEFAULT_FORWARD_DEADLINE_MS, type GrpcReplicationOptions, SERVICE_NAME } from './options.js'
import { peerCertificateCommonName } from './peer-identity.js'
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
import { shutdownGrpcServer } from './server-shutdown.js'
import { writeWithBackpressure } from './stream-util.js'

/**
 * Sends replication messages between nodes over gRPC. Unless you set `insecure`, it requires `tlsCert` and `tlsKey`,
 * and when you also set `tlsCaCert`, each side of a connection verifies the other's certificate against it.
 *
 * @public
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
  localEncrypted = false
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

  private runtime: GrpcRuntime | null = null
  private peerEndpoints: ReadonlyMap<string, string> | null = null

  constructor(options: GrpcReplicationOptions = {}) {
    this.options = options
  }

  /**
   * Returns the port that the gRPC server listens on, including the port that the operating system picks when `port`
   * is 0, and 0 while no server is listening.
   */
  getPort(): number {
    return this.boundPort
  }

  /**
   * Loads the gRPC packages, starts the gRPC server when this node is the primary or has a group ID, and dials every
   * configured endpoint. Without configured endpoints, it dials the peer addresses from the last
   * {@link GrpcReplicationTransport.setPeerEndpoints} call, so that a transport that reconnects reaches its peers at
   * once.
   *
   * @throws A `SirannonError` with code `TRANSPORT_DEPENDENCY_MISSING` when this process cannot load `@grpc/grpc-js`, `@bufbuild/protobuf`, or `grpc-health-check`.
   */
  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    const runtime = await loadGrpcRuntime()
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }
    if (!localNodeId || typeof localNodeId !== 'string') {
      throw new TransportError('localNodeId must be a non-empty string')
    }

    this.runtime = runtime
    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'
    this.localGroupId = config.groupId
    this.localPrimaryTerm = config.primaryTerm
    this.localProtocolVersion = config.protocolVersion
    this.localEncrypted = config.encrypted === true
    this.connected = true

    if (this.localRole === 'primary' || config.groupId) {
      await runtime.startServer(this)
    }

    if (config.endpoints && config.endpoints.length > 0) {
      for (const endpoint of config.endpoints) {
        runtime.connectToEndpoint(this, endpoint)
      }
    } else if (this.peerEndpoints) {
      this.setPeerEndpoints(this.peerEndpoints)
    }
  }

  /**
   * Dials the replication address of every peer in `endpoints` and closes the connections that this transport dialled
   * to any other address, so that the transport follows the addresses in the members' coordinator sessions. The
   * transport keeps the addresses and dials them again whenever it connects.
   *
   * @param endpoints - Maps each peer's node ID to the replication address to dial.
   */
  setPeerEndpoints(endpoints: ReadonlyMap<string, string>): void {
    this.peerEndpoints = new Map(endpoints)
    const runtime = this.runtime
    if (!this.connected || !runtime) return
    for (const endpoint of replaceWantedEndpoints(this, endpoints.values())) {
      runtime.connectToEndpoint(this, endpoint)
    }
  }

  /** Closes every peer connection and shuts down the gRPC server. */
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
      await shutdownGrpcServer(this.server)
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
    if (!clientEntry || !this.runtime) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    return this.runtime.forwardOverRpc(
      clientEntry,
      request,
      this.options.forwardDeadlineMs ?? DEFAULT_FORWARD_DEADLINE_MS,
    )
  }

  /** Asks a peer to stream a full copy of the database. */
  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    await this.writeSync(peerId, () => ({ syncRequest: toSyncRequestPayload(request) }))
  }

  /** Sends one page of first-sync table data. */
  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    await this.writeSync(peerId, () => ({ syncBatch: toSyncBatchPayload(batch) }))
  }

  /** Tells a joining node that first sync is complete, and sends the manifests that the node checks its copy against. */
  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    await this.writeSync(peerId, () => ({ syncComplete: toSyncCompletePayload(complete) }))
  }

  /** Tells the source whether this joining node stored one first-sync page. */
  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    await this.writeSync(peerId, () => ({ syncAck: toSyncAckPayload(ack) }))
  }

  /**
   * Ends every stream with one peer, and carries the error's code to the peer in the `sirannon-error-code` trailer of
   * each stream that the peer opened.
   *
   * @param peerId - The peer to refuse.
   * @param error - The error whose code the streams carry, such as `ENCRYPTION_REQUIRED`.
   */
  async refusePeer(peerId: string, error: SirannonError): Promise<void> {
    if (!this.connected || !this.runtime) return
    this.runtime.refusePeerStreams(this, peerId, error)
  }

  /** Registers the handler that applies incoming change batches. */
  onBatchReceived(handler: BatchHandler): void {
    this.batchHandler = handler
  }

  /** Registers the handler that records incoming acknowledgements. */
  onAckReceived(handler: AckHandler): void {
    this.ackHandler = handler
  }

  /** Registers the handler that executes a write that a replica forwards. */
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

  /** Registers the handler that the transport calls when a peer connects. */
  onPeerConnected(handler: PeerConnectedHandler): void {
    this.peerConnectedHandler = handler
  }

  /** Registers the handler that the transport calls when a peer disconnects. */
  onPeerDisconnected(handler: PeerDisconnectedHandler): void {
    this.peerDisconnectedHandler = handler
  }

  /** Returns every connected peer, keyed by node ID. */
  peers(): ReadonlyMap<string, NodeInfo> {
    return this.connectedPeers
  }

  /** @internal */
  validateTlsIdentity(call: { getAuthContext(): unknown }, claimedNodeId: string): boolean {
    if (this.options.insecure) return true
    if (!this.options.tlsCaCert) return true

    const cn = peerCertificateCommonName(this.options, call)
    return cn === claimedNodeId
  }

  /** @internal */
  resolveForwardPeerId(call: { getPeer(): string; getAuthContext(): unknown }): string | null {
    const cn = peerCertificateCommonName(this.options, call)
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

  private async writeSync(peerId: string, message: () => SyncMessage): Promise<void> {
    this.ensureConnected()
    const stream = syncWriteStream(this.serverPeerStreams, this.clientPeerStreams, peerId)
    if (!stream) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    await writeWithBackpressure(stream, message())
  }
}
