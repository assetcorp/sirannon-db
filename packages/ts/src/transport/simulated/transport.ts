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
  TransportConfig,
} from '../../replication/types.js'
import type {
  AckHandler,
  BatchHandler,
  ForwardHandler,
  PeerConnectedHandler,
  PeerDisconnectedHandler,
  SyncAckHandler,
  SyncBatchHandler,
  SyncCompleteHandler,
  SyncRequestHandler,
} from './handler-types.js'
import type { SimulatedNetwork } from './network.js'
import {
  isValidAck,
  isValidBatch,
  isValidForwardedTransaction,
  isValidSyncAck,
  isValidSyncBatch,
  isValidSyncComplete,
  isValidSyncRequest,
} from './validators.js'

export class SimulatedTransport implements ReplicationTransport {
  private localNodeId = ''
  localRole: 'primary' | 'replica' = 'replica'
  private connected = false
  private readonly network: SimulatedNetwork
  readonly connectedPeers = new Map<string, NodeInfo>()

  private batchHandler: BatchHandler | null = null
  private ackHandler: AckHandler | null = null
  private forwardHandler: ForwardHandler | null = null
  peerConnectedHandler: PeerConnectedHandler | null = null
  peerDisconnectedHandler: PeerDisconnectedHandler | null = null
  private syncRequestHandler: SyncRequestHandler | null = null
  private syncBatchHandler: SyncBatchHandler | null = null
  private syncCompleteHandler: SyncCompleteHandler | null = null
  private syncAckHandler: SyncAckHandler | null = null

  constructor(network: SimulatedNetwork) {
    this.network = network
  }

  get isConnected(): boolean {
    return this.connected
  }

  get nodeId(): string {
    return this.localNodeId
  }

  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }

    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'
    this.connected = true
    this.network.register(localNodeId, this)

    for (const [peerId, peerTransport] of this.allPeerTransports()) {
      const peerInfo: NodeInfo = {
        id: peerId,
        role: peerTransport.localRole,
        joinedAt: Date.now(),
        lastSeenAt: Date.now(),
        lastAckedSeq: 0n,
      }
      this.connectedPeers.set(peerId, peerInfo)

      const localInfo: NodeInfo = {
        id: localNodeId,
        role: this.localRole,
        joinedAt: Date.now(),
        lastSeenAt: Date.now(),
        lastAckedSeq: 0n,
      }
      peerTransport.connectedPeers.set(localNodeId, localInfo)

      if (this.peerConnectedHandler) {
        this.peerConnectedHandler(peerInfo)
      }
      if (peerTransport.peerConnectedHandler) {
        peerTransport.peerConnectedHandler(localInfo)
      }
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return

    this.connected = false
    this.network.unregister(this.localNodeId)

    for (const [peerId] of this.connectedPeers) {
      const peerTransport = this.network.getTransport(peerId)
      if (peerTransport) {
        peerTransport.connectedPeers.delete(this.localNodeId)
        if (peerTransport.peerDisconnectedHandler) {
          peerTransport.peerDisconnectedHandler(this.localNodeId)
        }
      }
    }

    this.connectedPeers.clear()
  }

  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidBatch(batch)) throw new TransportError('Invalid batch structure')

    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)

    this.network.scheduleMessage(this.localNodeId, peerId, 'batch', batch)
  }

  async broadcast(batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidBatch(batch)) throw new TransportError('Invalid batch structure')

    for (const [peerId] of this.connectedPeers) {
      const peer = this.network.getTransport(peerId)
      if (peer?.isConnected) {
        this.network.scheduleMessage(this.localNodeId, peerId, 'batch', batch)
      }
    }
  }

  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    if (!isValidAck(ack)) throw new TransportError('Invalid ack structure')

    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)

    this.network.scheduleMessage(this.localNodeId, peerId, 'ack', ack)
  }

  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()
    if (!isValidForwardedTransaction(request)) throw new TransportError('Invalid forward request structure')

    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)

    return this.network.scheduleForward(this.localNodeId, peerId, request)
  }

  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncRequest(request)) throw new TransportError('Invalid sync request structure')
    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)
    this.network.scheduleMessage(this.localNodeId, peerId, 'sync_request', request)
  }

  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncBatch(batch)) throw new TransportError('Invalid sync batch structure')
    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)
    this.network.scheduleMessage(this.localNodeId, peerId, 'sync_batch', batch)
  }

  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncComplete(complete)) throw new TransportError('Invalid sync complete structure')
    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)
    this.network.scheduleMessage(this.localNodeId, peerId, 'sync_complete', complete)
  }

  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncAck(ack)) throw new TransportError('Invalid sync ack structure')
    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.isConnected) throw new TransportError(`Peer '${peerId}' is not connected`)
    this.network.scheduleMessage(this.localNodeId, peerId, 'sync_ack', ack)
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

  async deliverBatch(batch: ReplicationBatch, fromPeerId: string): Promise<void> {
    if (this.batchHandler) {
      await this.batchHandler(batch, fromPeerId)
    }
  }

  deliverAck(ack: ReplicationAck, fromPeerId: string): void {
    if (this.ackHandler) {
      this.ackHandler(ack, fromPeerId)
    }
  }

  async deliverForward(request: ForwardedTransaction, fromPeerId: string): Promise<ForwardedTransactionResult> {
    if (!this.forwardHandler) {
      throw new TransportError('No forward handler registered')
    }
    return this.forwardHandler(request, fromPeerId)
  }

  async deliverSyncRequest(request: SyncRequest, fromPeerId: string): Promise<void> {
    if (this.syncRequestHandler) await this.syncRequestHandler(request, fromPeerId)
  }

  async deliverSyncBatch(batch: SyncBatch, fromPeerId: string): Promise<void> {
    if (this.syncBatchHandler) await this.syncBatchHandler(batch, fromPeerId)
  }

  async deliverSyncComplete(complete: SyncComplete, fromPeerId: string): Promise<void> {
    if (this.syncCompleteHandler) await this.syncCompleteHandler(complete, fromPeerId)
  }

  deliverSyncAck(ack: SyncAck, fromPeerId: string): void {
    if (this.syncAckHandler) this.syncAckHandler(ack, fromPeerId)
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }

  private allPeerTransports(): Array<[string, SimulatedTransport]> {
    return this.network.connectedPeerTransports(this.localNodeId)
  }
}
