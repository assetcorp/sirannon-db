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
} from '../simulated/handler-types.js'
import {
  isValidAck,
  isValidBatch,
  isValidForwardedTransaction,
  isValidSyncAck,
  isValidSyncBatch,
  isValidSyncComplete,
  isValidSyncRequest,
} from '../validators.js'
import type { MemoryBus } from './bus.js'

/**
 * In-process ReplicationTransport for testing and single-process multi-node
 * scenarios.
 *
 * Messages between peers are delivered through a shared MemoryBus via
 * microtask scheduling (`queueMicrotask`), preserving the async delivery
 * semantics of a real network transport while avoiding actual I/O. All
 * message types (batches, acks, forwards) go through runtime
 * validation before delivery, and malformed payloads are silently dropped
 * to match the behavior of a lossy network.
 *
 * @public
 */
export class InMemoryTransport implements ReplicationTransport {
  private localNodeId = ''
  private localRole: 'primary' | 'replica' = 'replica'
  private localGroupId: string | undefined
  private localPrimaryTerm: bigint | undefined
  private localProtocolVersion: string | undefined
  private connected = false
  private readonly bus: MemoryBus
  private readonly connectedPeers = new Map<string, NodeInfo>()

  private batchHandler: BatchHandler | null = null
  private ackHandler: AckHandler | null = null
  private forwardHandler: ForwardHandler | null = null
  private peerConnectedHandler: PeerConnectedHandler | null = null
  private peerDisconnectedHandler: PeerDisconnectedHandler | null = null
  private syncRequestHandler: SyncRequestHandler | null = null
  private syncBatchHandler: SyncBatchHandler | null = null
  private syncCompleteHandler: SyncCompleteHandler | null = null
  private syncAckHandler: SyncAckHandler | null = null

  constructor(bus: MemoryBus) {
    this.bus = bus
  }

  private get role(): 'primary' | 'replica' {
    return this.localRole
  }

  /** Connects to the configured peers and announces this node. */
  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }

    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'
    this.localGroupId = config.groupId
    this.localPrimaryTerm = config.primaryTerm
    this.localProtocolVersion = config.protocolVersion
    this.connected = true
    this.bus.join(localNodeId, this)

    for (const peerId of this.bus.peerIds()) {
      if (peerId === localNodeId) continue

      const peerTransport = this.bus.getTransport(peerId)
      if (!peerTransport || !peerTransport.connected) continue

      const peerInfo: NodeInfo = {
        id: peerId,
        groupId: peerTransport.localGroupId,
        role: peerTransport.role,
        primaryTerm: peerTransport.localPrimaryTerm,
        protocolVersion: peerTransport.localProtocolVersion,
        joinedAt: Date.now(),
        lastSeenAt: Date.now(),
        lastAckedSeq: 0n,
      }
      this.connectedPeers.set(peerId, peerInfo)

      const localInfo: NodeInfo = {
        id: localNodeId,
        groupId: this.localGroupId,
        role: this.localRole,
        primaryTerm: this.localPrimaryTerm,
        protocolVersion: this.localProtocolVersion,
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

  /** Closes every peer connection. */
  async disconnect(): Promise<void> {
    if (!this.connected) return

    this.connected = false
    this.bus.leave(this.localNodeId)

    for (const [peerId] of this.connectedPeers) {
      const peerTransport = this.bus.getTransport(peerId)
      if (peerTransport) {
        peerTransport.connectedPeers.delete(this.localNodeId)
        if (peerTransport.peerDisconnectedHandler) {
          peerTransport.peerDisconnectedHandler(this.localNodeId)
        }
      }
    }

    this.connectedPeers.clear()
  }

  /** Sends one batch of changes to one peer. */
  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidBatch(batch)) {
      throw new TransportError('Invalid batch structure')
    }
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveBatch(batch, fromPeerId).catch(() => {})
    })
  }

  /** Sends one batch of changes to every connected peer. */
  async broadcast(batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidBatch(batch)) {
      throw new TransportError('Invalid batch structure')
    }
    const fromPeerId = this.localNodeId
    for (const [peerId] of this.connectedPeers) {
      const peer = this.bus.getTransport(peerId)
      if (peer?.connected) {
        queueMicrotask(() => {
          peer._receiveBatch(batch, fromPeerId).catch(() => {})
        })
      }
    }
  }

  /** Confirms to a peer that this node applied one of its batches. */
  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    if (!isValidAck(ack)) {
      throw new TransportError('Invalid ack structure')
    }
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveAck(ack, fromPeerId)
    })
  }

  /** Sends a write to the primary and waits for its result. */
  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()
    if (!isValidForwardedTransaction(request)) {
      throw new TransportError('Invalid forward request structure')
    }
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }
    return peer._receiveForward(request, this.localNodeId)
  }

  /** Asks a peer to stream a full copy of the database. */
  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncRequest(request)) throw new TransportError('Invalid sync request structure')
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)
    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveSyncRequest(request, fromPeerId).catch(() => {})
    })
  }

  /** Sends one page of first-sync table data. */
  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncBatch(batch)) throw new TransportError('Invalid sync batch structure')
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)
    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveSyncBatch(batch, fromPeerId).catch(() => {})
    })
  }

  /** Tells a joining node that first sync has finished, and sends the manifests to verify it. */
  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncComplete(complete)) throw new TransportError('Invalid sync complete structure')
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)
    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveSyncComplete(complete, fromPeerId).catch(() => {})
    })
  }

  /** Confirms to the source that a joining node stored one first-sync page. */
  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.ensureConnected()
    if (!isValidSyncAck(ack)) throw new TransportError('Invalid sync ack structure')
    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)
    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveSyncAck(ack, fromPeerId)
    })
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

  private async _receiveBatch(batch: ReplicationBatch, fromPeerId: string): Promise<void> {
    if (this.batchHandler) {
      await this.batchHandler(batch, fromPeerId)
    }
  }

  private _receiveAck(ack: ReplicationAck, fromPeerId: string): void {
    if (this.ackHandler) {
      this.ackHandler(ack, fromPeerId)
    }
  }

  private async _receiveForward(
    request: ForwardedTransaction,
    fromPeerId: string,
  ): Promise<ForwardedTransactionResult> {
    if (!this.forwardHandler) {
      throw new TransportError('No forward handler registered')
    }
    return this.forwardHandler(request, fromPeerId)
  }

  private async _receiveSyncRequest(request: SyncRequest, fromPeerId: string): Promise<void> {
    if (this.syncRequestHandler) await this.syncRequestHandler(request, fromPeerId)
  }

  private async _receiveSyncBatch(batch: SyncBatch, fromPeerId: string): Promise<void> {
    if (this.syncBatchHandler) await this.syncBatchHandler(batch, fromPeerId)
  }

  private async _receiveSyncComplete(complete: SyncComplete, fromPeerId: string): Promise<void> {
    if (this.syncCompleteHandler) await this.syncCompleteHandler(complete, fromPeerId)
  }

  private _receiveSyncAck(ack: SyncAck, fromPeerId: string): void {
    if (this.syncAckHandler) this.syncAckHandler(ack, fromPeerId)
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }
}
