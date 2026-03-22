import { TransportError } from '../../replication/errors.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  RaftMessage,
  ReplicationAck,
  ReplicationBatch,
  ReplicationTransport,
  TransportConfig,
} from '../../replication/types.js'

/**
 * Shared message bus that connects InMemoryTransport instances within the
 * same process. Each transport registers itself on `connect()` and messages
 * are delivered via direct method calls on the target transport through
 * microtask scheduling, simulating async network delivery without actual I/O.
 */
export class MemoryBus {
  private readonly transports = new Map<string, InMemoryTransport>()

  join(peerId: string, transport: InMemoryTransport): void {
    this.transports.set(peerId, transport)
  }

  leave(peerId: string): void {
    this.transports.delete(peerId)
  }

  getTransport(peerId: string): InMemoryTransport | undefined {
    return this.transports.get(peerId)
  }

  peerIds(): IterableIterator<string> {
    return this.transports.keys()
  }

  get size(): number {
    return this.transports.size
  }
}

type BatchHandler = (batch: ReplicationBatch, fromPeerId: string) => Promise<void>
type AckHandler = (ack: ReplicationAck, fromPeerId: string) => void
type ForwardHandler = (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>
type RaftHandler = (message: RaftMessage, fromPeerId: string) => void
type PeerConnectedHandler = (peer: NodeInfo) => void
type PeerDisconnectedHandler = (peerId: string) => void

function isValidBatch(batch: unknown): batch is ReplicationBatch {
  if (typeof batch !== 'object' || batch === null) return false
  const b = batch as Record<string, unknown>
  return (
    typeof b.sourceNodeId === 'string' &&
    typeof b.batchId === 'string' &&
    typeof b.fromSeq === 'bigint' &&
    typeof b.toSeq === 'bigint' &&
    typeof b.checksum === 'string' &&
    Array.isArray(b.changes) &&
    typeof b.hlcRange === 'object' &&
    b.hlcRange !== null
  )
}

function isValidAck(ack: unknown): ack is ReplicationAck {
  if (typeof ack !== 'object' || ack === null) return false
  const a = ack as Record<string, unknown>
  return typeof a.batchId === 'string' && typeof a.ackedSeq === 'bigint' && typeof a.nodeId === 'string'
}

function isValidRaftMessage(msg: unknown): msg is RaftMessage {
  if (typeof msg !== 'object' || msg === null) return false
  const m = msg as Record<string, unknown>
  return typeof m.type === 'string' && typeof m.term === 'number'
}

function isValidForwardedTransaction(req: unknown): req is ForwardedTransaction {
  if (typeof req !== 'object' || req === null) return false
  const r = req as Record<string, unknown>
  return typeof r.requestId === 'string' && Array.isArray(r.statements)
}

/**
 * In-process ReplicationTransport for testing and single-process multi-node
 * scenarios.
 *
 * Messages between peers are delivered through a shared MemoryBus via
 * microtask scheduling (`queueMicrotask`), preserving the async delivery
 * semantics of a real network transport while avoiding actual I/O. All
 * message types (batches, acks, forwards, Raft messages) go through runtime
 * validation before delivery, and malformed payloads are silently dropped
 * to match the behavior of a lossy network.
 */
export class InMemoryTransport implements ReplicationTransport {
  private localNodeId = ''
  private connected = false
  private readonly bus: MemoryBus
  private readonly connectedPeers = new Map<string, NodeInfo>()

  private batchHandler: BatchHandler | null = null
  private ackHandler: AckHandler | null = null
  private forwardHandler: ForwardHandler | null = null
  private raftHandler: RaftHandler | null = null
  private peerConnectedHandler: PeerConnectedHandler | null = null
  private peerDisconnectedHandler: PeerDisconnectedHandler | null = null

  constructor(bus: MemoryBus) {
    this.bus = bus
  }

  async connect(localNodeId: string, _config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }

    this.localNodeId = localNodeId
    this.connected = true
    this.bus.join(localNodeId, this)

    for (const peerId of this.bus.peerIds()) {
      if (peerId === localNodeId) continue

      const peerTransport = this.bus.getTransport(peerId)
      if (!peerTransport || !peerTransport.connected) continue

      const peerInfo: NodeInfo = {
        id: peerId,
        role: 'peer',
        joinedAt: Date.now(),
        lastSeenAt: Date.now(),
        lastAckedSeq: 0n,
      }
      this.connectedPeers.set(peerId, peerInfo)

      const localInfo: NodeInfo = {
        id: localNodeId,
        role: 'peer',
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

  onBatchReceived(handler: BatchHandler): void {
    this.batchHandler = handler
  }

  onAckReceived(handler: AckHandler): void {
    this.ackHandler = handler
  }

  onForwardReceived(handler: ForwardHandler): void {
    this.forwardHandler = handler
  }

  async sendRaftMessage(peerId: string, message: RaftMessage): Promise<void> {
    this.ensureConnected()

    if (!isValidRaftMessage(message)) {
      throw new TransportError('Invalid raft message structure')
    }

    const peer = this.bus.getTransport(peerId)
    if (!peer || !peer.connected) {
      throw new TransportError(`Peer '${peerId}' is not connected`)
    }

    const fromPeerId = this.localNodeId
    queueMicrotask(() => {
      peer._receiveRaftMessage(message, fromPeerId)
    })
  }

  async broadcastRaftMessage(message: RaftMessage): Promise<void> {
    this.ensureConnected()

    if (!isValidRaftMessage(message)) {
      throw new TransportError('Invalid raft message structure')
    }

    const fromPeerId = this.localNodeId
    for (const [peerId] of this.connectedPeers) {
      const peer = this.bus.getTransport(peerId)
      if (peer?.connected) {
        queueMicrotask(() => {
          peer._receiveRaftMessage(message, fromPeerId)
        })
      }
    }
  }

  onRaftMessage(handler: RaftHandler): void {
    this.raftHandler = handler
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

  async _receiveBatch(batch: ReplicationBatch, fromPeerId: string): Promise<void> {
    if (this.batchHandler) {
      await this.batchHandler(batch, fromPeerId)
    }
  }

  _receiveAck(ack: ReplicationAck, fromPeerId: string): void {
    if (this.ackHandler) {
      this.ackHandler(ack, fromPeerId)
    }
  }

  async _receiveForward(request: ForwardedTransaction, fromPeerId: string): Promise<ForwardedTransactionResult> {
    if (!this.forwardHandler) {
      throw new TransportError('No forward handler registered')
    }
    return this.forwardHandler(request, fromPeerId)
  }

  _receiveRaftMessage(message: RaftMessage, fromPeerId: string): void {
    if (this.raftHandler) {
      this.raftHandler(message, fromPeerId)
    }
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }
}
