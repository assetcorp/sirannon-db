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
import type { FaultPolicy } from './fault-policy.js'
import type { DeterministicScheduler, ScheduledEvent } from './scheduler.js'

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

type BatchHandler = (batch: ReplicationBatch, fromPeerId: string) => Promise<void>
type AckHandler = (ack: ReplicationAck, fromPeerId: string) => void
type ForwardHandler = (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>
type RaftHandler = (message: RaftMessage, fromPeerId: string) => void
type PeerConnectedHandler = (peer: NodeInfo) => void
type PeerDisconnectedHandler = (peerId: string) => void

export class SimulatedNetwork {
  private readonly transports = new Map<string, SimulatedTransport>()
  private readonly scheduler: DeterministicScheduler
  private readonly faultPolicy: FaultPolicy
  private readonly forwardTimeoutMs: number

  constructor(scheduler: DeterministicScheduler, faultPolicy: FaultPolicy, forwardTimeoutMs = 30_000) {
    this.scheduler = scheduler
    this.faultPolicy = faultPolicy
    this.forwardTimeoutMs = forwardTimeoutMs
    this.scheduler.setDeliverFn(event => this.deliverEvent(event))
  }

  createTransport(): SimulatedTransport {
    return new SimulatedTransport(this)
  }

  getTransport(nodeId: string): SimulatedTransport | undefined {
    return this.transports.get(nodeId)
  }

  get policy(): FaultPolicy {
    return this.faultPolicy
  }

  connectedPeerTransports(excludingNodeId: string): Array<[string, SimulatedTransport]> {
    const peers: Array<[string, SimulatedTransport]> = []
    for (const [peerId, transport] of this.transports) {
      if (peerId !== excludingNodeId && transport.isConnected) {
        peers.push([peerId, transport])
      }
    }
    return peers
  }

  register(nodeId: string, transport: SimulatedTransport): void {
    this.transports.set(nodeId, transport)
  }

  unregister(nodeId: string): void {
    this.transports.delete(nodeId)
  }

  scheduleMessage(from: string, to: string, kind: string, payload: unknown): void {
    if (this.faultPolicy.shouldDrop(from, to, kind as 'batch' | 'ack' | 'raft')) return
    const latency = this.faultPolicy.sampleLatency(from, to, kind as 'batch' | 'ack' | 'raft')
    this.scheduler.enqueue({
      deliverAt: this.scheduler.now + latency,
      from,
      to,
      kind,
      payload,
    })
  }

  scheduleForward(from: string, to: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    return new Promise<ForwardedTransactionResult>((resolve, reject) => {
      if (this.faultPolicy.shouldDrop(from, to, 'forward_request')) {
        const timer = setTimeout(() => {
          reject(new TransportError('Forward request timed out (dropped by fault policy)'))
        }, this.forwardTimeoutMs)
        if (typeof timer === 'object' && 'unref' in timer) timer.unref()
        return
      }

      const latency = this.faultPolicy.sampleLatency(from, to, 'forward_request')
      this.scheduler.enqueue({
        deliverAt: this.scheduler.now + latency,
        from,
        to,
        kind: 'forward_request',
        payload: { request, resolve, reject },
      })
    })
  }

  private async deliverEvent(event: ScheduledEvent): Promise<void> {
    const target = this.transports.get(event.to)
    if (!target || !target.isConnected) return

    switch (event.kind) {
      case 'batch':
        try {
          await target.deliverBatch(event.payload as ReplicationBatch, event.from)
        } catch {
          /* swallow errors; matches InMemoryTransport fire-and-forget semantics */
        }
        break

      case 'ack':
        try {
          target.deliverAck(event.payload as ReplicationAck, event.from)
        } catch {
          /* swallow */
        }
        break

      case 'raft':
        try {
          target.deliverRaftMessage(event.payload as RaftMessage, event.from)
        } catch {
          /* swallow */
        }
        break

      case 'forward_request': {
        const fwd = event.payload as {
          request: ForwardedTransaction
          resolve: (v: ForwardedTransactionResult) => void
          reject: (e: Error) => void
        }
        try {
          const result = await target.deliverForward(fwd.request, event.from)
          if (this.faultPolicy.shouldDrop(event.to, event.from, 'forward_response')) {
            const timer = setTimeout(() => {
              fwd.reject(new TransportError('Forward response timed out (dropped by fault policy)'))
            }, this.forwardTimeoutMs)
            if (typeof timer === 'object' && 'unref' in timer) timer.unref()
            return
          }
          const latency = this.faultPolicy.sampleLatency(event.to, event.from, 'forward_response')
          this.scheduler.enqueue({
            deliverAt: this.scheduler.now + latency,
            from: event.to,
            to: event.from,
            kind: 'forward_response',
            payload: { result, resolve: fwd.resolve },
          })
        } catch (err) {
          fwd.reject(err instanceof Error ? err : new TransportError(String(err)))
        }
        break
      }

      case 'forward_response': {
        const resp = event.payload as {
          result: ForwardedTransactionResult
          resolve: (v: ForwardedTransactionResult) => void
        }
        resp.resolve(resp.result)
        break
      }
    }
  }
}

export class SimulatedTransport implements ReplicationTransport {
  private localNodeId = ''
  private connected = false
  private readonly network: SimulatedNetwork
  private readonly connectedPeers = new Map<string, NodeInfo>()

  private batchHandler: BatchHandler | null = null
  private ackHandler: AckHandler | null = null
  private forwardHandler: ForwardHandler | null = null
  private raftHandler: RaftHandler | null = null
  private peerConnectedHandler: PeerConnectedHandler | null = null
  private peerDisconnectedHandler: PeerDisconnectedHandler | null = null

  constructor(network: SimulatedNetwork) {
    this.network = network
  }

  get isConnected(): boolean {
    return this.connected
  }

  get nodeId(): string {
    return this.localNodeId
  }

  async connect(localNodeId: string, _config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }

    this.localNodeId = localNodeId
    this.connected = true
    this.network.register(localNodeId, this)

    for (const [peerId, peerTransport] of this.allPeerTransports()) {
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
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)

    this.network.scheduleMessage(this.localNodeId, peerId, 'batch', batch)
  }

  async broadcast(batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    if (!isValidBatch(batch)) throw new TransportError('Invalid batch structure')

    for (const [peerId] of this.connectedPeers) {
      const peer = this.network.getTransport(peerId)
      if (peer?.connected) {
        this.network.scheduleMessage(this.localNodeId, peerId, 'batch', batch)
      }
    }
  }

  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    if (!isValidAck(ack)) throw new TransportError('Invalid ack structure')

    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)

    this.network.scheduleMessage(this.localNodeId, peerId, 'ack', ack)
  }

  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()
    if (!isValidForwardedTransaction(request)) throw new TransportError('Invalid forward request structure')

    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)

    return this.network.scheduleForward(this.localNodeId, peerId, request)
  }

  async sendRaftMessage(peerId: string, message: RaftMessage): Promise<void> {
    this.ensureConnected()
    if (!isValidRaftMessage(message)) throw new TransportError('Invalid raft message structure')

    const peer = this.network.getTransport(peerId)
    if (!peer || !peer.connected) throw new TransportError(`Peer '${peerId}' is not connected`)

    this.network.scheduleMessage(this.localNodeId, peerId, 'raft', message)
  }

  async broadcastRaftMessage(message: RaftMessage): Promise<void> {
    this.ensureConnected()
    if (!isValidRaftMessage(message)) throw new TransportError('Invalid raft message structure')

    for (const [peerId] of this.connectedPeers) {
      const peer = this.network.getTransport(peerId)
      if (peer?.connected) {
        this.network.scheduleMessage(this.localNodeId, peerId, 'raft', message)
      }
    }
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

  deliverRaftMessage(message: RaftMessage, fromPeerId: string): void {
    if (this.raftHandler) {
      this.raftHandler(message, fromPeerId)
    }
  }

  async deliverForward(request: ForwardedTransaction, fromPeerId: string): Promise<ForwardedTransactionResult> {
    if (!this.forwardHandler) {
      throw new TransportError('No forward handler registered')
    }
    return this.forwardHandler(request, fromPeerId)
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

export type { ConvergenceResult, NodeSnapshot, RowDivergence, TableDivergence, TableSnapshot } from './convergence.js'
export { ConvergenceOracle } from './convergence.js'
export type { EventKind, FaultPolicyConfig } from './fault-policy.js'
export { FaultPolicy } from './fault-policy.js'
export { SeededPRNG } from './prng.js'
export type { ScheduledEvent } from './scheduler.js'
export { DeterministicScheduler } from './scheduler.js'
