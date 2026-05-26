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
} from '../../types.js'

export class MockTransport implements ReplicationTransport {
  private ackHandler: ((ack: ReplicationAck, from: string) => void) | null = null
  private peerConnectedHandler: ((peer: NodeInfo) => void) | null = null
  private peerDisconnectedHandler: ((peerId: string) => void) | null = null

  private readonly _peers = new Map<string, NodeInfo>()
  readonly sentBatches: Array<{ peerId: string; batch: ReplicationBatch }> = []
  connected = false
  sendShouldFail = false
  sendError = new Error('transport send failed')

  async connect(_localNodeId: string, _config: TransportConfig): Promise<void> {
    this.connected = true
  }
  async disconnect(): Promise<void> {
    this.connected = false
  }
  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    if (this.sendShouldFail) {
      throw this.sendError
    }
    this.sentBatches.push({ peerId, batch })
  }
  async broadcast(_batch: ReplicationBatch): Promise<void> {}
  async sendAck(_peerId: string, _ack: ReplicationAck): Promise<void> {}
  async forward(_peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    return { results: [{ changes: 1, lastInsertRowId: 1 }], requestId: request.requestId }
  }

  onBatchReceived(_handler: (batch: ReplicationBatch, from: string) => Promise<void>): void {}
  onAckReceived(handler: (ack: ReplicationAck, from: string) => void): void {
    this.ackHandler = handler
  }
  onForwardReceived(_handler: (req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>): void {}
  async requestSync(_peerId: string, _request: SyncRequest): Promise<void> {}
  async sendSyncBatch(_peerId: string, _batch: SyncBatch): Promise<void> {}
  async sendSyncComplete(_peerId: string, _complete: SyncComplete): Promise<void> {}
  async sendSyncAck(_peerId: string, _ack: SyncAck): Promise<void> {}
  onSyncRequested(_handler: (request: SyncRequest, fromPeerId: string) => Promise<void>): void {}
  onSyncBatchReceived(_handler: (batch: SyncBatch, fromPeerId: string) => Promise<void>): void {}
  onSyncCompleteReceived(_handler: (complete: SyncComplete, fromPeerId: string) => Promise<void>): void {}
  onSyncAckReceived(_handler: (ack: SyncAck, fromPeerId: string) => void): void {}
  onPeerConnected(handler: (peer: NodeInfo) => void): void {
    this.peerConnectedHandler = handler
  }
  onPeerDisconnected(handler: (peerId: string) => void): void {
    this.peerDisconnectedHandler = handler
  }

  peers(): ReadonlyMap<string, NodeInfo> {
    return this._peers
  }

  addPeer(id: string, role: 'primary' | 'replica' = 'replica'): void {
    this._peers.set(id, {
      id,
      role,
      joinedAt: Date.now(),
      lastSeenAt: Date.now(),
      lastAckedSeq: 0n,
    })
    if (this.peerConnectedHandler) {
      const peer = this._peers.get(id)
      if (peer) {
        this.peerConnectedHandler(peer)
      }
    }
  }

  removePeer(id: string): void {
    this._peers.delete(id)
    if (this.peerDisconnectedHandler) {
      this.peerDisconnectedHandler(id)
    }
  }

  triggerAckReceived(ack: ReplicationAck, from: string): void {
    if (this.ackHandler) {
      this.ackHandler(ack, from)
    }
  }
}

export const NODE_A = 'aaaa0000aaaa0000aaaa0000aaaa0000'
export const NODE_B = 'bbbb0000bbbb0000bbbb0000bbbb0000'
