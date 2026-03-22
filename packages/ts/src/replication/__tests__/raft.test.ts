import { beforeEach, describe, expect, it, vi } from 'vitest'
import { RaftLog } from '../raft/raft-log.js'
import { RaftNode } from '../raft/raft-node.js'
import type {
  ForwardedTransaction,
  ForwardedTransactionResult,
  NodeInfo,
  RaftMessage,
  ReplicationAck,
  ReplicationBatch,
  ReplicationTransport,
  TransportConfig,
} from '../types.js'

class MockTransport implements ReplicationTransport {
  private raftHandler: ((msg: RaftMessage, from: string) => void) | null = null

  private readonly _peers = new Map<string, NodeInfo>()
  readonly sentRaftMessages: Array<{ peerId: string; message: RaftMessage }> = []
  readonly broadcastedRaftMessages: RaftMessage[] = []

  async connect(_localNodeId: string, _config: TransportConfig): Promise<void> {}
  async disconnect(): Promise<void> {}
  async send(_peerId: string, _batch: ReplicationBatch): Promise<void> {}
  async broadcast(_batch: ReplicationBatch): Promise<void> {}
  async sendAck(_peerId: string, _ack: ReplicationAck): Promise<void> {}
  async forward(_peerId: string, _request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    return { results: [], requestId: '' }
  }

  onBatchReceived(handler: (batch: ReplicationBatch, from: string) => Promise<void>): void {
    this.batchHandler = handler
  }
  onAckReceived(handler: (ack: ReplicationAck, from: string) => void): void {
    this.ackHandler = handler
  }
  onForwardReceived(handler: (req: ForwardedTransaction, from: string) => Promise<ForwardedTransactionResult>): void {
    this.forwardHandler = handler
  }

  async sendRaftMessage(peerId: string, message: RaftMessage): Promise<void> {
    this.sentRaftMessages.push({ peerId, message })
  }

  async broadcastRaftMessage(message: RaftMessage): Promise<void> {
    this.broadcastedRaftMessages.push(message)
  }

  onRaftMessage(handler: (message: RaftMessage, fromPeerId: string) => void): void {
    this.raftHandler = handler
  }

  onPeerConnected(handler: (peer: NodeInfo) => void): void {
    this.peerConnectedHandler = handler
  }

  onPeerDisconnected(handler: (peerId: string) => void): void {
    this.peerDisconnectedHandler = handler
  }

  peers(): ReadonlyMap<string, NodeInfo> {
    return this._peers
  }

  addPeer(id: string): void {
    this._peers.set(id, {
      id,
      role: 'peer',
      joinedAt: Date.now(),
      lastSeenAt: Date.now(),
      lastAckedSeq: 0n,
    })
  }

  deliverRaftMessage(message: RaftMessage, fromPeerId: string): void {
    if (this.raftHandler) {
      this.raftHandler(message, fromPeerId)
    }
  }
}

describe('RaftNode', () => {
  let transport: MockTransport

  beforeEach(() => {
    transport = new MockTransport()
  })

  it('starts as a follower', () => {
    const node = new RaftNode('node1', transport)
    node.start()
    expect(node.state).toBe('follower')
    expect(node.currentTerm).toBe(0)
    node.stop()
  })

  it('becomes leader with no peers (single node cluster)', async () => {
    vi.useFakeTimers()
    const node = new RaftNode('node1', transport, {
      electionTimeoutMin: 100,
      electionTimeoutMax: 100,
      heartbeatInterval: 50,
    })

    let leaderNotified: string | null = null
    node.onLeaderChange(leaderId => {
      leaderNotified = leaderId
    })

    node.start()
    vi.advanceTimersByTime(150)

    expect(node.state).toBe('leader')
    expect(node.leaderId).toBe('node1')
    expect(leaderNotified).toBe('node1')

    node.stop()
    vi.useRealTimers()
  })

  it('steps down to follower on receiving a higher term', () => {
    const node = new RaftNode('node1', transport)
    node.start()

    transport.deliverRaftMessage({ type: 'heartbeat', term: 5, leaderId: 'node2' }, 'node2')

    expect(node.state).toBe('follower')
    expect(node.currentTerm).toBe(5)
    expect(node.leaderId).toBe('node2')

    node.stop()
  })

  it('resets election timer on heartbeat', () => {
    vi.useFakeTimers()
    const node = new RaftNode('node1', transport, {
      electionTimeoutMin: 200,
      electionTimeoutMax: 200,
      heartbeatInterval: 50,
    })

    transport.addPeer('node2')
    node.start()

    vi.advanceTimersByTime(150)
    transport.deliverRaftMessage({ type: 'heartbeat', term: 1, leaderId: 'node2' }, 'node2')

    vi.advanceTimersByTime(150)
    expect(node.state).toBe('follower')

    node.stop()
    vi.useRealTimers()
  })

  it('broadcasts pre-vote requests before becoming candidate', () => {
    vi.useFakeTimers()
    transport.addPeer('node2')
    transport.addPeer('node3')

    const node = new RaftNode('node1', transport, {
      electionTimeoutMin: 100,
      electionTimeoutMax: 100,
      heartbeatInterval: 50,
    })

    node.start()
    vi.advanceTimersByTime(150)

    const preVotes = transport.broadcastedRaftMessages.filter(m => m.type === 'request_vote')
    expect(preVotes.length).toBeGreaterThan(0)

    node.stop()
    vi.useRealTimers()
  })
})

describe('RaftLog', () => {
  it('appends entries and retrieves them', () => {
    const log = new RaftLog()
    const idx = log.append({ term: 1, data: 'cmd1' })
    expect(idx).toBe(1)
    expect(log.getEntry(1)).toEqual({ term: 1, data: 'cmd1' })
  })

  it('tracks last index and term', () => {
    const log = new RaftLog()
    log.append({ term: 1, data: 'a' })
    log.append({ term: 2, data: 'b' })
    expect(log.getLastIndex()).toBe(2)
    expect(log.getLastTerm()).toBe(2)
  })

  it('returns 0 for empty log last index and term', () => {
    const log = new RaftLog()
    expect(log.getLastIndex()).toBe(0)
    expect(log.getLastTerm()).toBe(0)
  })

  it('returns undefined for out-of-range getEntry', () => {
    const log = new RaftLog()
    expect(log.getEntry(0)).toBeUndefined()
    expect(log.getEntry(1)).toBeUndefined()
  })

  it('gets entries from a start index', () => {
    const log = new RaftLog()
    log.append({ term: 1, data: 'a' })
    log.append({ term: 1, data: 'b' })
    log.append({ term: 2, data: 'c' })

    const entries = log.getEntriesFrom(2)
    expect(entries).toHaveLength(2)
    expect(entries[0].data).toBe('b')
  })

  it('truncates entries from a given index', () => {
    const log = new RaftLog()
    log.append({ term: 1, data: 'a' })
    log.append({ term: 1, data: 'b' })
    log.append({ term: 2, data: 'c' })

    log.truncateFrom(2)
    expect(log.getLastIndex()).toBe(1)
    expect(log.getEntry(2)).toBeUndefined()
  })

  it('manages commit index', () => {
    const log = new RaftLog()
    expect(log.commitIndex).toBe(0)
    log.setCommitIndex(5)
    expect(log.commitIndex).toBe(5)
    log.setCommitIndex(3)
    expect(log.commitIndex).toBe(5)
  })
})
