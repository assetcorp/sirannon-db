import type { RaftConfig, RaftMessage, ReplicationTransport } from '../types.js'

type RaftState = 'follower' | 'pre_candidate' | 'candidate' | 'leader'

const DEFAULT_ELECTION_TIMEOUT_MIN = 150
const DEFAULT_ELECTION_TIMEOUT_MAX = 300
const DEFAULT_HEARTBEAT_INTERVAL = 50

/**
 * Raft consensus node implementing leader election with the PreVote protocol.
 *
 * A RaftNode cycles through four states: follower, pre_candidate, candidate,
 * and leader. When the election timer fires on a follower, it enters the
 * pre_candidate phase and broadcasts a PreVote request at `currentTerm + 1`
 * without incrementing the term, preventing disruptive elections from
 * partitioned nodes. Only after receiving a majority of pre-votes does it
 * advance to candidate (incrementing the real term) and run a standard
 * RequestVote round.
 *
 * Vote deduplication uses a per-round `Set<string>` of voter IDs so that
 * retransmitted vote responses from the same peer are counted once.
 *
 * The leader sends periodic heartbeats on a configurable interval. Receiving
 * a heartbeat with a term >= the local term resets the election timer and
 * updates the known leader. Subscribe to leader changes via `onLeaderChange`.
 */
export class RaftNode {
  private readonly nodeId: string
  private readonly transport: ReplicationTransport
  private readonly electionTimeoutMin: number
  private readonly electionTimeoutMax: number
  private readonly heartbeatInterval: number

  private _state: RaftState = 'follower'
  private _currentTerm = 0
  private _votedFor: string | null = null
  private _leaderId: string | null = null
  private _running = false

  private electionTimer: ReturnType<typeof setTimeout> | null = null
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null

  private preVoters = new Set<string>()
  private voters = new Set<string>()
  private peerCount = 0

  private readonly randomFn: () => number
  private leaderChangeHandlers: Array<(leaderId: string | null) => void> = []

  constructor(nodeId: string, transport: ReplicationTransport, config?: RaftConfig) {
    this.nodeId = nodeId
    this.transport = transport
    this.electionTimeoutMin = config?.electionTimeoutMin ?? DEFAULT_ELECTION_TIMEOUT_MIN
    this.electionTimeoutMax = config?.electionTimeoutMax ?? DEFAULT_ELECTION_TIMEOUT_MAX
    this.heartbeatInterval = config?.heartbeatInterval ?? DEFAULT_HEARTBEAT_INTERVAL
    this.randomFn = config?.randomFn ?? Math.random

    this.transport.onRaftMessage((message, fromPeerId) => {
      if (this._running) {
        this.handleMessage(message, fromPeerId)
      }
    })
  }

  start(): void {
    if (this._running) return
    this._running = true
    this.peerCount = this.transport.peers().size
    this.becomeFollower(this._currentTerm)
  }

  stop(): void {
    if (!this._running) return
    this._running = false
    this.clearElectionTimer()
    this.clearHeartbeatTimer()
  }

  get state(): RaftState {
    return this._state
  }

  get currentTerm(): number {
    return this._currentTerm
  }

  get leaderId(): string | null {
    return this._leaderId
  }

  onLeaderChange(handler: (leaderId: string | null) => void): void {
    this.leaderChangeHandlers.push(handler)
  }

  handleMessage(message: RaftMessage, fromPeerId: string): void {
    if (message.type !== 'pre_vote' && message.type !== 'pre_vote_response') {
      if (message.term > this._currentTerm) {
        this.becomeFollower(message.term)
      }
    }

    this.peerCount = this.transport.peers().size

    switch (message.type) {
      case 'pre_vote':
        this.handlePreVote(message, fromPeerId)
        break
      case 'pre_vote_response':
        this.handleVoteResponse(message, fromPeerId)
        break
      case 'request_vote':
        this.handleRequestVote(message, fromPeerId)
        break
      case 'vote_response':
        this.handleVoteResponse(message, fromPeerId)
        break
      case 'heartbeat':
        this.handleHeartbeat(message, fromPeerId)
        break
      case 'append_entries':
        this.handleHeartbeat(message, fromPeerId)
        break
      case 'append_response':
        break
    }
  }

  private handlePreVote(message: RaftMessage, fromPeerId: string): void {
    const granted = message.term > this._currentTerm || (message.term >= this._currentTerm && this._leaderId === null)

    this.transport
      .sendRaftMessage(fromPeerId, {
        type: 'pre_vote_response',
        term: message.term,
        voteGranted: granted,
      })
      .catch(() => {})
  }

  private handleRequestVote(message: RaftMessage, fromPeerId: string): void {
    const candidateId = message.candidateId ?? fromPeerId
    let granted = false

    if (message.term >= this._currentTerm) {
      if (this._votedFor === null || this._votedFor === candidateId) {
        this._votedFor = candidateId
        granted = true
        this.resetElectionTimer()
      }
    }

    this.transport
      .sendRaftMessage(fromPeerId, {
        type: 'vote_response',
        term: this._currentTerm,
        voteGranted: granted,
      })
      .catch(() => {})
  }

  private handleVoteResponse(message: RaftMessage, fromPeerId: string): void {
    if (message.term < this._currentTerm) return

    if (this._state === 'pre_candidate' && message.voteGranted) {
      this.preVoters.add(fromPeerId)
      const majority = Math.floor((this.peerCount + 1) / 2) + 1
      if (this.preVoters.size >= majority) {
        this.becomeCandidate()
      }
      return
    }

    if (this._state === 'candidate' && message.voteGranted) {
      this.voters.add(fromPeerId)
      const majority = Math.floor((this.peerCount + 1) / 2) + 1
      if (this.voters.size >= majority) {
        this.becomeLeader()
      }
    }
  }

  private handleHeartbeat(message: RaftMessage, fromPeerId: string): void {
    if (message.term >= this._currentTerm) {
      const oldLeader = this._leaderId
      this._leaderId = message.leaderId ?? fromPeerId

      if (this._state !== 'follower') {
        this.becomeFollower(message.term)
      }

      this.resetElectionTimer()

      if (oldLeader !== this._leaderId) {
        this.notifyLeaderChange()
      }
    }
  }

  private becomeFollower(term: number): void {
    this._state = 'follower'
    this._currentTerm = term
    this._votedFor = null
    this.clearHeartbeatTimer()
    this.resetElectionTimer()
  }

  private becomePreCandidate(): void {
    this.peerCount = this.transport.peers().size

    if (this.peerCount === 0) {
      this.becomeCandidate()
      return
    }

    this._state = 'pre_candidate'
    this.preVoters.clear()
    this.preVoters.add(this.nodeId)

    this.transport
      .broadcastRaftMessage({
        type: 'pre_vote',
        term: this._currentTerm + 1,
        candidateId: this.nodeId,
      })
      .catch(() => {})

    this.resetElectionTimer()
  }

  private becomeCandidate(): void {
    this._state = 'candidate'
    this._currentTerm += 1
    this._votedFor = this.nodeId
    this.voters.clear()
    this.voters.add(this.nodeId)

    this.peerCount = this.transport.peers().size

    if (this.peerCount === 0) {
      this.becomeLeader()
      return
    }

    this.transport
      .broadcastRaftMessage({
        type: 'request_vote',
        term: this._currentTerm,
        candidateId: this.nodeId,
      })
      .catch(() => {})

    this.resetElectionTimer()
  }

  private becomeLeader(): void {
    this._state = 'leader'
    this._leaderId = this.nodeId
    this.clearElectionTimer()
    this.sendHeartbeat()
    this.startHeartbeatTimer()
    this.notifyLeaderChange()
  }

  private sendHeartbeat(): void {
    this.transport
      .broadcastRaftMessage({
        type: 'heartbeat',
        term: this._currentTerm,
        leaderId: this.nodeId,
      })
      .catch(() => {})
  }

  private resetElectionTimer(): void {
    this.clearElectionTimer()

    if (!this._running) return

    const timeout =
      this.electionTimeoutMin + Math.floor(this.randomFn() * (this.electionTimeoutMax - this.electionTimeoutMin))

    this.electionTimer = setTimeout(() => {
      if (!this._running) return
      if (this._state === 'follower' || this._state === 'pre_candidate') {
        this.becomePreCandidate()
      } else if (this._state === 'candidate') {
        this.becomeCandidate()
      }
    }, timeout)
    this.electionTimer.unref()
  }

  private clearElectionTimer(): void {
    if (this.electionTimer !== null) {
      clearTimeout(this.electionTimer)
      this.electionTimer = null
    }
  }

  private startHeartbeatTimer(): void {
    this.clearHeartbeatTimer()

    if (!this._running) return

    this.heartbeatTimer = setTimeout(() => {
      if (!this._running || this._state !== 'leader') return
      this.sendHeartbeat()
      this.startHeartbeatTimer()
    }, this.heartbeatInterval)
    this.heartbeatTimer.unref()
  }

  private clearHeartbeatTimer(): void {
    if (this.heartbeatTimer !== null) {
      clearTimeout(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  private notifyLeaderChange(): void {
    for (const handler of this.leaderChangeHandlers) {
      handler(this._leaderId)
    }
  }
}
