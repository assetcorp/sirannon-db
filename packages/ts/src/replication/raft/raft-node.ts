import type { RaftConfig, RaftMessage, ReplicationTransport } from '../types.js'

type RaftState = 'follower' | 'pre_candidate' | 'candidate' | 'leader'

const DEFAULT_ELECTION_TIMEOUT_MIN = 150
const DEFAULT_ELECTION_TIMEOUT_MAX = 300
const DEFAULT_HEARTBEAT_INTERVAL = 50

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

  private preVotesReceived = 0
  private votesReceived = 0
  private peerCount = 0

  private leaderChangeHandlers: Array<(leaderId: string | null) => void> = []

  constructor(nodeId: string, transport: ReplicationTransport, config?: RaftConfig) {
    this.nodeId = nodeId
    this.transport = transport
    this.electionTimeoutMin = config?.electionTimeoutMin ?? DEFAULT_ELECTION_TIMEOUT_MIN
    this.electionTimeoutMax = config?.electionTimeoutMax ?? DEFAULT_ELECTION_TIMEOUT_MAX
    this.heartbeatInterval = config?.heartbeatInterval ?? DEFAULT_HEARTBEAT_INTERVAL

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
    if (message.term > this._currentTerm) {
      this.becomeFollower(message.term)
    }

    this.peerCount = this.transport.peers().size

    switch (message.type) {
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

    this.transport.sendRaftMessage(fromPeerId, {
      type: 'vote_response',
      term: this._currentTerm,
      voteGranted: granted,
    })
  }

  private handleVoteResponse(message: RaftMessage, _fromPeerId: string): void {
    if (message.term < this._currentTerm) return

    if (this._state === 'pre_candidate' && message.voteGranted) {
      this.preVotesReceived += 1
      const majority = Math.floor(this.peerCount / 2) + 1
      if (this.preVotesReceived >= majority) {
        this.becomeCandidate()
      }
      return
    }

    if (this._state === 'candidate' && message.voteGranted) {
      this.votesReceived += 1
      const majority = Math.floor((this.peerCount + 1) / 2) + 1
      if (this.votesReceived >= majority) {
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
    this.preVotesReceived = 0

    this.transport.broadcastRaftMessage({
      type: 'request_vote',
      term: this._currentTerm + 1,
      candidateId: this.nodeId,
    })

    this.resetElectionTimer()
  }

  private becomeCandidate(): void {
    this._state = 'candidate'
    this._currentTerm += 1
    this._votedFor = this.nodeId
    this.votesReceived = 1

    this.peerCount = this.transport.peers().size

    if (this.peerCount === 0) {
      this.becomeLeader()
      return
    }

    this.transport.broadcastRaftMessage({
      type: 'request_vote',
      term: this._currentTerm,
      candidateId: this.nodeId,
    })

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
    this.transport.broadcastRaftMessage({
      type: 'heartbeat',
      term: this._currentTerm,
      leaderId: this.nodeId,
    })
  }

  private resetElectionTimer(): void {
    this.clearElectionTimer()

    if (!this._running) return

    const timeout =
      this.electionTimeoutMin + Math.floor(Math.random() * (this.electionTimeoutMax - this.electionTimeoutMin))

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
