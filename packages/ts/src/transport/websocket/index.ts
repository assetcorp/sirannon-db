import uWS from 'uWebSockets.js'
import { timingSafeEqual } from 'node:crypto'
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
import {
  deserializeBatch,
  deserializeSyncComplete,
  serializeBatch,
  serializeSyncComplete,
} from '../../replication/types.js'

export interface WebSocketReplicationOptions {
  port?: number
  host?: string
  tls?: { key: string; cert: string }
  maxPayloadLength?: number
  reconnectInitialDelay?: number
  reconnectMaxDelay?: number
  reconnectMultiplier?: number
  forwardTimeout?: number
  authToken?: string
}

interface ReplicationMessage {
  type:
    | 'batch'
    | 'ack'
    | 'forward_request'
    | 'forward_response'
    | 'hello'
    | 'sync_request'
    | 'sync_batch'
    | 'sync_complete'
    | 'sync_ack'
  payload: unknown
}

interface HelloPayload {
  nodeId: string
  role: 'primary' | 'replica'
  authToken?: string
}

interface ForwardResponsePayload {
  requestId: string
  result?: ForwardedTransactionResult
  error?: string
}

type BatchHandler = (batch: ReplicationBatch, fromPeerId: string) => Promise<void>
type AckHandler = (ack: ReplicationAck, fromPeerId: string) => void
type ForwardHandler = (request: ForwardedTransaction, fromPeerId: string) => Promise<ForwardedTransactionResult>
type PeerConnectedHandler = (peer: NodeInfo) => void
type PeerDisconnectedHandler = (peerId: string) => void

interface PendingForward {
  resolve: (result: ForwardedTransactionResult) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

interface PeerConnection {
  nodeId: string
  ws: WebSocket
  reconnectAttempts: number
  reconnectTimer: ReturnType<typeof setTimeout> | null
  endpoint: string
}

interface ServerPeerData {
  nodeId: string
}

const BIGINT_PREFIX = '\x00sirannon:bigint:'

function bigintReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    return `${BIGINT_PREFIX}${value.toString()}`
  }
  return value
}

function bigintReviver(_key: string, value: unknown): unknown {
  if (typeof value === 'string' && value.startsWith(BIGINT_PREFIX)) {
    return BigInt(value.slice(BIGINT_PREFIX.length))
  }
  return value
}

function serializeMessage(msg: ReplicationMessage): string {
  return JSON.stringify(msg, bigintReplacer)
}

function deserializeMessage(raw: string): ReplicationMessage | null {
  try {
    const parsed = JSON.parse(raw, bigintReviver) as unknown
    if (typeof parsed !== 'object' || parsed === null) return null
    const msg = parsed as Record<string, unknown>
    if (typeof msg.type !== 'string') return null
    return parsed as ReplicationMessage
  } catch {
    return null
  }
}

function isValidHello(payload: unknown): payload is HelloPayload {
  if (typeof payload !== 'object' || payload === null) return false
  const p = payload as Record<string, unknown>
  return typeof p.nodeId === 'string' && typeof p.role === 'string'
}

function isValidAck(payload: unknown): payload is ReplicationAck {
  if (typeof payload !== 'object' || payload === null) return false
  const p = payload as Record<string, unknown>
  return (
    typeof p.batchId === 'string' &&
    typeof p.nodeId === 'string' &&
    (typeof p.ackedSeq === 'bigint' || typeof p.ackedSeq === 'number')
  )
}

function isValidForwardRequest(payload: unknown): payload is ForwardedTransaction {
  if (typeof payload !== 'object' || payload === null) return false
  const p = payload as Record<string, unknown>
  return typeof p.requestId === 'string' && Array.isArray(p.statements)
}

function isValidForwardResponse(payload: unknown): payload is ForwardResponsePayload {
  if (typeof payload !== 'object' || payload === null) return false
  const p = payload as Record<string, unknown>
  return typeof p.requestId === 'string'
}

function isValidSyncRequest(payload: unknown): payload is SyncRequest {
  if (typeof payload !== 'object' || payload === null) return false
  const p = payload as Record<string, unknown>
  return typeof p.requestId === 'string' && typeof p.joinerNodeId === 'string' && Array.isArray(p.completedTables)
}

function isValidSyncBatch(payload: unknown): payload is SyncBatch {
  if (typeof payload !== 'object' || payload === null) return false
  const b = payload as Record<string, unknown>
  return (
    typeof b.requestId === 'string' &&
    typeof b.table === 'string' &&
    typeof b.batchIndex === 'number' &&
    Array.isArray(b.rows) &&
    typeof b.checksum === 'string' &&
    typeof b.isLastBatchForTable === 'boolean'
  )
}

function isValidSyncAck(payload: unknown): payload is SyncAck {
  if (typeof payload !== 'object' || payload === null) return false
  const a = payload as Record<string, unknown>
  return (
    typeof a.requestId === 'string' &&
    typeof a.joinerNodeId === 'string' &&
    typeof a.table === 'string' &&
    typeof a.batchIndex === 'number' &&
    typeof a.success === 'boolean'
  )
}

/**
 * WebSocket-based ReplicationTransport for production multi-node replication.
 *
 * Runs a Bun WebSocket server that peers connect to. Outbound messages
 * (batches, acks, Raft messages, forwards) are JSON-serialized with bigint
 * support and sent to the target peer's socket. Inbound messages are
 * validated, deserialized, and dispatched to the registered handlers.
 *
 * Peer identity is established during the WebSocket upgrade via the
 * `x-node-id` and `x-node-role` headers. An optional `authToken` provides
 * symmetric authentication: both sides must present matching tokens or the
 * connection is rejected with a 401 during the upgrade handshake.
 *
 * Message payload size is bounded by `maxPayloadLength` (default 10 MB).
 * Malformed or oversized messages are silently dropped to prevent a single
 * misbehaving peer from crashing the node.
 */
export class WebSocketReplicationTransport implements ReplicationTransport {
  private readonly options: Required<
    Pick<
      WebSocketReplicationOptions,
      | 'port'
      | 'host'
      | 'maxPayloadLength'
      | 'reconnectInitialDelay'
      | 'reconnectMaxDelay'
      | 'reconnectMultiplier'
      | 'forwardTimeout'
    >
  >
  private readonly tls: { key: string; cert: string } | undefined
  private readonly authToken: string | undefined

  private localNodeId = ''
  private localRole: TopologyRole = 'replica'
  private connected = false
  private listenSocket: uWS.us_listen_socket | null = null
  private readonly connectedPeers = new Map<string, NodeInfo>()
  private readonly peerConnections = new Map<string, PeerConnection>()
  private readonly serverPeerSockets = new Map<string, uWS.WebSocket<ServerPeerData>>()
  private readonly pendingForwards = new Map<string, PendingForward>()

  private batchHandler: BatchHandler | null = null
  private ackHandler: AckHandler | null = null
  private forwardHandler: ForwardHandler | null = null
  private peerConnectedHandler: PeerConnectedHandler | null = null
  private peerDisconnectedHandler: PeerDisconnectedHandler | null = null
  private syncRequestHandler: ((request: SyncRequest, fromPeerId: string) => Promise<void>) | null = null
  private syncBatchHandler: ((batch: SyncBatch, fromPeerId: string) => Promise<void>) | null = null
  private syncCompleteHandler: ((complete: SyncComplete, fromPeerId: string) => Promise<void>) | null = null
  private syncAckHandler: ((ack: SyncAck, fromPeerId: string) => void) | null = null

  constructor(options?: WebSocketReplicationOptions) {
    this.options = {
      port: options?.port ?? 0,
      host: options?.host ?? '127.0.0.1',
      maxPayloadLength: options?.maxPayloadLength ?? 16 * 1024 * 1024,
      reconnectInitialDelay: options?.reconnectInitialDelay ?? 100,
      reconnectMaxDelay: options?.reconnectMaxDelay ?? 30_000,
      reconnectMultiplier: options?.reconnectMultiplier ?? 2,
      forwardTimeout: options?.forwardTimeout ?? 30_000,
    }
    this.tls = options?.tls
    this.authToken = options?.authToken
  }

  async connect(localNodeId: string, config: TransportConfig): Promise<void> {
    if (this.connected) {
      throw new TransportError('Transport is already connected')
    }

    this.localNodeId = localNodeId
    this.localRole = config.localRole ?? 'replica'

    await this.startServer()
    this.connected = true

    const endpoints = config.endpoints ?? []
    for (const endpoint of endpoints) {
      this.connectToPeer(endpoint)
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) return
    this.connected = false

    for (const [, pending] of this.pendingForwards) {
      clearTimeout(pending.timer)
      pending.reject(new TransportError('Transport disconnected'))
    }
    this.pendingForwards.clear()

    for (const [, conn] of this.peerConnections) {
      if (conn.reconnectTimer) {
        clearTimeout(conn.reconnectTimer)
      }
      try {
        conn.ws.close()
      } catch {
        /* ws may already be closed */
      }
    }
    this.peerConnections.clear()

    for (const [, ws] of this.serverPeerSockets) {
      try {
        ws.end(1000, 'Transport disconnecting')
      } catch {
        /* socket may already be closed */
      }
    }
    this.serverPeerSockets.clear()

    if (this.listenSocket) {
      uWS.us_listen_socket_close(this.listenSocket)
      this.listenSocket = null
    }
    for (const [peerId] of this.connectedPeers) {
      if (this.peerDisconnectedHandler) {
        this.peerDisconnectedHandler(peerId)
      }
    }
    this.connectedPeers.clear()
  }

  async send(peerId: string, batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const serialized = serializeBatch(batch)
    const msg: ReplicationMessage = { type: 'batch', payload: serialized }
    this.sendToPeer(peerId, serializeMessage(msg))
  }

  async broadcast(batch: ReplicationBatch): Promise<void> {
    this.ensureConnected()
    const serialized = serializeBatch(batch)
    const msg: ReplicationMessage = { type: 'batch', payload: serialized }
    const raw = serializeMessage(msg)
    for (const [peerId] of this.connectedPeers) {
      try {
        this.sendToPeer(peerId, raw)
      } catch {
        /* peer may have disconnected */
      }
    }
  }

  async sendAck(peerId: string, ack: ReplicationAck): Promise<void> {
    this.ensureConnected()
    const msg: ReplicationMessage = { type: 'ack', payload: ack }
    this.sendToPeer(peerId, serializeMessage(msg))
  }

  async forward(peerId: string, request: ForwardedTransaction): Promise<ForwardedTransactionResult> {
    this.ensureConnected()

    const msg: ReplicationMessage = { type: 'forward_request', payload: request }
    this.sendToPeer(peerId, serializeMessage(msg))

    return new Promise<ForwardedTransactionResult>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingForwards.delete(request.requestId)
        reject(
          new TransportError(`Forward request '${request.requestId}' timed out after ${this.options.forwardTimeout}ms`),
        )
      }, this.options.forwardTimeout)
      timer.unref()

      this.pendingForwards.set(request.requestId, { resolve, reject, timer })
    })
  }

  async requestSync(peerId: string, request: SyncRequest): Promise<void> {
    this.ensureConnected()
    const msg: ReplicationMessage = { type: 'sync_request', payload: request }
    this.sendToPeer(peerId, serializeMessage(msg))
  }

  async sendSyncBatch(peerId: string, batch: SyncBatch): Promise<void> {
    this.ensureConnected()
    const msg: ReplicationMessage = { type: 'sync_batch', payload: batch }
    this.sendToPeer(peerId, serializeMessage(msg))
  }

  async sendSyncComplete(peerId: string, complete: SyncComplete): Promise<void> {
    this.ensureConnected()
    const serialized = serializeSyncComplete(complete)
    const msg: ReplicationMessage = { type: 'sync_complete', payload: serialized }
    this.sendToPeer(peerId, serializeMessage(msg))
  }

  async sendSyncAck(peerId: string, ack: SyncAck): Promise<void> {
    this.ensureConnected()
    const msg: ReplicationMessage = { type: 'sync_ack', payload: ack }
    this.sendToPeer(peerId, serializeMessage(msg))
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

  onSyncRequested(handler: (request: SyncRequest, fromPeerId: string) => Promise<void>): void {
    this.syncRequestHandler = handler
  }

  onSyncBatchReceived(handler: (batch: SyncBatch, fromPeerId: string) => Promise<void>): void {
    this.syncBatchHandler = handler
  }

  onSyncCompleteReceived(handler: (complete: SyncComplete, fromPeerId: string) => Promise<void>): void {
    this.syncCompleteHandler = handler
  }

  onSyncAckReceived(handler: (ack: SyncAck, fromPeerId: string) => void): void {
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

  get listeningPort(): number {
    if (!this.listenSocket) return -1
    return uWS.us_socket_local_port(this.listenSocket as unknown as uWS.us_socket)
  }

  private async startServer(): Promise<void> {
    const app = this.tls ? uWS.SSLApp({ key_file_name: this.tls.key, cert_file_name: this.tls.cert }) : uWS.App()

    app.ws<ServerPeerData>('/replication', {
      maxPayloadLength: this.options.maxPayloadLength,
      idleTimeout: 120,
      sendPingsAutomatically: true,

      open: _ws => {},

      message: (ws, message) => {
        const text = Buffer.from(message).toString('utf-8')
        const msg = deserializeMessage(text)
        if (!msg) return

        const userData = ws.getUserData()

        if (msg.type === 'hello') {
          if (!isValidHello(msg.payload)) return
          const hello = msg.payload

          if (!this.validateAuthToken(hello.authToken)) {
            ws.end(4001, 'Authentication failed')
            return
          }

          userData.nodeId = hello.nodeId
          this.serverPeerSockets.set(hello.nodeId, ws)

          const peerInfo: NodeInfo = {
            id: hello.nodeId,
            role: hello.role,
            joinedAt: Date.now(),
            lastSeenAt: Date.now(),
            lastAckedSeq: 0n,
          }
          this.connectedPeers.set(hello.nodeId, peerInfo)

          const helloBackPayload: HelloPayload = { nodeId: this.localNodeId, role: this.localRole }
          if (this.authToken) {
            helloBackPayload.authToken = this.authToken
          }
          const helloBack: ReplicationMessage = {
            type: 'hello',
            payload: helloBackPayload,
          }
          ws.send(serializeMessage(helloBack), false)

          if (this.peerConnectedHandler) {
            this.peerConnectedHandler(peerInfo)
          }
          return
        }

        if (!userData.nodeId) return

        this.handleMessage(msg, userData.nodeId)
      },

      close: ws => {
        const userData = ws.getUserData()
        if (!userData.nodeId) return

        if (this.serverPeerSockets.get(userData.nodeId) === ws) {
          this.serverPeerSockets.delete(userData.nodeId)
          this.removePeer(userData.nodeId)
        }
      },
    })

    return new Promise<void>((resolve, reject) => {
      app.listen(this.options.host, this.options.port, socket => {
        if (socket) {
          this.listenSocket = socket
          resolve()
        } else {
          reject(new TransportError(`Failed to start replication server on ${this.options.host}:${this.options.port}`))
        }
      })
    })
  }

  private connectToPeer(endpoint: string): void {
    const wsUrl = endpoint.replace(/^http:\/\//i, 'ws://').replace(/^https:\/\//i, 'wss://')
    const fullUrl = `${wsUrl}/replication`

    let ws: WebSocket
    try {
      ws = new WebSocket(fullUrl)
    } catch (err) {
      console.error(
        `[sirannon] WebSocket constructor failed for ${endpoint}:`,
        err instanceof Error ? err.message : err,
      )
      const failedConn: PeerConnection = {
        nodeId: '',
        ws: null as unknown as WebSocket,
        reconnectAttempts: 0,
        reconnectTimer: null,
        endpoint,
      }
      if (this.connected) {
        this.scheduleReconnect(failedConn)
      }
      return
    }

    const conn: PeerConnection = {
      nodeId: '',
      ws,
      reconnectAttempts: 0,
      reconnectTimer: null,
      endpoint,
    }

    ws.onopen = () => {
      conn.reconnectAttempts = 0
      const helloPayload: HelloPayload = { nodeId: this.localNodeId, role: this.localRole }
      if (this.authToken) {
        helloPayload.authToken = this.authToken
      }
      const hello: ReplicationMessage = {
        type: 'hello',
        payload: helloPayload,
      }
      ws.send(serializeMessage(hello))
    }

    ws.onmessage = (event: MessageEvent) => {
      const raw = typeof event.data === 'string' ? event.data : ''
      if (!raw) return

      const msg = deserializeMessage(raw)
      if (!msg) return

      if (msg.type === 'hello' && isValidHello(msg.payload)) {
        const hello = msg.payload

        if (!this.validateAuthToken(hello.authToken)) {
          ws.close(4001, 'Authentication failed')
          return
        }

        conn.nodeId = hello.nodeId
        this.peerConnections.set(hello.nodeId, conn)

        const peerInfo: NodeInfo = {
          id: hello.nodeId,
          role: hello.role,
          joinedAt: Date.now(),
          lastSeenAt: Date.now(),
          lastAckedSeq: 0n,
        }
        this.connectedPeers.set(hello.nodeId, peerInfo)

        if (this.peerConnectedHandler) {
          this.peerConnectedHandler(peerInfo)
        }
        return
      }

      if (conn.nodeId) {
        this.handleMessage(msg, conn.nodeId)
      }
    }

    ws.onclose = () => {
      if (conn.nodeId && this.peerConnections.get(conn.nodeId) === conn) {
        this.peerConnections.delete(conn.nodeId)
        this.removePeer(conn.nodeId)
      }

      if (this.connected) {
        this.scheduleReconnect(conn)
      }
    }

    ws.onerror = () => {}
  }

  private scheduleReconnect(conn: PeerConnection): void {
    const delay = Math.min(
      this.options.reconnectInitialDelay * this.options.reconnectMultiplier ** conn.reconnectAttempts,
      this.options.reconnectMaxDelay,
    )
    conn.reconnectAttempts += 1

    conn.reconnectTimer = setTimeout(() => {
      if (this.connected) {
        this.connectToPeer(conn.endpoint)
      }
    }, delay)
    conn.reconnectTimer.unref()
  }

  private handleMessage(msg: ReplicationMessage, fromPeerId: string): void {
    const peer = this.connectedPeers.get(fromPeerId)
    if (peer) {
      peer.lastSeenAt = Date.now()
    }

    switch (msg.type) {
      case 'batch': {
        if (typeof msg.payload !== 'string') return
        try {
          const batch = deserializeBatch(msg.payload)
          if (this.batchHandler) {
            this.batchHandler(batch, fromPeerId).catch(() => {})
          }
        } catch {
          /* malformed batch, drop */
        }
        break
      }

      case 'ack': {
        if (!isValidAck(msg.payload)) return
        if (this.ackHandler) {
          this.ackHandler(msg.payload, fromPeerId)
        }
        break
      }

      case 'forward_request': {
        if (!isValidForwardRequest(msg.payload)) return
        const request = msg.payload
        if (this.forwardHandler) {
          this.forwardHandler(request, fromPeerId)
            .then(result => {
              const response: ReplicationMessage = {
                type: 'forward_response',
                payload: { requestId: request.requestId, result } satisfies ForwardResponsePayload,
              }
              try {
                this.sendToPeer(fromPeerId, serializeMessage(response))
              } catch {
                /* peer may have disconnected */
              }
            })
            .catch(err => {
              const response: ReplicationMessage = {
                type: 'forward_response',
                payload: {
                  requestId: request.requestId,
                  error: err instanceof Error ? err.message : String(err),
                } satisfies ForwardResponsePayload,
              }
              try {
                this.sendToPeer(fromPeerId, serializeMessage(response))
              } catch {
                /* peer may have disconnected */
              }
            })
        }
        break
      }

      case 'forward_response': {
        if (!isValidForwardResponse(msg.payload)) return
        const resp = msg.payload
        const pending = this.pendingForwards.get(resp.requestId)
        if (pending) {
          clearTimeout(pending.timer)
          this.pendingForwards.delete(resp.requestId)
          if (resp.error) {
            pending.reject(new TransportError(resp.error))
          } else if (resp.result) {
            pending.resolve(resp.result)
          } else {
            pending.reject(new TransportError('Forward response contained neither result nor error'))
          }
        }
        break
      }

      case 'sync_request': {
        if (!isValidSyncRequest(msg.payload)) return
        if (this.syncRequestHandler) {
          this.syncRequestHandler(msg.payload, fromPeerId).catch(() => {})
        }
        break
      }

      case 'sync_batch': {
        if (!isValidSyncBatch(msg.payload)) return
        if (this.syncBatchHandler) {
          this.syncBatchHandler(msg.payload, fromPeerId).catch(() => {})
        }
        break
      }

      case 'sync_complete': {
        if (typeof msg.payload !== 'string') return
        try {
          const complete = deserializeSyncComplete(msg.payload)
          if (this.syncCompleteHandler) {
            this.syncCompleteHandler(complete, fromPeerId).catch(() => {})
          }
        } catch {
          /* malformed sync complete, drop */
        }
        break
      }

      case 'sync_ack': {
        if (!isValidSyncAck(msg.payload)) return
        if (this.syncAckHandler) {
          this.syncAckHandler(msg.payload, fromPeerId)
        }
        break
      }
    }
  }

  private sendToPeer(peerId: string, data: string): void {
    const serverSocket = this.serverPeerSockets.get(peerId)
    if (serverSocket) {
      const result = serverSocket.send(data, false)
      if (result === 0) {
        throw new TransportError(`Failed to send to peer '${peerId}': backpressure`)
      }
      return
    }

    const clientConn = this.peerConnections.get(peerId)
    if (clientConn && clientConn.ws.readyState === WebSocket.OPEN) {
      clientConn.ws.send(data)
      return
    }

    throw new TransportError(`Peer '${peerId}' is not connected`)
  }

  private removePeer(peerId: string): void {
    this.connectedPeers.delete(peerId)
    if (this.peerDisconnectedHandler) {
      this.peerDisconnectedHandler(peerId)
    }
  }

  private validateAuthToken(peerToken: string | undefined): boolean {
    if (!this.authToken) return true
    if (!peerToken) return false
    const expected = Buffer.from(this.authToken)
    const received = Buffer.from(peerToken)
    if (expected.length !== received.length) return false
    return timingSafeEqual(expected, received)
  }

  private ensureConnected(): void {
    if (!this.connected) {
      throw new TransportError('Transport is not connected')
    }
  }
}
