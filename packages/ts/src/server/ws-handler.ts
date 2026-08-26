import type { Database } from '../core/database.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ChangeEvent, ServerExecutionTarget, Subscription, WSHandlerOptions } from '../core/types.js'
import { SQL_NOT_ACCEPTED_MESSAGE } from './http-common.js'
import type { OperationSource } from './operation-lookup.js'
import { createOperationSource } from './operation-lookup.js'
import type { WSLiveMessage, WSServerMessage } from './protocol.js'
import { handleAckMessage } from './ws-ack.js'
import { CdcContextRegistry } from './ws-cdc.js'
import type { WSConnection, WSSendOutcome } from './ws-connection.js'
import { WS_CLOSE_OVERLOADED } from './ws-connection.js'
import { wireChangeEvent } from './ws-device-frames.js'
import type { DeviceChangeStream } from './ws-device-stream.js'
import { DEFAULT_MAX_UNACKNOWLEDGED_CHANGES } from './ws-device-stream.js'
import type { WSLiveDeps } from './ws-live.js'
import { handleLiveSubscribeMessage } from './ws-live.js'
import type { WSNamedContext } from './ws-named.js'
import { handleNamedExecuteMessage, handleNamedQueryMessage } from './ws-named.js'
import type { WSOperationContext } from './ws-operations.js'
import {
  handleBatchMessage,
  handleExecuteMessage,
  handleLoadMessage,
  handleQueryMessage,
  handleTransactionMessage,
} from './ws-operations.js'
import type { SubscriptionAttachment, WSSubscribeDeps } from './ws-subscribe.js'
import { handleSubscribeMessage } from './ws-subscribe.js'

export type { WSConnection, WSSendOutcome } from './ws-connection.js'

const DEFAULT_MAX_PAYLOAD_LENGTH = 1_048_576

const DEFAULT_MAX_BACKPRESSURE_BYTES = 16 * 1_048_576

const SQL_MESSAGE_TYPES = new Set(['query', 'execute', 'transaction', 'batch', 'load'])

const NAMED_MESSAGE_TYPES = new Set(['query', 'execute', 'subscribe'])

export interface ConnectionState {
  databaseId: string
  database: Database
  executionTarget: ServerExecutionTarget
  identity: unknown
  subscriptions: Map<string, Subscription>
  deviceStreams: Map<string, DeviceChangeStream>
  overloaded: boolean
}

/**
 * Serves the WebSocket protocol for one server: queries, writes, change subscriptions, and live queries.
 *
 * @internal
 */
export class WSHandler<Identity = unknown> {
  private readonly sirannon: Sirannon
  private readonly maxPayloadLength: number
  private readonly resolveExecutionTarget: WSHandlerOptions['resolveExecutionTarget']
  private readonly connections = new Map<WSConnection, ConnectionState>()
  private readonly cdc: CdcContextRegistry
  private readonly maxUnacknowledgedChanges: number
  private readonly socketResumeBytes: number
  private readonly acceptSql: boolean
  private readonly operations: OperationSource
  private closed = false

  constructor(sirannon: Sirannon, options?: WSHandlerOptions<Identity>) {
    this.sirannon = sirannon
    this.acceptSql = options?.acceptSql === true
    this.operations = createOperationSource<Identity>(options?.operations)
    this.maxPayloadLength = options?.maxPayloadLength ?? DEFAULT_MAX_PAYLOAD_LENGTH
    this.maxUnacknowledgedChanges = options?.maxUnacknowledgedChanges ?? DEFAULT_MAX_UNACKNOWLEDGED_CHANGES
    this.socketResumeBytes = Math.ceil((options?.maxBackpressureBytes ?? DEFAULT_MAX_BACKPRESSURE_BYTES) / 2)
    this.resolveExecutionTarget = options?.resolveExecutionTarget
    this.cdc = new CdcContextRegistry(sirannon, options?.cdcRetentionMs, options?.deviceCursorRetentionMs)
  }

  async handleOpen(conn: WSConnection, databaseId: string, identity?: unknown): Promise<void> {
    if (this.closed) {
      this.sendError(conn, '', 'HANDLER_CLOSED', 'WebSocket handler is shut down')
      conn.close(1013, 'Handler shutting down')
      return
    }

    const database = await this.sirannon.resolve(databaseId)
    if (!database) {
      this.sendError(conn, '', 'DATABASE_NOT_FOUND', `Database '${databaseId}' not found`)
      conn.close(1008, 'Database not found')
      return
    }

    if (database.closed) {
      this.sendError(conn, '', 'DATABASE_CLOSED', `Database '${databaseId}' is closed`)
      conn.close(1008, 'Database closed')
      return
    }

    let executionTarget: ServerExecutionTarget | null
    try {
      executionTarget = await this.resolveTarget(databaseId)
    } catch (err) {
      this.sendSirannonError(conn, '', err)
      conn.close(1011, 'Execution target resolution failed')
      return
    }
    if (!executionTarget) {
      this.sendError(conn, '', 'DATABASE_NOT_FOUND', `Database '${databaseId}' not found`)
      conn.close(1008, 'Database not found')
      return
    }

    this.connections.set(conn, {
      databaseId,
      database,
      executionTarget,
      identity,
      subscriptions: new Map(),
      deviceStreams: new Map(),
      overloaded: false,
    })
  }

  /**
   * Tears down a connection whose outbound buffer overflowed. Closing lets the
   * client reject in-flight requests and reconnect; guarding on `overloaded`
   * keeps a burst of dropped frames from repeatedly re-closing the socket.
   */
  handleOverload(conn: WSConnection): void {
    const state = this.connections.get(conn)
    if (!state || state.overloaded) return
    state.overloaded = true
    conn.close(WS_CLOSE_OVERLOADED, 'Connection overloaded: backpressure limit exceeded')
  }

  handleMessage(conn: WSConnection, data: string): void {
    const state = this.connections.get(conn)
    if (!state) return

    if (Buffer.byteLength(data) > this.maxPayloadLength) {
      this.sendError(conn, '', 'PAYLOAD_TOO_LARGE', 'Message exceeds maximum payload length')
      return
    }

    let msg: Record<string, unknown>
    try {
      msg = JSON.parse(data) as Record<string, unknown>
    } catch {
      this.sendError(conn, '', 'INVALID_JSON', 'Failed to parse message as JSON')
      return
    }

    if (typeof msg !== 'object' || msg === null || Array.isArray(msg)) {
      this.sendError(conn, '', 'INVALID_MESSAGE', 'Message must be a JSON object')
      return
    }

    if (typeof msg.type !== 'string') {
      this.sendError(conn, '', 'INVALID_MESSAGE', 'Message must have a string "type" field')
      return
    }

    if (typeof msg.id !== 'string') {
      const echoId = typeof msg.id === 'number' || typeof msg.id === 'bigint' ? String(msg.id) : ''
      this.sendError(conn, echoId, 'INVALID_MESSAGE', 'Message must have a string "id" field')
      return
    }

    const id = msg.id
    const name = typeof msg.name === 'string' ? msg.name : null

    if (name !== null && !NAMED_MESSAGE_TYPES.has(msg.type)) {
      this.sendError(conn, id, 'INVALID_MESSAGE', `A "${msg.type}" message names no registered operation`)
      return
    }

    if (!this.acceptSql && SQL_MESSAGE_TYPES.has(msg.type) && name === null) {
      this.sendError(conn, id, 'SQL_NOT_ACCEPTED', SQL_NOT_ACCEPTED_MESSAGE)
      return
    }

    switch (msg.type) {
      case 'query':
        if (name !== null) handleNamedQueryMessage(this.namedContext(conn, state), msg, id, name)
        else handleQueryMessage(this.operationContext(conn, state), msg, id)
        break
      case 'execute':
        if (name !== null) handleNamedExecuteMessage(this.namedContext(conn, state), msg, id, name)
        else handleExecuteMessage(this.operationContext(conn, state), msg, id)
        break
      case 'transaction':
        handleTransactionMessage(this.operationContext(conn, state), msg, id)
        break
      case 'batch':
        handleBatchMessage(this.operationContext(conn, state), msg, id)
        break
      case 'load':
        handleLoadMessage(this.operationContext(conn, state), msg, id)
        break
      case 'subscribe':
        if (name !== null) handleLiveSubscribeMessage(this.liveDeps(), conn, state, msg, id, name)
        else handleSubscribeMessage(this.subscribeDeps(), conn, state, msg, id)
        break
      case 'unsubscribe':
        this.handleUnsubscribe(conn, state, id)
        break
      case 'ack':
        handleAckMessage(this.subscribeDeps(), conn, state, msg, id)
        break
      default:
        this.sendError(conn, id, 'UNKNOWN_TYPE', `Unknown message type: '${msg.type}'`)
    }
  }

  handleSocketDrain(conn: WSConnection): void {
    const state = this.connections.get(conn)
    if (!state) return
    for (const stream of state.deviceStreams.values()) {
      stream.onSocketDrain()
    }
  }

  handleClose(conn: WSConnection): void {
    const state = this.connections.get(conn)
    if (!state) return

    for (const sub of state.subscriptions.values()) {
      sub.unsubscribe()
    }
    state.subscriptions.clear()

    this.cdc.maybeCleanup(state.databaseId)
    this.connections.delete(conn)
  }

  get connectionCount(): number {
    return this.connections.size
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true

    for (const [conn, state] of this.connections) {
      for (const sub of state.subscriptions.values()) {
        sub.unsubscribe()
      }
      state.subscriptions.clear()
      conn.close(1001, 'Handler shutting down')
    }
    this.connections.clear()

    await this.cdc.closeAll()
  }

  private operationContext(conn: WSConnection, state: ConnectionState): WSOperationContext {
    return {
      target: state.executionTarget,
      sendResult: (id, data) => this.send(conn, { type: 'result', id, data }),
      sendError: (id, code, message) => this.sendError(conn, id, code, message),
      sendCaughtError: (id, err) => this.sendSirannonError(conn, id, err),
    }
  }

  private namedContext(conn: WSConnection, state: ConnectionState): WSNamedContext {
    return {
      ...this.operationContext(conn, state),
      databaseId: state.databaseId,
      identity: state.identity,
      operations: this.operations,
    }
  }

  private liveDeps(): WSLiveDeps {
    return {
      operations: this.operations,
      sendSubscribedRows: (conn, id, rows) => this.send(conn, { type: 'subscribed', id, rows }),
      sendLive: (conn, message: WSLiveMessage) => this.send(conn, message),
      sendError: (conn, id, code, message) => this.sendError(conn, id, code, message),
      sendSirannonError: (conn, id, err) => this.sendSirannonError(conn, id, err),
    }
  }

  private subscribeDeps(): WSSubscribeDeps {
    return {
      cdc: this.cdc,
      maxUnacknowledgedChanges: this.maxUnacknowledgedChanges,
      socketResumeBytes: this.socketResumeBytes,
      hasSubscribeHook: () => this.sirannon.hookRegistry.has('beforeSubscribe'),
      attachSubscription: (conn, id, subscription) => this.attachSubscription(conn, id, subscription),
      detachSubscription: (conn, id, subscription) => this.detachSubscription(conn, id, subscription),
      beforeSubscribe: ctx => this.sirannon.hookRegistry.invoke('beforeSubscribe', ctx),
      sendSubscribed: (conn, id, seq, epoch, resync, maxUnacknowledgedChanges) =>
        this.send(conn, {
          type: 'subscribed',
          id,
          seq,
          epoch,
          ...(resync ? { resync: true } : {}),
          ...(maxUnacknowledgedChanges !== undefined ? { maxUnacknowledgedChanges } : {}),
        }),
      sendResult: (conn, id, data) => this.send(conn, { type: 'result', id, data }),
      sendError: (conn, id, code, message) => this.sendError(conn, id, code, message),
      sendSirannonError: (conn, id, err) => this.sendSirannonError(conn, id, err),
      sendChange: (conn, id, event) => this.sendChange(conn, id, event),
      sendText: (conn, data) => this.sendText(conn, data),
      closeFaulted: conn => conn.close(1011, 'Device stream failed'),
      handleOverload: conn => this.handleOverload(conn),
    }
  }

  private handleUnsubscribe(conn: WSConnection, state: ConnectionState, id: string): void {
    const sub = state.subscriptions.get(id)
    if (!sub) {
      this.sendError(conn, id, 'SUBSCRIPTION_NOT_FOUND', `Subscription '${id}' not found`)
      return
    }

    sub.unsubscribe()
    state.subscriptions.delete(id)
    this.send(conn, { type: 'unsubscribed', id })
    this.cdc.maybeCleanup(state.databaseId)
  }

  private async resolveTarget(databaseId: string): Promise<ServerExecutionTarget | null> {
    if (!this.resolveExecutionTarget) {
      return (await this.sirannon.resolve(databaseId)) ?? null
    }
    return (await this.resolveExecutionTarget(databaseId)) ?? null
  }

  private attachSubscription(conn: WSConnection, id: string, subscription: Subscription): SubscriptionAttachment {
    const state = this.connections.get(conn)
    if (!state) return 'disconnected'
    if (state.subscriptions.has(id)) return 'duplicate'
    state.subscriptions.set(id, subscription)
    return 'attached'
  }

  private detachSubscription(conn: WSConnection, id: string, subscription: Subscription): void {
    const state = this.connections.get(conn)
    if (state?.subscriptions.get(id) === subscription) state.subscriptions.delete(id)
    subscription.unsubscribe()
  }

  private send(conn: WSConnection, msg: WSServerMessage): WSSendOutcome {
    let data: string
    try {
      data = JSON.stringify(msg)
    } catch {
      this.handleOverload(conn)
      return 'dropped'
    }
    const outcome = conn.send(data)
    if (outcome === 'dropped') {
      this.handleOverload(conn)
    }
    return outcome
  }

  private sendError(conn: WSConnection, id: string, code: string, message: string): void {
    this.send(conn, { type: 'error', id, error: { code, message } })
  }

  private sendSirannonError(conn: WSConnection, id: string, err: unknown): void {
    const code = err instanceof SirannonError ? err.code : 'INTERNAL_ERROR'
    const message = err instanceof SirannonError ? err.message : 'An unexpected error occurred'
    this.sendError(conn, id, code, message)
  }

  private sendChange(conn: WSConnection, subscriptionId: string, event: ChangeEvent): WSSendOutcome {
    return this.send(conn, { type: 'change', id: subscriptionId, event: wireChangeEvent(event) })
  }

  private sendText(conn: WSConnection, data: string): WSSendOutcome {
    const outcome = conn.send(data)
    if (outcome === 'dropped') {
      this.handleOverload(conn)
    }
    return outcome
  }
}

/**
 * Builds the WebSocket handler a server routes its upgrades and messages through.
 *
 * @internal
 */
export function createWSHandler<Identity = unknown>(
  sirannon: Sirannon,
  options?: WSHandlerOptions<Identity>,
): WSHandler<Identity> {
  return new WSHandler<Identity>(sirannon, options)
}
