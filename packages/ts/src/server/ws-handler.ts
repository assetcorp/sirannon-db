import type { Database } from '../core/database.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ServerExecutionTarget, Subscription, WSHandlerOptions } from '../core/types.js'
import { SQL_NOT_ACCEPTED_MESSAGE } from './http-common.js'
import type { OperationSource } from './operation-lookup.js'
import { createOperationSource } from './operation-lookup.js'
import type { WSLiveMessage } from './protocol.js'
import { handleAckMessage } from './ws-ack.js'
import { CdcContextRegistry, toChangeRetentionOptions } from './ws-cdc.js'
import type { WSConnection } from './ws-connection.js'
import { WS_CLOSE_GOING_AWAY, WS_CLOSE_MESSAGE_TOO_BIG, WS_CLOSE_OVERLOADED } from './ws-connection.js'
import type { DeviceChangeStream } from './ws-device-stream.js'
import { DEFAULT_MAX_UNACKNOWLEDGED_CHANGES } from './ws-device-stream.js'
import { WSFrameWriter } from './ws-frame-writer.js'
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
import { closeGoingAway, InFlightRequests, SHUTTING_DOWN_MESSAGE, SHUTTING_DOWN_REASON } from './ws-shutdown.js'
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
  requests: InFlightRequests
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
  private readonly acceptDeviceSync: boolean
  private readonly operations: OperationSource
  private readonly frames = new WSFrameWriter(conn => this.handleOverload(conn))
  private closed = false

  constructor(sirannon: Sirannon, options?: WSHandlerOptions<Identity>) {
    this.sirannon = sirannon
    this.acceptSql = options?.acceptSql === true
    this.acceptDeviceSync = options?.acceptDeviceSync === true
    this.operations = createOperationSource<Identity>(options?.operations, options?.sharedOperations)
    this.maxPayloadLength = options?.maxPayloadLength ?? DEFAULT_MAX_PAYLOAD_LENGTH
    this.maxUnacknowledgedChanges = options?.maxUnacknowledgedChanges ?? DEFAULT_MAX_UNACKNOWLEDGED_CHANGES
    this.socketResumeBytes = Math.ceil((options?.maxBackpressureBytes ?? DEFAULT_MAX_BACKPRESSURE_BYTES) / 2)
    this.resolveExecutionTarget = options?.resolveExecutionTarget
    this.cdc = new CdcContextRegistry(sirannon, toChangeRetentionOptions(options))
  }

  async handleOpen(conn: WSConnection, databaseId: string, identity?: unknown): Promise<void> {
    if (this.refuseWhileClosed(conn)) return

    let database: Database | undefined
    try {
      database = await this.sirannon.resolve(databaseId)
    } catch (err) {
      this.frames.sendSirannonError(conn, '', err)
      conn.close(1011, 'Database open failed')
      return
    }
    if (!database) {
      this.frames.sendError(conn, '', 'DATABASE_NOT_FOUND', `Database '${databaseId}' not found`)
      conn.close(1008, 'Database not found')
      return
    }

    if (database.closed) {
      this.frames.sendError(conn, '', 'DATABASE_CLOSED', `Database '${databaseId}' is closed`)
      conn.close(1008, 'Database closed')
      return
    }

    let executionTarget: ServerExecutionTarget | null
    try {
      executionTarget = await this.resolveTarget(databaseId)
    } catch (err) {
      this.frames.sendSirannonError(conn, '', err)
      conn.close(1011, 'Execution target resolution failed')
      return
    }
    if (!executionTarget) {
      this.frames.sendError(conn, '', 'DATABASE_NOT_FOUND', `Database '${databaseId}' not found`)
      conn.close(1008, 'Database not found')
      return
    }
    if (this.refuseWhileClosed(conn)) return

    this.connections.set(conn, {
      databaseId,
      database,
      executionTarget,
      identity,
      subscriptions: new Map(),
      deviceStreams: new Map(),
      requests: new InFlightRequests(),
      overloaded: false,
    })
  }

  private refuseWhileClosed(conn: WSConnection): boolean {
    if (!this.closed) return false
    this.frames.sendError(conn, '', 'HANDLER_CLOSED', SHUTTING_DOWN_MESSAGE)
    conn.close(WS_CLOSE_GOING_AWAY, SHUTTING_DOWN_REASON)
    return true
  }

  /**
   * Closes a connection after the server drops a frame on it at the backpressure limit,
   * so the client can reject its pending requests and reconnect. The `overloaded` flag
   * makes the handler close the socket once, however many frames the server drops in a burst.
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
      conn.close(WS_CLOSE_MESSAGE_TOO_BIG, 'PAYLOAD_TOO_LARGE')
      return
    }

    let msg: Record<string, unknown>
    try {
      msg = JSON.parse(data) as Record<string, unknown>
    } catch {
      this.frames.sendError(conn, '', 'INVALID_JSON', 'Failed to parse message as JSON')
      return
    }

    if (typeof msg !== 'object' || msg === null || Array.isArray(msg)) {
      this.frames.sendError(conn, '', 'INVALID_MESSAGE', 'Message must be a JSON object')
      return
    }

    if (typeof msg.type !== 'string') {
      this.frames.sendError(conn, '', 'INVALID_MESSAGE', 'Message must have a string "type" field')
      return
    }

    if (typeof msg.id !== 'string') {
      const echoId = typeof msg.id === 'number' || typeof msg.id === 'bigint' ? String(msg.id) : ''
      this.frames.sendError(conn, echoId, 'INVALID_MESSAGE', 'Message must have a string "id" field')
      return
    }

    const id = msg.id
    const name = typeof msg.name === 'string' ? msg.name : null

    if (name !== null && !NAMED_MESSAGE_TYPES.has(msg.type)) {
      this.frames.sendError(conn, id, 'INVALID_MESSAGE', `A "${msg.type}" message names no registered operation`)
      return
    }

    if (!this.acceptSql && SQL_MESSAGE_TYPES.has(msg.type) && name === null) {
      this.frames.sendError(conn, id, 'SQL_NOT_ACCEPTED', SQL_NOT_ACCEPTED_MESSAGE)
      return
    }

    if (this.closed) {
      this.frames.sendError(conn, id, 'HANDLER_CLOSED', SHUTTING_DOWN_MESSAGE)
      return
    }

    const request = this.dispatch(conn, state, msg, msg.type, id, name)
    if (request !== undefined) state.requests.track(request)
  }

  private dispatch(
    conn: WSConnection,
    state: ConnectionState,
    msg: Record<string, unknown>,
    type: string,
    id: string,
    name: string | null,
  ): Promise<void> | undefined {
    switch (type) {
      case 'query':
        if (name !== null) return handleNamedQueryMessage(this.namedContext(conn, state), msg, id, name)
        return handleQueryMessage(this.operationContext(conn, state), msg, id)
      case 'execute':
        if (name !== null) return handleNamedExecuteMessage(this.namedContext(conn, state), msg, id, name)
        return handleExecuteMessage(this.operationContext(conn, state), msg, id)
      case 'transaction':
        return handleTransactionMessage(this.operationContext(conn, state), msg, id)
      case 'batch':
        return handleBatchMessage(this.operationContext(conn, state), msg, id)
      case 'load':
        return handleLoadMessage(this.operationContext(conn, state), msg, id)
      case 'subscribe':
        if (name !== null) return handleLiveSubscribeMessage(this.liveDeps(), conn, state, msg, id, name)
        return handleSubscribeMessage(this.subscribeDeps(), conn, state, msg, id)
      case 'unsubscribe':
        this.handleUnsubscribe(conn, state, id)
        return undefined
      case 'ack':
        return handleAckMessage(this.subscribeDeps(), conn, state, msg, id)
      default:
        this.frames.sendError(conn, id, 'UNKNOWN_TYPE', `Unknown message type: '${type}'`)
        return undefined
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
    state.requests.release()
  }

  get connectionCount(): number {
    return this.connections.size
  }

  /**
   * Stops every subscription, refuses every new connection and request, and closes each open connection with close
   * code 1001 once its requests have replied and its outbound buffer has emptied.
   *
   * @param deadline - The signal that aborts when the close deadline passes, after which the handler closes every
   * remaining connection at once. Without it, the handler waits for as long as the requests take.
   * @returns A promise that resolves once the handler has closed every connection and its change feeds.
   */
  async close(deadline: AbortSignal = new AbortController().signal): Promise<void> {
    if (this.closed) return
    this.closed = true

    const closing: Promise<void>[] = []
    for (const [conn, state] of this.connections) {
      for (const sub of state.subscriptions.values()) {
        sub.unsubscribe()
      }
      state.subscriptions.clear()
      closing.push(closeGoingAway(conn, state.requests, deadline))
    }
    await Promise.all(closing)
    this.connections.clear()

    await this.cdc.closeAll()
  }

  private operationContext(conn: WSConnection, state: ConnectionState): WSOperationContext {
    return {
      target: state.executionTarget,
      sendResult: (id, data) => this.frames.send(conn, { type: 'result', id, data }),
      sendError: (id, code, message) => this.frames.sendError(conn, id, code, message),
      sendCaughtError: (id, err) => this.frames.sendSirannonError(conn, id, err),
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
      sendSubscribedRows: (conn, id, rows) => this.frames.send(conn, { type: 'subscribed', id, rows }),
      sendLive: (conn, message: WSLiveMessage) => this.frames.send(conn, message),
      sendError: (conn, id, code, message) => this.frames.sendError(conn, id, code, message),
      sendSirannonError: (conn, id, err) => this.frames.sendSirannonError(conn, id, err),
    }
  }

  private subscribeDeps(): WSSubscribeDeps {
    return {
      cdc: this.cdc,
      acceptSql: this.acceptSql,
      acceptDeviceSync: this.acceptDeviceSync,
      maxUnacknowledgedChanges: this.maxUnacknowledgedChanges,
      socketResumeBytes: this.socketResumeBytes,
      hasSubscribeHook: () => this.sirannon.hookRegistry.has('beforeSubscribe'),
      attachSubscription: (conn, id, subscription) => this.attachSubscription(conn, id, subscription),
      detachSubscription: (conn, id, subscription) => this.detachSubscription(conn, id, subscription),
      beforeSubscribe: ctx => this.sirannon.hookRegistry.invoke('beforeSubscribe', ctx),
      sendSubscribed: (conn, id, seq, epoch, resync, maxUnacknowledgedChanges) =>
        this.frames.send(conn, {
          type: 'subscribed',
          id,
          seq,
          epoch,
          ...(resync ? { resync: true } : {}),
          ...(maxUnacknowledgedChanges !== undefined ? { maxUnacknowledgedChanges } : {}),
        }),
      sendResult: (conn, id, data) => this.frames.send(conn, { type: 'result', id, data }),
      sendError: (conn, id, code, message) => this.frames.sendError(conn, id, code, message),
      sendSirannonError: (conn, id, err) => this.frames.sendSirannonError(conn, id, err),
      sendChange: (conn, id, event) => this.frames.sendChange(conn, id, event),
      sendText: (conn, data) => this.frames.sendText(conn, data),
      closeFaulted: conn => conn.close(1011, 'Device stream failed'),
      handleOverload: conn => this.handleOverload(conn),
    }
  }

  private handleUnsubscribe(conn: WSConnection, state: ConnectionState, id: string): void {
    const sub = state.subscriptions.get(id)
    if (!sub) {
      this.frames.sendError(conn, id, 'SUBSCRIPTION_NOT_FOUND', `Subscription '${id}' not found`)
      return
    }

    sub.unsubscribe()
    state.subscriptions.delete(id)
    this.frames.send(conn, { type: 'unsubscribed', id })
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
}

/**
 * Builds the handler that processes a server's WebSocket connections and messages.
 *
 * @internal
 */
export function createWSHandler<Identity = unknown>(
  sirannon: Sirannon,
  options?: WSHandlerOptions<Identity>,
): WSHandler<Identity> {
  return new WSHandler<Identity>(sirannon, options)
}
