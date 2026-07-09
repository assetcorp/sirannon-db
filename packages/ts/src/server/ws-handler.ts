import type { Database } from '../core/database.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ChangeEvent, ServerExecutionTarget, Subscription, WSHandlerOptions } from '../core/types.js'
import type { WSServerMessage } from './protocol.js'
import { CdcContextRegistry } from './ws-cdc.js'
import type { WSOperationContext } from './ws-operations.js'
import {
  handleBatchMessage,
  handleExecuteMessage,
  handleLoadMessage,
  handleQueryMessage,
  handleTransactionMessage,
} from './ws-operations.js'

const DEFAULT_MAX_PAYLOAD_LENGTH = 1_048_576

export interface WSConnection {
  send(data: string): void
  close(code?: number, reason?: string): void
}

interface ConnectionState {
  databaseId: string
  database: Database
  executionTarget: ServerExecutionTarget
  subscriptions: Map<string, Subscription>
}

export class WSHandler {
  private readonly sirannon: Sirannon
  private readonly maxPayloadLength: number
  private readonly resolveExecutionTarget: WSHandlerOptions['resolveExecutionTarget']
  private readonly connections = new Map<WSConnection, ConnectionState>()
  private readonly cdc: CdcContextRegistry
  private closed = false

  constructor(sirannon: Sirannon, options?: WSHandlerOptions) {
    this.sirannon = sirannon
    this.maxPayloadLength = options?.maxPayloadLength ?? DEFAULT_MAX_PAYLOAD_LENGTH
    this.resolveExecutionTarget = options?.resolveExecutionTarget
    this.cdc = new CdcContextRegistry(sirannon)
  }

  async handleOpen(conn: WSConnection, databaseId: string): Promise<void> {
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
      subscriptions: new Map(),
    })
  }

  handleMessage(conn: WSConnection, data: string): void {
    const state = this.connections.get(conn)
    if (!state) return

    if (data.length > this.maxPayloadLength) {
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
      this.sendError(conn, '', 'INVALID_MESSAGE', 'Message must have a string "id" field')
      return
    }

    const id = msg.id

    switch (msg.type) {
      case 'query':
        handleQueryMessage(this.operationContext(conn, state), msg, id)
        break
      case 'execute':
        handleExecuteMessage(this.operationContext(conn, state), msg, id)
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
        this.handleSubscribe(conn, state, msg, id)
        break
      case 'unsubscribe':
        this.handleUnsubscribe(conn, state, id)
        break
      default:
        this.sendError(conn, id, 'UNKNOWN_TYPE', `Unknown message type: '${msg.type}'`)
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

  private async handleSubscribe(
    conn: WSConnection,
    state: ConnectionState,
    msg: Record<string, unknown>,
    id: string,
  ): Promise<void> {
    if (typeof msg.table !== 'string') {
      this.sendError(conn, id, 'INVALID_MESSAGE', 'Subscribe message requires a "table" string field')
      return
    }

    if (state.subscriptions.has(id)) {
      this.sendError(conn, id, 'DUPLICATE_SUBSCRIPTION', `Subscription '${id}' already exists on this connection`)
      return
    }

    if (state.database.readOnly) {
      this.sendError(conn, id, 'READ_ONLY', 'Subscriptions are not available on read-only databases')
      return
    }

    if (state.database.path === ':memory:') {
      this.sendError(conn, id, 'CDC_UNSUPPORTED', 'CDC subscriptions require file-based databases')
      return
    }

    if (
      msg.filter !== undefined &&
      msg.filter !== null &&
      (typeof msg.filter !== 'object' || Array.isArray(msg.filter))
    ) {
      this.sendError(conn, id, 'INVALID_MESSAGE', '"filter" must be a plain object')
      return
    }

    const filter = (msg.filter ?? undefined) as Record<string, unknown> | undefined

    try {
      const ctx = await this.cdc.ensure(state.databaseId, state.database)
      await ctx.tracker.watch(ctx.cdcConn, msg.table)

      const sub = ctx.manager.subscribe(msg.table, filter, (event: ChangeEvent) => {
        this.sendChange(conn, id, event)
      })

      state.subscriptions.set(id, sub)
      this.send(conn, { type: 'subscribed', id })
    } catch (err) {
      this.cdc.maybeCleanup(state.databaseId)
      this.sendSirannonError(conn, id, err)
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

  private send(conn: WSConnection, msg: WSServerMessage): void {
    try {
      conn.send(JSON.stringify(msg))
    } catch {
      // Connection might be closing; ignore send failures
    }
  }

  private sendError(conn: WSConnection, id: string, code: string, message: string): void {
    this.send(conn, { type: 'error', id, error: { code, message } })
  }

  private sendSirannonError(conn: WSConnection, id: string, err: unknown): void {
    const code = err instanceof SirannonError ? err.code : 'INTERNAL_ERROR'
    const message = err instanceof SirannonError ? err.message : 'An unexpected error occurred'
    this.sendError(conn, id, code, message)
  }

  private sendChange(conn: WSConnection, subscriptionId: string, event: ChangeEvent): void {
    this.send(conn, {
      type: 'change',
      id: subscriptionId,
      event: {
        type: event.type,
        table: event.table,
        row: event.row as Record<string, unknown>,
        oldRow: event.oldRow as Record<string, unknown> | undefined,
        seq: event.seq.toString(),
        timestamp: event.timestamp,
      },
    })
  }
}

export function createWSHandler(sirannon: Sirannon, options?: WSHandlerOptions): WSHandler {
  return new WSHandler(sirannon, options)
}
