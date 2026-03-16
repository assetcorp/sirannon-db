import { ChangeTracker } from '../core/cdc/change-tracker.js'
import { SubscriptionManager } from '../core/cdc/subscription.js'
import type { Database } from '../core/database.js'
import type { SQLiteConnection } from '../core/driver/types.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ChangeEvent, Subscription, WSHandlerOptions } from '../core/types.js'
import type { WSServerMessage } from './protocol.js'
import { toExecuteResponse } from './protocol.js'

const DEFAULT_POLL_INTERVAL_MS = 50
const DEFAULT_MAX_PAYLOAD_LENGTH = 1_048_576

export interface WSConnection {
  send(data: string): void
  close(code?: number, reason?: string): void
}

interface ConnectionState {
  databaseId: string
  database: Database
  subscriptions: Map<string, Subscription>
}

interface CDCContext {
  cdcConn: SQLiteConnection
  tracker: ChangeTracker
  manager: SubscriptionManager
  stopPolling: () => void
}

export class WSHandler {
  private readonly sirannon: Sirannon
  private readonly maxPayloadLength: number
  private readonly connections = new Map<WSConnection, ConnectionState>()
  private readonly cdcContexts = new Map<string, CDCContext>()
  private readonly cdcPending = new Map<string, Promise<CDCContext>>()
  private closed = false

  constructor(sirannon: Sirannon, options?: WSHandlerOptions) {
    this.sirannon = sirannon
    this.maxPayloadLength = options?.maxPayloadLength ?? DEFAULT_MAX_PAYLOAD_LENGTH
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

    this.connections.set(conn, {
      databaseId,
      database,
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
        this.handleQuery(conn, state, msg, id)
        break
      case 'execute':
        this.handleExecute(conn, state, msg, id)
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

    this.maybeCleanupCDC(state.databaseId)
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

    for (const ctx of this.cdcContexts.values()) {
      ctx.stopPolling()
      try {
        await ctx.cdcConn.close()
      } catch {
        /* best effort */
      }
    }
    this.cdcContexts.clear()
  }

  private async handleQuery(
    conn: WSConnection,
    state: ConnectionState,
    msg: Record<string, unknown>,
    id: string,
  ): Promise<void> {
    if (typeof msg.sql !== 'string') {
      this.sendError(conn, id, 'INVALID_MESSAGE', 'Query message requires a "sql" string field')
      return
    }

    if (!this.isValidParams(msg.params)) {
      this.sendError(conn, id, 'INVALID_MESSAGE', '"params" must be an object or array')
      return
    }

    try {
      const params = (msg.params ?? undefined) as Record<string, unknown> | unknown[] | undefined
      const rows = await state.database.query(msg.sql, params)
      this.send(conn, { type: 'result', id, data: { rows } })
    } catch (err) {
      this.sendSirannonError(conn, id, err)
    }
  }

  private async handleExecute(
    conn: WSConnection,
    state: ConnectionState,
    msg: Record<string, unknown>,
    id: string,
  ): Promise<void> {
    if (typeof msg.sql !== 'string') {
      this.sendError(conn, id, 'INVALID_MESSAGE', 'Execute message requires a "sql" string field')
      return
    }

    if (!this.isValidParams(msg.params)) {
      this.sendError(conn, id, 'INVALID_MESSAGE', '"params" must be an object or array')
      return
    }

    try {
      const params = (msg.params ?? undefined) as Record<string, unknown> | unknown[] | undefined
      const result = await state.database.execute(msg.sql, params)
      this.send(conn, {
        type: 'result',
        id,
        data: toExecuteResponse(result),
      })
    } catch (err) {
      this.sendSirannonError(conn, id, err)
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
      const ctx = await this.ensureCDC(state.databaseId, state.database)
      await ctx.tracker.watch(ctx.cdcConn, msg.table)

      const sub = ctx.manager.subscribe(msg.table, filter, (event: ChangeEvent) => {
        this.sendChange(conn, id, event)
      })

      state.subscriptions.set(id, sub)
      this.send(conn, { type: 'subscribed', id })
    } catch (err) {
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
    this.maybeCleanupCDC(state.databaseId)
  }

  private async ensureCDC(databaseId: string, database: Database): Promise<CDCContext> {
    const existing = this.cdcContexts.get(databaseId)
    if (existing) return existing

    const pending = this.cdcPending.get(databaseId)
    if (pending) return pending

    const promise = this.createCDCContext(database)
    this.cdcPending.set(databaseId, promise)
    try {
      const ctx = await promise
      this.cdcContexts.set(databaseId, ctx)
      return ctx
    } finally {
      this.cdcPending.delete(databaseId)
    }
  }

  private async createCDCContext(database: Database): Promise<CDCContext> {
    const cdcConn = await this.sirannon.driver.open(database.path, { walMode: true })

    const tracker = new ChangeTracker()
    const manager = new SubscriptionManager()

    let polling = false
    let consecutiveErrors = 0
    const MAX_CONSECUTIVE_ERRORS = 10

    const stopPolling = () => {
      clearInterval(interval)
    }

    const tick = async () => {
      if (manager.size === 0) return
      if (polling) return
      polling = true
      try {
        const events = await tracker.poll(cdcConn)
        if (events.length > 0) {
          manager.dispatch(events)
        }
        consecutiveErrors = 0
      } catch {
        consecutiveErrors++
        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          stopPolling()
        }
      } finally {
        polling = false
      }
    }
    const interval = setInterval(tick, DEFAULT_POLL_INTERVAL_MS)
    if (typeof interval === 'object' && 'unref' in interval && typeof interval.unref === 'function') {
      interval.unref()
    }

    return { cdcConn, tracker, manager, stopPolling }
  }

  private maybeCleanupCDC(databaseId: string): void {
    const ctx = this.cdcContexts.get(databaseId)
    if (!ctx || ctx.manager.size > 0) return

    ctx.stopPolling()
    ctx.cdcConn.close().catch(() => {
      /* best effort */
    })
    this.cdcContexts.delete(databaseId)
  }

  private isValidParams(params: unknown): boolean {
    if (params === undefined || params === null) return true
    return typeof params === 'object'
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
