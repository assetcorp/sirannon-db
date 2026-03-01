import SqliteDatabase from 'better-sqlite3'
import { ChangeTracker } from '../core/cdc/change-tracker.js'
import { SubscriptionManager } from '../core/cdc/subscription.js'
import type { Database } from '../core/database.js'
import { SirannonError } from '../core/errors.js'
import type { Sirannon } from '../core/sirannon.js'
import type { ChangeEvent, Subscription, WSHandlerOptions } from '../core/types.js'
import type { WSServerMessage } from './protocol.js'
import { toExecuteResponse } from './protocol.js'

const DEFAULT_POLL_INTERVAL_MS = 50
const DEFAULT_MAX_PAYLOAD_LENGTH = 1_048_576

/** Transport-agnostic WebSocket connection. */
export interface WSConnection {
  send(data: string): void
  close(code?: number, reason?: string): void
}

/** Tracks subscriptions and database reference for a single WS connection. */
interface ConnectionState {
  databaseId: string
  database: Database
  subscriptions: Map<string, Subscription>
}

/** CDC resources shared across all WS connections to the same database. */
interface CDCContext {
  cdcDb: InstanceType<typeof SqliteDatabase>
  tracker: ChangeTracker
  manager: SubscriptionManager
  stopPolling: () => void
}

/**
 * Manages WebSocket connections, routes messages to databases, and integrates
 * with CDC for real-time change subscriptions.
 *
 * Designed to be transport-agnostic: any WebSocket implementation can drive
 * this handler by calling handleOpen/handleMessage/handleClose with a
 * WSConnection adapter.
 */
export class WSHandler {
  private readonly sirannon: Sirannon
  private readonly maxPayloadLength: number
  private readonly connections = new Map<WSConnection, ConnectionState>()
  private readonly cdcContexts = new Map<string, CDCContext>()
  private closed = false

  constructor(sirannon: Sirannon, options?: WSHandlerOptions) {
    this.sirannon = sirannon
    this.maxPayloadLength = options?.maxPayloadLength ?? DEFAULT_MAX_PAYLOAD_LENGTH
  }

  // --- Public API ---

  /**
   * Register a new WebSocket connection for the given database.
   *
   * Sends an error and closes the connection if the database does not exist,
   * is closed, or the handler itself has been shut down.
   */
  handleOpen(conn: WSConnection, databaseId: string): void {
    if (this.closed) {
      this.sendError(conn, '', 'HANDLER_CLOSED', 'WebSocket handler is shut down')
      conn.close(1013, 'Handler shutting down')
      return
    }

    const database = this.sirannon.get(databaseId)
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

  /**
   * Process an incoming WebSocket message.
   *
   * Validates the JSON payload, extracts the message type and correlation ID,
   * and routes to the appropriate handler.
   */
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

  /**
   * Clean up after a WebSocket connection closes.
   *
   * Unsubscribes all active CDC subscriptions for this connection and
   * releases the per-database CDC context if no subscribers remain.
   */
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

  /** Number of active WebSocket connections. */
  get connectionCount(): number {
    return this.connections.size
  }

  /**
   * Shut down the handler.
   *
   * Unsubscribes all CDC subscriptions, closes all WebSocket connections
   * with code 1001 (Going Away), and releases all CDC resources.
   */
  close(): void {
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
        ctx.cdcDb.close()
      } catch {
        /* best effort */
      }
    }
    this.cdcContexts.clear()
  }

  // --- Private message handlers ---

  private handleQuery(conn: WSConnection, state: ConnectionState, msg: Record<string, unknown>, id: string): void {
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
      const rows = state.database.query(msg.sql, params)
      this.send(conn, { type: 'result', id, data: { rows } })
    } catch (err) {
      this.sendSirannonError(conn, id, err)
    }
  }

  private handleExecute(conn: WSConnection, state: ConnectionState, msg: Record<string, unknown>, id: string): void {
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
      const result = state.database.execute(msg.sql, params)
      this.send(conn, {
        type: 'result',
        id,
        data: toExecuteResponse(result),
      })
    } catch (err) {
      this.sendSirannonError(conn, id, err)
    }
  }

  private handleSubscribe(conn: WSConnection, state: ConnectionState, msg: Record<string, unknown>, id: string): void {
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
      const ctx = this.ensureCDC(state.databaseId, state.database)
      ctx.tracker.watch(ctx.cdcDb, msg.table)

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

  // --- CDC management ---

  /**
   * Get or create a CDC context for the given database.
   *
   * Opens a separate better-sqlite3 connection for trigger installation and
   * change polling. WAL mode and a 5-second busy timeout are set so that
   * the CDC connection coexists with the Database class's connection pool.
   */
  private ensureCDC(databaseId: string, database: Database): CDCContext {
    const existing = this.cdcContexts.get(databaseId)
    if (existing) return existing

    const cdcDb = new SqliteDatabase(database.path)
    cdcDb.pragma('journal_mode = WAL')
    cdcDb.pragma('busy_timeout = 5000')

    const tracker = new ChangeTracker()
    const manager = new SubscriptionManager()

    let running = true
    let consecutiveErrors = 0
    const MAX_CONSECUTIVE_ERRORS = 10

    const stopPolling = () => {
      running = false
      clearInterval(interval)
    }

    const tick = () => {
      if (!running || manager.size === 0) return
      try {
        const events = tracker.poll(cdcDb)
        if (events.length > 0) {
          manager.dispatch(events)
        }
        consecutiveErrors = 0
      } catch {
        consecutiveErrors++
        if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
          stopPolling()
        }
      }
    }
    const interval = setInterval(tick, DEFAULT_POLL_INTERVAL_MS)
    if (typeof interval.unref === 'function') interval.unref()

    const ctx: CDCContext = { cdcDb, tracker, manager, stopPolling }
    this.cdcContexts.set(databaseId, ctx)
    return ctx
  }

  /**
   * Release CDC resources for a database if no subscribers remain.
   *
   * Stops the polling loop and closes the dedicated CDC connection.
   */
  private maybeCleanupCDC(databaseId: string): void {
    const ctx = this.cdcContexts.get(databaseId)
    if (!ctx || ctx.manager.size > 0) return

    ctx.stopPolling()
    try {
      ctx.cdcDb.close()
    } catch {
      /* best effort */
    }
    this.cdcContexts.delete(databaseId)
  }

  // --- Validation ---

  private isValidParams(params: unknown): boolean {
    if (params === undefined || params === null) return true
    return typeof params === 'object'
  }

  // --- Send helpers ---

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

/** Create a transport-agnostic WebSocket handler bound to a Sirannon registry. */
export function createWSHandler(sirannon: Sirannon, options?: WSHandlerOptions): WSHandler {
  return new WSHandler(sirannon, options)
}
