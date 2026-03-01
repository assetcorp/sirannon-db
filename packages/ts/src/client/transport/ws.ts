import type { ChangeEvent, Params } from '../../core/types.js'
import type {
  ExecuteResponse,
  QueryResponse,
  TransactionResponse,
  WSClientMessage,
  WSServerMessage,
} from '../../server/protocol.js'
import { RemoteError } from '../types.js'
import type { RemoteSubscription, Transport } from '../types.js'

const DEFAULT_REQUEST_TIMEOUT = 30_000

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout>
}

interface ActiveSubscription {
  table: string
  filter: Record<string, unknown> | undefined
  callback: (event: ChangeEvent) => void
}

/**
 * WebSocket transport for sirannon-db. Connects to
 * `ws(s)://host:port/db/{id}` and supports query, execute,
 * and real-time CDC subscriptions over a single persistent connection.
 *
 * Connections are established lazily on first use and will
 * auto-reconnect (with subscription restoration) when
 * `autoReconnect` is enabled.
 *
 * Transactions are not supported over WebSocket; use
 * {@link HttpTransport} for batch transactions.
 */
export class WebSocketTransport implements Transport {
  private ws: WebSocket | null = null
  private readonly url: string
  private readonly autoReconnect: boolean
  private readonly reconnectInterval: number
  private readonly requestTimeout: number

  private pendingRequests = new Map<string, PendingRequest>()
  private activeSubscriptions = new Map<string, ActiveSubscription>()
  private idCounter = 0
  private closed = false
  private connectPromise: Promise<void> | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null

  constructor(
    url: string,
    options?: {
      autoReconnect?: boolean
      reconnectInterval?: number
      requestTimeout?: number
    },
  ) {
    this.url = url
    this.autoReconnect = options?.autoReconnect ?? true
    this.reconnectInterval = options?.reconnectInterval ?? 1000
    this.requestTimeout = options?.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT
  }

  async query(sql: string, params?: Params): Promise<QueryResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<QueryResponse>({ type: 'query', id, sql, params })
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<ExecuteResponse>({ type: 'execute', id, sql, params })
  }

  async transaction(
    _statements: Array<{ sql: string; params?: Params }>,
  ): Promise<TransactionResponse> {
    throw new RemoteError(
      'TRANSPORT_ERROR',
      'Transactions are not supported over WebSocket. Use HTTP transport for batch transactions.',
    )
  }

  async subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
  ): Promise<RemoteSubscription> {
    await this.ensureConnected()
    const id = this.nextId()

    // Store the subscription before sending so the callback is
    // available if a change event arrives before the 'subscribed'
    // confirmation (unlikely but safe).
    this.activeSubscriptions.set(id, { table, filter, callback })

    try {
      const msg: WSClientMessage = {
        type: 'subscribe',
        id,
        table,
        ...(filter ? { filter } : {}),
      }
      await this.request<void>(msg)
    } catch (err) {
      this.activeSubscriptions.delete(id)
      throw err
    }

    return {
      unsubscribe: () => {
        this.activeSubscriptions.delete(id)
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify({ type: 'unsubscribe', id }))
        }
      },
    }
  }

  close(): void {
    this.closed = true
    this.cancelReconnect()
    this.rejectAllPending(new RemoteError('TRANSPORT_ERROR', 'Transport closed'))
    this.activeSubscriptions.clear()

    if (this.ws) {
      this.ws.close(1000, 'Client closed')
      this.ws = null
    }

    this.connectPromise = null
  }

  private nextId(): string {
    return `c_${++this.idCounter}_${Date.now()}`
  }

  private async ensureConnected(): Promise<void> {
    if (this.closed) {
      throw new RemoteError('TRANSPORT_ERROR', 'Transport is closed')
    }

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return
    }

    if (this.connectPromise) {
      return this.connectPromise
    }

    this.connectPromise = this.connect()
    try {
      await this.connectPromise
    } finally {
      this.connectPromise = null
    }
  }

  private connect(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let settled = false
      const ws = new WebSocket(this.url)

      const onOpen = () => {
        settled = true
        this.ws = ws
        resolve()
      }

      const onError = () => {
        if (!settled) {
          settled = true
          reject(
            new RemoteError(
              'CONNECTION_ERROR',
              `Failed to connect to ${this.url}`,
            ),
          )
        }
      }

      const onClose = (event: CloseEvent) => {
        ws.removeEventListener('open', onOpen)
        ws.removeEventListener('error', onError)

        if (!settled) {
          settled = true
          reject(
            new RemoteError(
              'CONNECTION_ERROR',
              `Connection closed during handshake: ${event.code} ${event.reason}`,
            ),
          )
          return
        }

        this.ws = null
        this.handleDisconnect()
      }

      const onMessage = (event: MessageEvent) => {
        this.handleMessage(String(event.data))
      }

      ws.addEventListener('open', onOpen)
      ws.addEventListener('error', onError)
      ws.addEventListener('close', onClose)
      ws.addEventListener('message', onMessage)
    })
  }

  private handleMessage(raw: string): void {
    let msg: WSServerMessage
    try {
      msg = JSON.parse(raw) as WSServerMessage
    } catch {
      return
    }

    switch (msg.type) {
      case 'result': {
        const pending = this.pendingRequests.get(msg.id)
        if (pending) {
          clearTimeout(pending.timer)
          this.pendingRequests.delete(msg.id)
          pending.resolve(msg.data)
        }
        break
      }

      case 'subscribed': {
        const pending = this.pendingRequests.get(msg.id)
        if (pending) {
          clearTimeout(pending.timer)
          this.pendingRequests.delete(msg.id)
          pending.resolve(undefined)
        }
        break
      }

      case 'error': {
        const pending = this.pendingRequests.get(msg.id)
        if (pending) {
          clearTimeout(pending.timer)
          this.pendingRequests.delete(msg.id)
          pending.reject(new RemoteError(msg.error.code, msg.error.message))
        }
        break
      }

      case 'change': {
        const sub = this.activeSubscriptions.get(msg.id)
        if (sub) {
          const event: ChangeEvent = {
            type: msg.event.type,
            table: msg.event.table,
            row: msg.event.row,
            oldRow: msg.event.oldRow,
            seq: BigInt(msg.event.seq),
            timestamp: msg.event.timestamp,
          }
          try {
            sub.callback(event)
          } catch {
            // Swallow subscriber callback errors to prevent one broken
            // subscriber from disrupting the message processing loop.
          }
        }
        break
      }

      case 'unsubscribed': {
        // Cleanup already handled in unsubscribe().
        break
      }
    }
  }

  private handleDisconnect(): void {
    this.rejectAllPending(
      new RemoteError('CONNECTION_ERROR', 'WebSocket disconnected'),
    )

    if (this.autoReconnect && !this.closed && this.activeSubscriptions.size > 0) {
      this.scheduleReconnect()
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null || this.closed) {
      return
    }

    this.reconnectTimer = setTimeout(async () => {
      this.reconnectTimer = null
      if (this.closed) return

      try {
        await this.ensureConnected()
        await this.resubscribeAll()
      } catch {
        // Connection attempt failed. Schedule another retry as long
        // as the transport is still open and has active subscriptions.
        if (!this.closed && this.activeSubscriptions.size > 0) {
          this.scheduleReconnect()
        }
      }
    }, this.reconnectInterval)
  }

  private async resubscribeAll(): Promise<void> {
    const entries = [...this.activeSubscriptions.entries()]
    for (const [id, sub] of entries) {
      if (this.closed) break
      try {
        const msg: WSClientMessage = {
          type: 'subscribe',
          id,
          table: sub.table,
          ...(sub.filter ? { filter: sub.filter } : {}),
        }
        await this.request<void>(msg)
      } catch {
        // If a particular subscription can't be restored, remove it
        // so we don't keep retrying a broken subscription.
        this.activeSubscriptions.delete(id)
      }
    }
  }

  private request<T>(msg: WSClientMessage): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const { id } = msg

      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(new RemoteError('CONNECTION_ERROR', 'WebSocket is not connected'))
        return
      }

      const timer = setTimeout(() => {
        this.pendingRequests.delete(id)
        reject(
          new RemoteError(
            'TIMEOUT',
            `Request timed out after ${this.requestTimeout}ms`,
          ),
        )
      }, this.requestTimeout)

      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
      })

      this.ws.send(JSON.stringify(msg))
    })
  }

  private rejectAllPending(error: Error): void {
    for (const [, pending] of this.pendingRequests) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.pendingRequests.clear()
  }

  private cancelReconnect(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }
}
