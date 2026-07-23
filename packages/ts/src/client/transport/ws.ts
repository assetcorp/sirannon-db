import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { BulkLoadDurability, ChangeEvent, Params, WriteConcern } from '../../core/types.js'
import type {
  AckResponse,
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
  WSClientMessage,
  WSServerMessage,
} from '../../server/protocol.js'
import type { RemoteSubscription, SubscribeOptions, Transport } from '../types.js'
import { RemoteError } from '../types.js'
import type { ClientWebSocket } from './ws-connect.js'
import { openWebSocket } from './ws-connect.js'
import type { ActiveSubscription } from './ws-subscription-state.js'
import { applySubscribedMessage, buildResubscribeMessage, deliverChangeMessage } from './ws-subscription-state.js'

const DEFAULT_REQUEST_TIMEOUT = 30_000

interface PendingRequest {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout> | undefined
}

/**
 * WebSocket transport for sirannon-db. Connects to
 * `ws(s)://host:port/db/{id}` and supports query, execute, transaction,
 * batch, load, and real-time CDC subscriptions over a single persistent
 * connection.
 *
 * Connections are established lazily on first use and will
 * auto-reconnect (with subscription restoration) when
 * `autoReconnect` is enabled.
 */
export class WebSocketTransport implements Transport {
  private ws: ClientWebSocket | null = null
  private readonly url: string
  private readonly autoReconnect: boolean
  private readonly reconnectInterval: number
  private readonly requestTimeout: number
  private readonly protocols: string | string[] | undefined

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
      protocols?: string | string[]
    },
  ) {
    this.url = url
    this.autoReconnect = options?.autoReconnect ?? true
    this.reconnectInterval = options?.reconnectInterval ?? 1000
    this.requestTimeout = options?.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT
    this.protocols = options?.protocols
  }

  async query(sql: string, params?: Params): Promise<QueryResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    const response = await this.request<QueryResponse>({
      type: 'query',
      id,
      sql,
      params: encodeTaggedValues(params) as Params | undefined,
    })
    return { rows: decodeTaggedValues(response.rows ?? []) as Record<string, unknown>[] }
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<ExecuteResponse>({
      type: 'execute',
      id,
      sql,
      params: encodeTaggedValues(params) as Params | undefined,
    })
  }

  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<TransactionResponse>({
      type: 'transaction',
      id,
      statements: statements.map(stmt => ({
        sql: stmt.sql,
        params: encodeTaggedValues(stmt.params) as Params | undefined,
      })),
    })
  }

  async batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<BatchResponse>({
      type: 'batch',
      id,
      sql,
      paramsBatch: paramsBatch.map(entry => encodeTaggedValues(entry) as Params),
      ...(writeConcern ? { writeConcern } : {}),
    })
  }

  async load(
    sql: string,
    paramsBatch: Params[],
    durability?: BulkLoadDurability,
    checkpoint?: boolean,
  ): Promise<LoadResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<LoadResponse>({
      type: 'load',
      id,
      sql,
      paramsBatch: paramsBatch.map(entry => encodeTaggedValues(entry) as Params),
      ...(durability ? { durability } : {}),
      ...(checkpoint !== undefined ? { checkpoint } : {}),
    })
  }

  async subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription> {
    await this.ensureConnected()
    const id = this.nextId()

    // Store the subscription before sending so the callback is
    // available if a change event arrives before the 'subscribed'
    // confirmation (unlikely but safe).
    this.activeSubscriptions.set(id, {
      table,
      filter,
      callback,
      onReset: options?.onReset,
      onSubscribed: options?.onSubscribed,
      deviceId: options?.deviceId,
      lastSeq: options?.sinceSeq,
      epoch: options?.epoch,
    })

    try {
      const msg: WSClientMessage = {
        type: 'subscribe',
        id,
        table,
        ...(filter ? { filter: encodeTaggedValues(filter) as Record<string, unknown> } : {}),
        ...(options?.sinceSeq !== undefined ? { sinceSeq: options.sinceSeq.toString() } : {}),
        ...(options?.epoch !== undefined ? { epoch: options.epoch } : {}),
        ...(options?.deviceId !== undefined ? { deviceId: options.deviceId } : {}),
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

  async ack(deviceId: string, seq: bigint): Promise<AckResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<AckResponse>({ type: 'ack', id, deviceId, seq: seq.toString() })
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

    if (!this.connectPromise) {
      this.connectPromise = this.connect().finally(() => {
        this.connectPromise = null
      })
    }

    return this.connectPromise
  }

  private connect(): Promise<void> {
    return openWebSocket(this.url, this.protocols, {
      onConnected: ws => {
        this.ws = ws
      },
      onDisconnected: () => {
        this.ws = null
        this.handleDisconnect()
      },
      onMessage: raw => this.handleMessage(raw),
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
        const sub = this.activeSubscriptions.get(msg.id)
        if (sub) {
          applySubscribedMessage(sub, msg)
        }
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
          deliverChangeMessage(sub, msg)
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
    this.rejectAllPending(new RemoteError('CONNECTION_ERROR', 'WebSocket disconnected'))

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
        await this.request<void>(buildResubscribeMessage(id, sub))
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

      const timer =
        this.requestTimeout > 0
          ? setTimeout(() => {
              this.pendingRequests.delete(id)
              reject(new RemoteError('TIMEOUT', `Request timed out after ${this.requestTimeout}ms`))
            }, this.requestTimeout)
          : undefined

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
