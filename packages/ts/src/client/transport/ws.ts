import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { BulkLoadDurability, ChangeEvent, Params, ReadConcern, WriteConcern } from '../../core/types.js'
import { withSirannonSubprotocol } from '../../core/ws-handshake.js'
import type {
  AckResponse,
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
  WSClientMessage,
} from '../../server/protocol.js'
import type { LiveHandlers, RegistryDigestSource, RemoteSubscription, SubscribeOptions, Transport } from '../types.js'
import { RemoteError } from '../types.js'
import type { ClientWebSocket } from './ws-connect.js'
import { openWebSocket } from './ws-connect.js'
import { routeServerMessage } from './ws-inbound.js'
import { LiveQueryRegistry } from './ws-live-state.js'
import { PendingRequests } from './ws-pending.js'
import type { ActiveSubscription } from './ws-subscription-state.js'
import { buildResubscribeMessage } from './ws-subscription-state.js'

const DEFAULT_REQUEST_TIMEOUT = 30_000

/**
 * WebSocket transport for sirannon-db. Connects to
 * `ws(s)://host:port/db/{id}` and supports query, execute, transaction,
 * batch, load, and real-time CDC subscriptions over a single persistent
 * connection.
 *
 * Connections are established lazily on first use and will
 * auto-reconnect (with subscription restoration) when
 * `autoReconnect` is enabled.
 *
 * @public
 */
export class WebSocketTransport implements Transport {
  /** @internal */
  readonly carriesReadConcern = true
  private ws: ClientWebSocket | null = null
  private readonly url: string
  private readonly autoReconnect: boolean
  private readonly reconnectInterval: number
  private readonly requestTimeout: number
  private readonly protocols: string[] | undefined
  private readonly headers: Record<string, string> | undefined
  private refusal: RemoteError | null = null

  private readonly pending: PendingRequests
  private activeSubscriptions = new Map<string, ActiveSubscription>()
  private readonly liveQueries = new LiveQueryRegistry({
    request: message => this.request<void>(message),
    sendUnsubscribe: id => this.sendUnsubscribe(id),
    isClosed: () => this.closed,
  })
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
      headers?: Record<string, string>
    },
  ) {
    this.url = url
    this.autoReconnect = options?.autoReconnect ?? true
    this.reconnectInterval = options?.reconnectInterval ?? 1000
    this.requestTimeout = options?.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT
    this.protocols = withSirannonSubprotocol(options?.protocols)
    this.headers = options?.headers
    this.pending = new PendingRequests(this.requestTimeout)
  }

  /** Sends a read and returns its rows. */
  async query(sql: string, params?: Params, readConcern?: ReadConcern): Promise<QueryResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    const response = await this.request<QueryResponse>({
      type: 'query',
      id,
      sql,
      params: encodeTaggedValues(params) as Params | undefined,
      ...(readConcern ? { readConcern } : {}),
    })
    return { rows: decodeTaggedValues(response.rows ?? []) as Record<string, unknown>[] }
  }

  /** Sends one write. */
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

  /** Sends several statements the server runs in one transaction. */
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

  /** Sends one statement over many parameter sets, which the server runs in one transaction. */
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

  /** Sends a bulk load, which the server runs at relaxed durability. */
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

  /** Runs a registered read by name and returns its rows. */
  async queryNamed(name: string, args?: Record<string, unknown>, readConcern?: ReadConcern): Promise<QueryResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    const response = await this.request<QueryResponse>({
      type: 'query',
      id,
      name,
      ...(args === undefined ? {} : { args: encodeTaggedValues(args) as Record<string, unknown> }),
      ...(readConcern ? { readConcern } : {}),
    })
    return { rows: decodeTaggedValues(response.rows ?? []) as Record<string, unknown>[] }
  }

  /** Runs a registered write by name. */
  async executeNamed(
    name: string,
    args?: Record<string, unknown>,
    writeConcern?: WriteConcern,
  ): Promise<TransactionResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<TransactionResponse>({
      type: 'execute',
      id,
      name,
      ...(args === undefined ? {} : { args: encodeTaggedValues(args) as Record<string, unknown> }),
      ...(writeConcern ? { writeConcern } : {}),
    })
  }

  /** Opens a live query on a registered read and delivers its updates to the handlers. */
  async liveSubscribe(
    name: string,
    args: Record<string, unknown> | undefined,
    handlers: LiveHandlers,
    registryDigest?: RegistryDigestSource,
  ): Promise<RemoteSubscription> {
    await this.ensureConnected()
    return this.liveQueries.open(this.nextId(), name, args, handlers, registryDigest)
  }

  /** Opens a change subscription on a watched table. */
  async subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
  ): Promise<RemoteSubscription> {
    await this.ensureConnected()
    const id = this.nextId()

    this.activeSubscriptions.set(id, {
      table,
      filter,
      callback,
      onReset: options?.onReset,
      onSubscribed: options?.onSubscribed,
      deviceId: options?.deviceId,
      tables: options?.tables,
      schemaVersion: options?.schemaVersion,
      lastSeq: options?.sinceSeq,
      resumeSeq: options?.getResumeSeq,
      epoch: options?.epoch,
      stagedStream: options?.stagedStream,
    })

    try {
      const msg: WSClientMessage = {
        type: 'subscribe',
        id,
        table,
        ...(filter ? { filter: encodeTaggedValues(filter) as Record<string, unknown> } : {}),
        ...(options?.tables !== undefined ? { tables: [...options.tables] } : {}),
        ...(options?.sinceSeq !== undefined ? { sinceSeq: options.sinceSeq.toString() } : {}),
        ...(options?.epoch !== undefined ? { epoch: options.epoch } : {}),
        ...(options?.deviceId !== undefined ? { deviceId: options.deviceId } : {}),
        ...(options?.schemaVersion !== undefined ? { schemaVersion: options.schemaVersion } : {}),
        ...(options?.stagedStream === true ? { stagedStream: true } : {}),
      }
      await this.request<void>(msg)
    } catch (err) {
      this.activeSubscriptions.delete(id)
      throw err
    }

    return {
      unsubscribe: () => {
        this.activeSubscriptions.delete(id)
        this.sendUnsubscribe(id)
      },
    }
  }

  private sendUnsubscribe(id: string): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: 'unsubscribe', id }))
    }
  }

  /** @internal */
  async ack(deviceId: string, seq: bigint): Promise<AckResponse> {
    await this.ensureConnected()
    const id = this.nextId()
    return this.request<AckResponse>({ type: 'ack', id, deviceId, seq: seq.toString() })
  }

  /** Closes the transport and every subscription running on it. */
  close(): void {
    this.closed = true
    this.cancelReconnect()
    this.pending.rejectAll(new RemoteError('TRANSPORT_ERROR', 'Transport closed'))
    this.activeSubscriptions.clear()
    this.liveQueries.clear()

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

    if (this.refusal) {
      throw this.refusal
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
    return openWebSocket(
      this.url,
      { protocols: this.protocols, headers: this.headers },
      {
        onConnected: ws => {
          this.ws = ws
        },
        onRefused: error => {
          this.refusal = error
        },
        onDisconnected: () => {
          this.ws = null
          this.handleDisconnect()
        },
        onMessage: raw =>
          routeServerMessage(raw, {
            pending: this.pending,
            subscriptions: this.activeSubscriptions,
            live: this.liveQueries,
          }),
      },
    )
  }

  private handleDisconnect(): void {
    this.pending.rejectAll(this.refusal ?? new RemoteError('CONNECTION_ERROR', 'WebSocket disconnected'))

    this.liveQueries.markDisconnected()

    const restorable = this.activeSubscriptions.size + this.liveQueries.size
    if (this.autoReconnect && !this.closed && this.refusal === null && restorable > 0) {
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
        if (!this.closed && this.refusal === null && this.activeSubscriptions.size + this.liveQueries.size > 0) {
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
        this.activeSubscriptions.delete(id)
      }
    }

    await this.liveQueries.restart()
  }

  private request<T>(msg: WSClientMessage): Promise<T> {
    if (this.refusal) {
      return Promise.reject(this.refusal)
    }
    const socket = this.ws
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return Promise.reject(new RemoteError('CONNECTION_ERROR', 'WebSocket is not connected'))
    }
    return this.pending.start<T>(msg.id, () => socket.send(JSON.stringify(msg)))
  }

  private cancelReconnect(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }
}
