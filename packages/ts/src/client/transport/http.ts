import { decodeTaggedValues, encodeTaggedValues } from '../../core/cdc/encoding.js'
import type { BulkLoadDurability, ChangeEvent, Params, ReadConcern, WriteConcern } from '../../core/types.js'
import type {
  BatchResponse,
  ErrorResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from '../../server/protocol.js'
import type { RemoteSubscription, Transport } from '../types.js'
import { RemoteError } from '../types.js'

/**
 * HTTP transport for sirannon-db. Sends requests via `fetch` to the
 * server's REST endpoints. Supports query, execute, transaction, batch,
 * and load operations. Real-time subscriptions are not available over
 * HTTP; use {@link WebSocketTransport} for CDC subscriptions.
 */
export class HttpTransport implements Transport {
  private readonly baseUrl: string
  private readonly headers: Record<string, string>
  private closed = false

  constructor(baseUrl: string, headers?: Record<string, string>) {
    this.baseUrl = baseUrl.replace(/\/$/, '')
    this.headers = {
      'content-type': 'application/json',
      ...headers,
    }
  }

  async query(sql: string, params?: Params, readConcern?: ReadConcern): Promise<QueryResponse> {
    const response = await this.post<QueryResponse>('/query', {
      sql,
      params: encodeTaggedValues(params),
      readConcern,
    })
    return { rows: decodeTaggedValues(response.rows ?? []) as Record<string, unknown>[] }
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    return this.post<ExecuteResponse>('/execute', { sql, params: encodeTaggedValues(params) })
  }

  async transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse> {
    return this.post<TransactionResponse>('/transaction', {
      statements: statements.map(stmt => ({ sql: stmt.sql, params: encodeTaggedValues(stmt.params) })),
    })
  }

  async batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse> {
    return this.post<BatchResponse>('/batch', {
      sql,
      paramsBatch: paramsBatch.map(entry => encodeTaggedValues(entry)),
      writeConcern,
    })
  }

  async load(
    sql: string,
    paramsBatch: Params[],
    durability?: BulkLoadDurability,
    checkpoint?: boolean,
  ): Promise<LoadResponse> {
    return this.post<LoadResponse>('/load', {
      sql,
      paramsBatch: paramsBatch.map(entry => encodeTaggedValues(entry)),
      durability,
      checkpoint,
    })
  }

  async subscribe(
    _table: string,
    _filter: Record<string, unknown> | undefined,
    _callback: (event: ChangeEvent) => void,
  ): Promise<RemoteSubscription> {
    throw new RemoteError(
      'TRANSPORT_ERROR',
      'Subscriptions require WebSocket transport. Create the client with { transport: "websocket" } to use real-time subscriptions.',
    )
  }

  close(): void {
    this.closed = true
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    if (this.closed) {
      throw new RemoteError('TRANSPORT_ERROR', 'Transport is closed')
    }

    const url = `${this.baseUrl}${path}`

    let response: Response
    try {
      response = await fetch(url, {
        method: 'POST',
        headers: this.headers,
        body: JSON.stringify(body),
      })
    } catch (err) {
      throw new RemoteError(
        'CONNECTION_ERROR',
        `Failed to connect to ${url}: ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    let data: unknown
    try {
      data = await response.json()
    } catch {
      throw new RemoteError('INVALID_RESPONSE', `Server returned non-JSON response (HTTP ${response.status})`)
    }

    if (!response.ok) {
      const errorData = data as ErrorResponse
      throw new RemoteError(
        errorData.error?.code ?? 'UNKNOWN_ERROR',
        errorData.error?.message ?? `HTTP ${response.status}`,
      )
    }

    return data as T
  }
}
