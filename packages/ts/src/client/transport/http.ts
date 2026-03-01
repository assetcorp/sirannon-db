import type { ChangeEvent, Params } from '../../core/types.js'
import type {
  ErrorResponse,
  ExecuteResponse,
  QueryResponse,
  TransactionResponse,
} from '../../server/protocol.js'
import { RemoteError } from '../types.js'
import type { RemoteSubscription, Transport } from '../types.js'

/**
 * HTTP transport for sirannon-db. Sends requests via `fetch` to the
 * server's REST endpoints. Supports query, execute, and transaction
 * operations. Real-time subscriptions are not available over HTTP;
 * use {@link WebSocketTransport} for CDC subscriptions.
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

  async query(sql: string, params?: Params): Promise<QueryResponse> {
    return this.post<QueryResponse>('/query', { sql, params })
  }

  async execute(sql: string, params?: Params): Promise<ExecuteResponse> {
    return this.post<ExecuteResponse>('/execute', { sql, params })
  }

  async transaction(
    statements: Array<{ sql: string; params?: Params }>,
  ): Promise<TransactionResponse> {
    return this.post<TransactionResponse>('/transaction', { statements })
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

    const response = await fetch(`${this.baseUrl}${path}`, {
      method: 'POST',
      headers: this.headers,
      body: JSON.stringify(body),
    })

    const data: unknown = await response.json()

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
