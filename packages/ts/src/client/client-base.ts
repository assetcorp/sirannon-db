import type { ClientOptions } from '../core/types.js'
import { RemoteDatabase } from './database-proxy.js'
import { toBaseUrl, toWsUrl } from './endpoint-urls.js'
import { DEFAULT_HTTP_REQUEST_TIMEOUT_MS } from './http-json.js'
import { ServerCapabilities } from './server-capabilities.js'
import { HttpTransport } from './transport/http.js'
import { WebSocketTransport } from './transport/ws.js'
import type { Transport } from './types.js'

export interface TransportSettings {
  transport: 'websocket' | 'http'
  headers: Record<string, string> | undefined
  webSocketProtocols: string | string[] | undefined
  autoReconnect: boolean
  reconnectInterval: number
  requestTimeout: number | undefined
}

export function resolveTransportSettings(options?: ClientOptions): TransportSettings {
  return {
    transport: options?.transport ?? 'websocket',
    headers: options?.headers,
    webSocketProtocols: options?.webSocketProtocols,
    autoReconnect: options?.autoReconnect ?? true,
    reconnectInterval: options?.reconnectInterval ?? 1000,
    requestTimeout: options?.requestTimeout,
  }
}

export function createEndpointTransport(settings: TransportSettings, baseUrl: string, databaseId: string): Transport {
  const base = toBaseUrl(baseUrl)
  const encodedId = encodeURIComponent(databaseId)

  if (settings.transport === 'http') {
    return new HttpTransport(`${base}/db/${encodedId}`, settings.headers)
  }

  return new WebSocketTransport(`${toWsUrl(base)}/db/${encodedId}`, {
    autoReconnect: settings.autoReconnect,
    reconnectInterval: settings.reconnectInterval,
    protocols: settings.webSocketProtocols,
    requestTimeout: settings.requestTimeout,
  })
}

export abstract class DatabaseClient {
  protected readonly settings: TransportSettings
  private readonly databases = new Map<string, RemoteDatabase>()
  private closed = false

  constructor(options?: ClientOptions) {
    this.settings = resolveTransportSettings(options)
  }

  database(id: string): RemoteDatabase {
    if (this.closed) {
      throw new Error('Client is closed')
    }

    const existing = this.databases.get(id)
    if (existing) {
      return existing
    }

    const db = new RemoteDatabase(id, this.createTransport(id), this.createCapabilities(id), () => {
      this.databases.delete(id)
    })
    this.databases.set(id, db)
    return db
  }

  protected createCapabilities(databaseId: string): ServerCapabilities {
    const timeout = this.settings.requestTimeout
    return new ServerCapabilities(
      () => this.resolveServerUrl(databaseId),
      this.settings.headers,
      timeout !== undefined && timeout > 0 ? timeout : DEFAULT_HTTP_REQUEST_TIMEOUT_MS,
    )
  }

  close(): void {
    this.closed = true
    const openDatabases = [...this.databases.values()]
    this.databases.clear()
    for (const db of openDatabases) {
      db.close()
    }
  }

  protected abstract createTransport(databaseId: string): Transport

  protected abstract resolveServerUrl(databaseId: string): string | Promise<string>
}
