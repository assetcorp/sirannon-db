import type { ClientOptions } from '../core/types.js'
import { RemoteDatabase } from './database-proxy.js'
import { HttpTransport } from './transport/http.js'
import { WebSocketTransport } from './transport/ws.js'
import type { Transport } from './types.js'

/**
 * Client for a remote sirannon-db server. Creates {@link RemoteDatabase}
 * instances that communicate with the server over HTTP or WebSocket.
 *
 * ```ts
 * const client = new SirannonClient('http://localhost:9876', {
 *   transport: 'websocket',
 * })
 *
 * const db = client.database('main')
 * const rows = await db.query('SELECT * FROM users')
 *
 * client.close()
 * ```
 */
export class SirannonClient {
  private readonly baseUrl: string
  private readonly wsBaseUrl: string
  private readonly transport: 'websocket' | 'http'
  private readonly headers: Record<string, string> | undefined
  private readonly autoReconnect: boolean
  private readonly reconnectInterval: number
  private readonly databases = new Map<string, RemoteDatabase>()
  private closed = false

  constructor(url: string, options?: ClientOptions) {
    this.baseUrl = url.replace(/\/$/, '')

    this.wsBaseUrl = this.baseUrl
      .replace(/^http:\/\//i, 'ws://')
      .replace(/^https:\/\//i, 'wss://')

    this.transport = options?.transport ?? 'websocket'
    this.headers = options?.headers
    this.autoReconnect = options?.autoReconnect ?? true
    this.reconnectInterval = options?.reconnectInterval ?? 1000
  }

  /**
   * Get a {@link RemoteDatabase} proxy for the given database ID.
   * Returns a cached instance if one already exists for this ID.
   *
   * The underlying transport connection is established lazily on
   * the first operation (query, execute, or subscribe).
   */
  database(id: string): RemoteDatabase {
    if (this.closed) {
      throw new Error('Client is closed')
    }

    const existing = this.databases.get(id)
    if (existing) {
      return existing
    }

    const transport = this.createTransport(id)
    const db = new RemoteDatabase(id, transport, () => {
      this.databases.delete(id)
    })
    this.databases.set(id, db)
    return db
  }

  /**
   * Close all database connections and release resources.
   * After calling `close()`, new calls to `database()` will throw.
   */
  close(): void {
    this.closed = true
    for (const db of this.databases.values()) {
      db.close()
    }
    this.databases.clear()
  }

  private createTransport(databaseId: string): Transport {
    const encodedId = encodeURIComponent(databaseId)

    if (this.transport === 'http') {
      return new HttpTransport(
        `${this.baseUrl}/db/${encodedId}`,
        this.headers,
      )
    }

    return new WebSocketTransport(
      `${this.wsBaseUrl}/db/${encodedId}`,
      {
        autoReconnect: this.autoReconnect,
        reconnectInterval: this.reconnectInterval,
      },
    )
  }
}
