import type { ChangeEvent, Params } from '../core/types.js'
import type { ExecuteResponse, QueryResponse, TransactionResponse } from '../server/protocol.js'

/**
 * Transport layer for communicating with a sirannon-db server.
 * Each transport instance is bound to a specific database.
 */
export interface Transport {
  query(sql: string, params?: Params): Promise<QueryResponse>
  execute(sql: string, params?: Params): Promise<ExecuteResponse>
  transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse>
  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
  ): Promise<RemoteSubscription>
  close(): void
}

/** Handle for an active remote subscription. */
export interface RemoteSubscription {
  unsubscribe(): void
}

/** Builder for creating remote CDC subscriptions with optional filters. */
export interface RemoteSubscriptionBuilder {
  filter(conditions: Record<string, unknown>): RemoteSubscriptionBuilder
  subscribe(callback: (event: ChangeEvent) => void): Promise<RemoteSubscription>
}

/**
 * Error originating from a remote sirannon-db server.
 * Carries the machine-readable error code from the server's error response.
 */
export class RemoteError extends Error {
  readonly code: string

  constructor(code: string, message: string) {
    super(message)
    this.name = 'RemoteError'
    this.code = code
  }
}
