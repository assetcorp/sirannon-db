import type { BulkLoadDurability, ChangeEvent, Params, ReadConcern, WriteConcern } from '../core/types.js'
import type {
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from '../server/protocol.js'

/** Optional behaviours for a CDC subscription. */
export interface SubscribeOptions {
  /**
   * Invoked when a reconnect cannot replay missed changes because they fell
   * outside the server's retained history. The subscription continues live
   * from the current moment; treat any prior state as stale and re-read.
   */
  onReset?: () => void
  deviceId?: string
  sinceSeq?: bigint
  epoch?: string
  onSubscribed?: (info: { seq: bigint | undefined; epoch: string | undefined; resync: boolean }) => void
}

/**
 * Transport layer for communicating with a sirannon-db server.
 * Each transport instance is bound to a specific database.
 */
export interface Transport {
  query(sql: string, params?: Params, readConcern?: ReadConcern): Promise<QueryResponse>
  execute(sql: string, params?: Params): Promise<ExecuteResponse>
  transaction(statements: Array<{ sql: string; params?: Params }>): Promise<TransactionResponse>
  batch(sql: string, paramsBatch: Params[], writeConcern?: WriteConcern): Promise<BatchResponse>
  load(sql: string, paramsBatch: Params[], durability?: BulkLoadDurability, checkpoint?: boolean): Promise<LoadResponse>
  subscribe(
    table: string,
    filter: Record<string, unknown> | undefined,
    callback: (event: ChangeEvent) => void,
    options?: SubscribeOptions,
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
  subscribe(callback: (event: ChangeEvent) => void, options?: SubscribeOptions): Promise<RemoteSubscription>
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
