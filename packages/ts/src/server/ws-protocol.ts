import type { BulkLoadDurability, ReadConcern, WriteConcern } from '../core/types.js'
import type { TransactionStatement } from './protocol.js'

/**
 * Every message that a client sends over the WebSocket. Each message has a `type`
 * and an `id` that the client chooses and the server repeats in its reply.
 *
 * @public
 */
export type WSClientMessage =
  | WSSubscribeMessage
  | WSUnsubscribeMessage
  | WSAckMessage
  | WSQueryMessage
  | WSExecuteMessage
  | WSTransactionMessage
  | WSBatchMessage
  | WSLoadMessage

/**
 * A request to open a change subscription on one or more tables, or a live query on a registered read.
 *
 * @public
 */
export interface WSSubscribeMessage {
  /** The message type for a subscribe. */
  type: 'subscribe'
  /** The identifier that the client chooses, which the server repeats in its replies and change events. */
  id: string
  /** The table to subscribe to. */
  table?: string
  /** Several tables to subscribe to at once. */
  tables?: string[]
  /** Narrows the subscription to rows whose columns equal these values. */
  filter?: Record<string, unknown>
  /** The name of a registered read, so that the server opens a live query on that read. */
  name?: string
  /** The arguments that the registered read takes. */
  args?: Record<string, unknown>
  /** The digest of the operation registry that the client built its live query against. */
  registryDigest?: string
  /**
   * The highest `seq` that the client has processed. When it is present, the server
   * replays every retained change with a greater seq before it sends live events, so
   * that a reconnecting subscriber receives the changes that it missed. When the server
   * answers with `resync: true`, it skips the replay. The client sends the value as a
   * decimal string, so that a value beyond `Number.MAX_SAFE_INTEGER` stays exact.
   */
  sinceSeq?: string
  /**
   * The `epoch` that the server reported with this cursor. A `sinceSeq` is valid
   * only within the sequence space that produced it, so when this epoch differs
   * from the server's own, the server answers with `resync: true` and skips the
   * replay.
   */
  epoch?: string
  /** The device's identifier, which makes this a device-sync subscription. */
  deviceId?: string
  /** The schema version of the device's local database, which the server checks before it opens the stream. */
  schemaVersion?: number
  /**
   * Set to true when this device stages pulled changes durably and
   * acknowledges staged sequences. The server then packs several events
   * into each `changes` frame and can pause the stream at any event. The
   * server accepts it only with `deviceId`.
   */
  stagedStream?: boolean
}

/**
 * A request to end a subscription.
 *
 * @public
 */
export interface WSUnsubscribeMessage {
  /** The message type for an unsubscribe. */
  type: 'unsubscribe'
  /** The identifier of the subscription to end. */
  id: string
}

/**
 * A device's acknowledgement of every change that it has stored up to a sequence.
 *
 * @public
 */
export interface WSAckMessage {
  /** The message type for an acknowledgement. */
  type: 'ack'
  /** The identifier that the client chooses, which the server repeats in its reply. */
  id: string
  /** The identifier of the device that sends the acknowledgement. */
  deviceId: string
  /** The highest sequence that the device has stored, as a decimal string. */
  seq: string
}

/**
 * A request to execute a read, either as SQL or by the name of a registered read.
 *
 * @public
 */
export interface WSQueryMessage {
  /** The message type for a read. */
  type: 'query'
  /** The identifier that the client chooses, which the server repeats in its reply. */
  id: string
  /** The statement to execute, which the server executes only when it accepts SQL. */
  sql?: string
  /** The values to bind to that statement, by name or by position. */
  params?: Record<string, unknown> | unknown[]
  /** The name of a registered read to execute in place of `sql`. */
  name?: string
  /** The arguments that the registered read takes. */
  args?: Record<string, unknown>
  /** The read concern that this read requires. */
  readConcern?: ReadConcern
}

/**
 * A request to execute a write, either as SQL or by the name of a registered write.
 *
 * @public
 */
export interface WSExecuteMessage {
  /** The message type for a write. */
  type: 'execute'
  /** The identifier that the client chooses, which the server repeats in its reply. */
  id: string
  /** The statement to execute, which the server executes only when it accepts SQL. */
  sql?: string
  /** The values to bind to that statement, by name or by position. */
  params?: Record<string, unknown> | unknown[]
  /** The name of a registered write to execute in place of `sql`. */
  name?: string
  /** The arguments that the registered write takes. */
  args?: Record<string, unknown>
  /** The acknowledgements that the server waits for before it confirms this write. */
  writeConcern?: WriteConcern
}

/**
 * A request to execute every statement in one server-side transaction, to which the
 * server replies once with all the results. The server makes no round trip to the
 * client between statements, so it holds the single writer lock only while the
 * statements execute.
 *
 * @public
 */
export interface WSTransactionMessage {
  /** The message type for a transaction. */
  type: 'transaction'
  /** The identifier that the client chooses, which the server repeats in its reply. */
  id: string
  /** The statements to execute, in order. */
  statements: TransactionStatement[]
  /** The acknowledgements that the server waits for before it confirms the transaction. */
  writeConcern?: WriteConcern
}

/**
 * A request to execute one statement with many parameter sets in a single server-side transaction.
 *
 * @public
 */
export interface WSBatchMessage {
  /** The message type for a batch. */
  type: 'batch'
  /** The identifier that the client chooses, which the server repeats in its reply. */
  id: string
  /** The statement to execute for each parameter set. */
  sql: string
  /** One parameter set for each execution of the statement. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** The acknowledgements that the server waits for before it confirms the batch. */
  writeConcern?: WriteConcern
}

/**
 * A request to import many rows at relaxed durability, which the server restores before it replies.
 *
 * @public
 */
export interface WSLoadMessage {
  /** The message type for a load. */
  type: 'load'
  /** The identifier that the client chooses, which the server repeats in its reply. */
  id: string
  /** The statement to execute for each parameter set. */
  sql: string
  /** One parameter set per row. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** The writer's durability level during the load. Defaults to 'off'. */
  durability?: BulkLoadDurability
  /** Whether the server checkpoints the WAL after this load. Defaults to true. */
  checkpoint?: boolean
}
