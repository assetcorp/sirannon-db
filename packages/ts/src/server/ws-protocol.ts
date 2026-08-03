import type { BulkLoadDurability, ReadConcern, WriteConcern } from '../core/types.js'
import type {
  AckResponse,
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
  TransactionStatement,
} from './protocol.js'

/**
 * Every message a client sends over the WebSocket. Each carries a `type` and a
 * client-chosen `id` the reply echoes.
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
 * Opens a change subscription on one or more tables, or a live query on a registered read.
 *
 * @public
 */
export interface WSSubscribeMessage {
  /** Names this message as a subscribe. */
  type: 'subscribe'
  /** Client-chosen identifier the replies and change events echo. */
  id: string
  /** Table to subscribe to. */
  table?: string
  /** Several tables to subscribe to at once. */
  tables?: string[]
  /** Narrows the subscription to rows whose columns equal these values. */
  filter?: Record<string, unknown>
  /** Name of a registered read, which opens a live query instead of a change subscription. */
  name?: string
  /** Arguments that registered read takes. */
  args?: Record<string, unknown>
  /** Digest of the operation registry the client built its live query against. */
  registryDigest?: string
  /**
   * Highest `seq` the client has already processed. When present, the server
   * replays every retained change with a greater seq before delivering live
   * events so that a reconnecting subscriber does not miss changes. Sent as a
   * decimal string to preserve values beyond `Number.MAX_SAFE_INTEGER`.
   */
  sinceSeq?: string
  /**
   * The `epoch` the server reported when this cursor was issued. A `sinceSeq`
   * only means something within the sequence space that produced it, so a
   * mismatch tells the server the cursor came from another database and it must
   * resync rather than replay foreign rows against it.
   */
  epoch?: string
  /** Identifies the device, which turns this into a device-sync subscription. */
  deviceId?: string
  /** Schema version the device's local database is at, which the server gates the stream on. */
  schemaVersion?: number
  /**
   * Declares that this device stages pulled changes durably and
   * acknowledges staged sequences. The server then packs several events
   * into each `changes` frame and paces the delivery window continuously
   * instead of per transaction. Meaningful only with `deviceId`.
   */
  stagedStream?: boolean
}

/**
 * Ends a subscription.
 *
 * @public
 */
export interface WSUnsubscribeMessage {
  /** Names this message as an unsubscribe. */
  type: 'unsubscribe'
  /** Identifier of the subscription to end. */
  id: string
}

/**
 * Acknowledges every change a device has stored up to a sequence.
 *
 * @public
 */
export interface WSAckMessage {
  /** Names this message as an acknowledgement. */
  type: 'ack'
  /** Client-chosen identifier the reply echoes. */
  id: string
  /** Identifies the device acknowledging. */
  deviceId: string
  /** Highest sequence the device has stored, as a decimal string. */
  seq: string
}

/**
 * Runs a read, either as SQL or by the name of a registered read.
 *
 * @public
 */
export interface WSQueryMessage {
  /** Names this message as a read. */
  type: 'query'
  /** Client-chosen identifier the reply echoes. */
  id: string
  /** The statement to run. The server refuses it unless it accepts SQL. */
  sql?: string
  /** Values bound to that statement, named or positional. */
  params?: Record<string, unknown> | unknown[]
  /** Name of a registered read to run instead, which carries no SQL. */
  name?: string
  /** Arguments that registered read takes. */
  args?: Record<string, unknown>
  /** Currency this read requires. */
  readConcern?: ReadConcern
}

/**
 * Runs a write, either as SQL or by the name of a registered write.
 *
 * @public
 */
export interface WSExecuteMessage {
  /** Names this message as a write. */
  type: 'execute'
  /** Client-chosen identifier the reply echoes. */
  id: string
  /** The statement to run. The server refuses it unless it accepts SQL. */
  sql?: string
  /** Values bound to that statement, named or positional. */
  params?: Record<string, unknown> | unknown[]
  /** Name of a registered write to run instead, which carries no SQL. */
  name?: string
  /** Arguments that registered write takes. */
  args?: Record<string, unknown>
  /** Acknowledgements this write waits for. */
  writeConcern?: WriteConcern
}

/**
 * Runs every statement in one server-side transaction and replies once with
 * all results. The client is never in the loop between statements, so the
 * single writer lock is held only for the duration of local execution.
 *
 * @public
 */
export interface WSTransactionMessage {
  /** Names this message as a transaction. */
  type: 'transaction'
  /** Client-chosen identifier the reply echoes. */
  id: string
  /** The statements to run, in order. */
  statements: TransactionStatement[]
  /** Acknowledgements the transaction waits for. */
  writeConcern?: WriteConcern
}

/**
 * Applies one statement over many parameter sets in a single server-side transaction.
 *
 * @public
 */
export interface WSBatchMessage {
  /** Names this message as a batch. */
  type: 'batch'
  /** Client-chosen identifier the reply echoes. */
  id: string
  /** The statement to run for each parameter set. */
  sql: string
  /** One parameter set per run. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** Acknowledgements the batch waits for. */
  writeConcern?: WriteConcern
}

/**
 * Imports many rows at relaxed durability, which the server restores before it replies.
 *
 * @public
 */
export interface WSLoadMessage {
  /** Names this message as a load. */
  type: 'load'
  /** Client-chosen identifier the reply echoes. */
  id: string
  /** The statement to run for each parameter set. */
  sql: string
  /** One parameter set per row. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** Durability in force while the load runs. Default: 'off'. */
  durability?: BulkLoadDurability
  /** Whether this load ends with a checkpoint. */
  checkpoint?: boolean
}

/**
 * Every message the server sends over the WebSocket.
 *
 * @public
 */
export type WSServerMessage =
  | WSSubscribedMessage
  | WSUnsubscribedMessage
  | WSChangeMessage
  | WSChangesMessage
  | WSLiveMessage
  | WSResultMessage
  | WSErrorMessage

/**
 * One edit to a live query's result set, as a position and the row at it.
 *
 * @public
 */
export type WSLiveOp =
  | { op: 'insert'; index: number; row: unknown }
  | { op: 'update'; index: number; row: unknown }
  | { op: 'delete'; index: number }

/**
 * Carries a live query's new state: the edits that move it, a full replacement, or notice that a re-read has started.
 *
 * @public
 */
export interface WSLiveMessage {
  /** Names this message as a live-query update. */
  type: 'live'
  /** Identifier of the subscription this update belongs to. */
  id: string
  /** The edits that move the result set to its new state. */
  ops?: WSLiveOp[]
  /** A complete replacement result set. */
  rows?: unknown[]
  /** Set while the server re-reads the query. */
  revalidating?: boolean
}

/**
 * Confirms a subscription opened, and states the cursor and sequence space it streams from.
 *
 * @public
 */
export interface WSSubscribedMessage {
  /** Names this message as a subscription confirmation. */
  type: 'subscribed'
  /** Identifier of the subscription that opened. */
  id: string
  /**
   * How far a device may run ahead of its acknowledged cursor before the
   * server holds delivery. Present only for a device subscription.
   */
  maxUnacknowledgedChanges?: number
  /**
   * The seq the subscription is live from. A client that has not yet seen any
   * change adopts this as its resume cursor so that a reconnect during an idle
   * spell still replays what it missed instead of silently skipping it.
   */
  seq?: string
  /**
   * Set when a requested `sinceSeq` fell below the retained history, so the
   * gap cannot be replayed. The subscription still starts live from now; the
   * client must treat its prior state as stale and re-read.
   */
  resync?: boolean
  /**
   * Identifies the sequence space this subscription streams from. The client
   * stores it and echoes it when resuming so that a cursor carried to a different
   * database forces a resync instead of a silent replay of unrelated rows.
   */
  epoch?: string
  /** First result set of a live query, sent when the subscription named a registered read. */
  rows?: unknown[]
}

/**
 * Confirms a subscription ended.
 *
 * @public
 */
export interface WSUnsubscribedMessage {
  /** Names this message as an unsubscribe confirmation. */
  type: 'unsubscribed'
  /** Identifier of the subscription that ended. */
  id: string
}

/**
 * One change event as it crosses the wire, with sequences as decimal strings
 * so a value beyond the safe integer range survives JSON.
 *
 * @public
 */
export interface WSWireChangeEvent {
  /** Whether the row was inserted, updated, or deleted. */
  type: 'insert' | 'update' | 'delete'
  /** Table the row belongs to. */
  table: string
  /** The row as it stands after the change. */
  row: Record<string, unknown>
  /** The row as it stood before an update or a delete. */
  oldRow?: Record<string, unknown>
  /** Position of this change in the database's change log, as a decimal string. */
  seq: string
  /** Milliseconds since the Unix epoch, taken when the change was recorded. */
  timestamp: number
  /** Hybrid logical clock stamp the writing node gave this change. */
  hlc?: string
  /** Identifier of the node that authored the change. */
  origin?: string
  /** Primary key of the changed row, encoded as a string. */
  rowId?: string
  /** Identifier of the transaction that produced this change. */
  txId?: string
  /** Set on the last change of a transaction. */
  txEnd?: boolean
}

/**
 * Carries one change event to a subscriber.
 *
 * @public
 */
export interface WSChangeMessage {
  /** Names this message as a single change event. */
  type: 'change'
  /** Identifier of the subscription this change belongs to. */
  id: string
  /** The change itself. */
  event: WSWireChangeEvent
}

/**
 * Several change events in one frame, in ascending seq order. Sent only on
 * a device subscription that requested `stagedStream`; the events carry the
 * same fields as a `change` frame's event.
 *
 * @public
 */
export interface WSChangesMessage {
  /** Names this message as a run of change events. */
  type: 'changes'
  /** Identifier of the subscription these changes belong to. */
  id: string
  /** The changes, in ascending sequence order. */
  events: WSWireChangeEvent[]
}

/**
 * Replies to a read, write, transaction, batch, load, or acknowledgement.
 *
 * @public
 */
export interface WSResultMessage {
  /** Names this message as a reply to a read, write, transaction, batch, load, or acknowledgement. */
  type: 'result'
  /** Identifier the request carried. */
  id: string
  /** The reply body, whose shape follows the request that produced it. */
  data: QueryResponse | ExecuteResponse | TransactionResponse | BatchResponse | LoadResponse | AckResponse
}

/**
 * Reports that a request failed.
 *
 * @public
 */
export interface WSErrorMessage {
  /** Names this message as a failure. */
  type: 'error'
  /** Identifier the request carried. */
  id: string
  /** Machine-readable code and human-readable message. */
  error: {
    code: string
    message: string
  }
}
