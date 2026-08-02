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

export type WSClientMessage =
  | WSSubscribeMessage
  | WSUnsubscribeMessage
  | WSAckMessage
  | WSQueryMessage
  | WSExecuteMessage
  | WSTransactionMessage
  | WSBatchMessage
  | WSLoadMessage

export interface WSSubscribeMessage {
  type: 'subscribe'
  id: string
  table?: string
  tables?: string[]
  filter?: Record<string, unknown>
  name?: string
  args?: Record<string, unknown>
  registryDigest?: string
  /**
   * Highest `seq` the client has already processed. When present, the server
   * replays every retained change with a greater seq before delivering live
   * events, so a reconnecting subscriber does not miss changes. Sent as a
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
  deviceId?: string
  schemaVersion?: number
  /**
   * Declares that this device stages pulled changes durably and
   * acknowledges staged sequences. The server then packs several events
   * into each `changes` frame and paces the delivery window continuously
   * instead of per transaction. Meaningful only with `deviceId`.
   */
  stagedStream?: boolean
}

export interface WSUnsubscribeMessage {
  type: 'unsubscribe'
  id: string
}

export interface WSAckMessage {
  type: 'ack'
  id: string
  deviceId: string
  seq: string
}

export interface WSQueryMessage {
  type: 'query'
  id: string
  sql?: string
  params?: Record<string, unknown> | unknown[]
  name?: string
  args?: Record<string, unknown>
  readConcern?: ReadConcern
}

export interface WSExecuteMessage {
  type: 'execute'
  id: string
  sql?: string
  params?: Record<string, unknown> | unknown[]
  name?: string
  args?: Record<string, unknown>
  writeConcern?: WriteConcern
}

/**
 * Runs every statement in one server-side transaction and replies once with
 * all results. The client is never in the loop between statements, so the
 * single writer lock is held only for the duration of local execution.
 */
export interface WSTransactionMessage {
  type: 'transaction'
  id: string
  statements: TransactionStatement[]
  writeConcern?: WriteConcern
}

export interface WSBatchMessage {
  type: 'batch'
  id: string
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  writeConcern?: WriteConcern
}

export interface WSLoadMessage {
  type: 'load'
  id: string
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  durability?: BulkLoadDurability
  checkpoint?: boolean
}

export type WSServerMessage =
  | WSSubscribedMessage
  | WSUnsubscribedMessage
  | WSChangeMessage
  | WSChangesMessage
  | WSLiveMessage
  | WSResultMessage
  | WSErrorMessage

export type WSLiveOp =
  | { op: 'insert'; index: number; row: unknown }
  | { op: 'update'; index: number; row: unknown }
  | { op: 'delete'; index: number }

export interface WSLiveMessage {
  type: 'live'
  id: string
  ops?: WSLiveOp[]
  rows?: unknown[]
  revalidating?: boolean
}

export interface WSSubscribedMessage {
  type: 'subscribed'
  id: string
  /**
   * How far a device may run ahead of its acknowledged cursor before the
   * server holds delivery. Present only for a device subscription.
   */
  maxUnacknowledgedChanges?: number
  /**
   * The seq the subscription is live from. A client that has not yet seen any
   * change adopts this as its resume cursor, so a reconnect during an idle
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
   * stores it and echoes it when resuming, so a cursor carried to a different
   * database forces a resync instead of a silent replay of unrelated rows.
   */
  epoch?: string
  rows?: unknown[]
}

export interface WSUnsubscribedMessage {
  type: 'unsubscribed'
  id: string
}

export interface WSWireChangeEvent {
  type: 'insert' | 'update' | 'delete'
  table: string
  row: Record<string, unknown>
  oldRow?: Record<string, unknown>
  seq: string
  timestamp: number
  hlc?: string
  origin?: string
  rowId?: string
  txId?: string
  txEnd?: boolean
}

export interface WSChangeMessage {
  type: 'change'
  id: string
  event: WSWireChangeEvent
}

/**
 * Several change events in one frame, in ascending seq order. Sent only on
 * a device subscription that requested `stagedStream`; the events carry the
 * same fields as a `change` frame's event.
 */
export interface WSChangesMessage {
  type: 'changes'
  id: string
  events: WSWireChangeEvent[]
}

export interface WSResultMessage {
  type: 'result'
  id: string
  data: QueryResponse | ExecuteResponse | TransactionResponse | BatchResponse | LoadResponse | AckResponse
}

export interface WSErrorMessage {
  type: 'error'
  id: string
  error: {
    code: string
    message: string
  }
}
