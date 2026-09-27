import type {
  AckResponse,
  BatchResponse,
  ExecuteResponse,
  LoadResponse,
  QueryResponse,
  TransactionResponse,
} from './protocol.js'

/**
 * Every message that the server sends over the WebSocket.
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
 * One edit to a live query's result set, as a position and the row at that position.
 *
 * @public
 */
export type WSLiveOp =
  | {
      /** Which edit this is: `'insert'` for a row added, `'update'` for a row changed in place, `'delete'` for a row removed. */
      op: 'insert'
      /** The zero-based position in the result set that the edit applies to. */
      index: number
      /** The row that is now at that position. */
      row: unknown
    }
  | {
      /** The edit type for a row changed in place. */
      op: 'update'
      /** The position of the row that changed. */
      index: number
      /** The row's new values. */
      row: unknown
    }
  | {
      /** The edit type for a removed row. */
      op: 'delete'
      /** The position of the removed row. */
      index: number
    }

/**
 * A live query's new state, which is the edits that move it, a full replacement, or a notice that the server has started to read the query again.
 *
 * @public
 */
export interface WSLiveMessage {
  /** The message type for a live-query update. */
  type: 'live'
  /** The identifier of the subscription that this update is for. */
  id: string
  /** The edits that move the result set to its new state. */
  ops?: WSLiveOp[]
  /** A complete replacement result set. */
  rows?: unknown[]
  /** Set while the server reads the query again. */
  revalidating?: boolean
}

/**
 * The server's confirmation that a subscription is open, with the cursor and the epoch that it streams from.
 *
 * @public
 */
export interface WSSubscribedMessage {
  /** The message type for a subscription confirmation. */
  type: 'subscribed'
  /** The identifier of the subscription that is now open. */
  id: string
  /**
   * The number of changes that the server sends past the device's acknowledged
   * cursor before it pauses delivery. The server sends it only for a device subscription.
   */
  maxUnacknowledgedChanges?: number
  /**
   * The seq from which the subscription is live. A client that has not yet received
   * any change stores this seq as its resume cursor, so that after a reconnect during
   * a quiet period, the server still replays every change that the client missed.
   */
  seq?: string
  /**
   * Set when the server cannot replay from the requested `sinceSeq`, because that seq
   * is older than the history that the server retains, the client's `epoch` differs
   * from the server's, or the replay fails. The server skips the replay and the
   * subscription starts from the server's current position, so the client must treat
   * its earlier state as stale and read it again.
   */
  resync?: boolean
  /**
   * The identifier of the sequence space that this subscription streams from. The
   * client stores it and sends it back when it resumes, so that the server answers a
   * cursor from a different database with `resync: true`.
   */
  epoch?: string
  /** The first result set of a live query, which the server sends when the subscription names a registered read. */
  rows?: unknown[]
}

/**
 * The server's confirmation that a subscription has ended.
 *
 * @public
 */
export interface WSUnsubscribedMessage {
  /** The message type for an unsubscribe confirmation. */
  type: 'unsubscribed'
  /** The identifier of the subscription that has ended. */
  id: string
}

/**
 * One change event in its wire form, with the sequence as a decimal string,
 * so that JSON keeps a value beyond the safe integer range exact.
 *
 * @public
 */
export interface WSWireChangeEvent {
  /** Whether the row was inserted, updated, or deleted. */
  type: 'insert' | 'update' | 'delete'
  /** The table of the row. */
  table: string
  /** The row's values after the change. */
  row: Record<string, unknown>
  /** The row's values before an update or a delete. */
  oldRow?: Record<string, unknown>
  /** The position of this change in the database's change log, as a decimal string. */
  seq: string
  /** Milliseconds since the Unix epoch, from when the database recorded the change. */
  timestamp: number
  /** The hybrid logical clock stamp that the writing node gave this change. */
  hlc?: string
  /** The identifier of the node or device that wrote the change. */
  origin?: string
  /** The primary key of the changed row, encoded as a string. */
  rowId?: string
  /** The identifier of the transaction that made this change. */
  txId?: string
  /** Set on the last change of a transaction. */
  txEnd?: boolean
}

/**
 * One change event for a subscriber.
 *
 * @public
 */
export interface WSChangeMessage {
  /** The message type for a single change event. */
  type: 'change'
  /** The identifier of the subscription that this change is for. */
  id: string
  /** The change itself. */
  event: WSWireChangeEvent
}

/**
 * Several change events in one frame, in ascending seq order. The server sends
 * this frame only on a device subscription that asks for `stagedStream`, and
 * each event has the same fields as the event in a `change` frame.
 *
 * @public
 */
export interface WSChangesMessage {
  /** The message type for several change events. */
  type: 'changes'
  /** The identifier of the subscription that these changes are for. */
  id: string
  /** The changes, in ascending sequence order. */
  events: WSWireChangeEvent[]
}

/**
 * The server's reply to a read, write, transaction, batch, load, or acknowledgement.
 *
 * @public
 */
export interface WSResultMessage {
  /** The message type for a reply to a read, write, transaction, batch, load, or acknowledgement. */
  type: 'result'
  /** The identifier from the request. */
  id: string
  /** The reply body, whose shape depends on the request. */
  data: QueryResponse | ExecuteResponse | TransactionResponse | BatchResponse | LoadResponse | AckResponse
}

/**
 * The server's report that a request failed.
 *
 * @public
 */
export interface WSErrorMessage {
  /** The message type for a failure. */
  type: 'error'
  /** The identifier from the request, or an empty string when the server cannot read one. */
  id: string
  /** A machine-readable code and a human-readable message. */
  error: {
    code: string
    message: string
  }
}
