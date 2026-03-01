// ---------------------------------------------------------------------------
// Wire protocol types for sirannon-db HTTP + WS server
// ---------------------------------------------------------------------------

import type { ExecuteResult } from '../core/types.js'

// ---------------------------------------------------------------------------
// HTTP request bodies
// ---------------------------------------------------------------------------

/** Body for POST /db/:id/query */
export interface QueryRequest {
  sql: string
  params?: Record<string, unknown> | unknown[]
}

/** Body for POST /db/:id/execute */
export interface ExecuteRequest {
  sql: string
  params?: Record<string, unknown> | unknown[]
}

/** A single statement within a transaction batch. */
export interface TransactionStatement {
  sql: string
  params?: Record<string, unknown> | unknown[]
}

/** Body for POST /db/:id/transaction */
export interface TransactionRequest {
  statements: TransactionStatement[]
}

// ---------------------------------------------------------------------------
// HTTP response bodies
// ---------------------------------------------------------------------------

/** Response for a successful query. */
export interface QueryResponse {
  rows: Record<string, unknown>[]
}

/** Response for a successful execute. */
export interface ExecuteResponse {
  changes: number
  lastInsertRowId: number | string
}

/** Response for a successful transaction. */
export interface TransactionResponse {
  results: ExecuteResponse[]
}

/** Standard error response envelope. */
export interface ErrorResponse {
  error: {
    code: string
    message: string
  }
}

// ---------------------------------------------------------------------------
// WebSocket message types (for Agent E, defined here for shared use)
// ---------------------------------------------------------------------------

/** Inbound WS message types. */
export type WSClientMessage = WSSubscribeMessage | WSUnsubscribeMessage | WSQueryMessage | WSExecuteMessage

export interface WSSubscribeMessage {
  type: 'subscribe'
  id: string
  table: string
  filter?: Record<string, unknown>
}

export interface WSUnsubscribeMessage {
  type: 'unsubscribe'
  id: string
}

export interface WSQueryMessage {
  type: 'query'
  id: string
  sql: string
  params?: Record<string, unknown> | unknown[]
}

export interface WSExecuteMessage {
  type: 'execute'
  id: string
  sql: string
  params?: Record<string, unknown> | unknown[]
}

/** Outbound WS message types. */
export type WSServerMessage =
  | WSSubscribedMessage
  | WSUnsubscribedMessage
  | WSChangeMessage
  | WSResultMessage
  | WSErrorMessage

export interface WSSubscribedMessage {
  type: 'subscribed'
  id: string
}

export interface WSUnsubscribedMessage {
  type: 'unsubscribed'
  id: string
}

export interface WSChangeMessage {
  type: 'change'
  id: string
  event: {
    type: 'insert' | 'update' | 'delete'
    table: string
    row: Record<string, unknown>
    oldRow?: Record<string, unknown>
    seq: string
    timestamp: number
  }
}

export interface WSResultMessage {
  type: 'result'
  id: string
  data: QueryResponse | ExecuteResponse
}

export interface WSErrorMessage {
  type: 'error'
  id: string
  error: {
    code: string
    message: string
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Convert an ExecuteResult (with possible bigint) to a JSON-safe response. */
export function toExecuteResponse(result: ExecuteResult): ExecuteResponse {
  return {
    changes: result.changes,
    lastInsertRowId:
      typeof result.lastInsertRowId === 'bigint' ? result.lastInsertRowId.toString() : result.lastInsertRowId,
  }
}
