import { isBulkLoadDurability } from '../core/bulk-load.js'
import type { BulkLoadDurability, ClusterStatusInfo, ExecuteResult, ReadConcern, WriteConcern } from '../core/types.js'

/** Body for POST /db/:id/query */
export interface QueryRequest {
  sql: string
  params?: Record<string, unknown> | unknown[]
  readConcern?: ReadConcern
}

/** Body for POST /db/:id/execute */
export interface ExecuteRequest {
  sql: string
  params?: Record<string, unknown> | unknown[]
  writeConcern?: WriteConcern
}

/** A single statement within a transaction batch. */
export interface TransactionStatement {
  sql: string
  params?: Record<string, unknown> | unknown[]
}

/** Body for POST /db/:id/transaction */
export interface TransactionRequest {
  statements: TransactionStatement[]
  writeConcern?: WriteConcern
}

/**
 * Body for POST /db/:id/batch. Runs one SQL statement over many parameter
 * sets inside a single server-side transaction, so the whole batch commits
 * atomically with one fsync.
 */
export interface BatchRequest {
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  writeConcern?: WriteConcern
}

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

/** Response for a successful batch. */
export interface BatchResponse {
  results: ExecuteResponse[]
}

/**
 * Body for POST /db/:id/load. Loads rows with relaxed writer durability;
 * the database's configured durability is restored before the response is
 * sent. A load interrupted by a crash is recovered by re-running it.
 */
export interface LoadRequest {
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  durability?: BulkLoadDurability
}

/**
 * Response for a successful load. Aggregated rather than per-row so a
 * million-row load never echoes a million result objects back.
 */
export interface LoadResponse {
  rowsLoaded: number
  changes: number
}

/** Standard error response envelope. */
export interface ErrorResponse {
  error: {
    code: string
    message: string
    details?: Record<string, unknown>
  }
}

export type ClusterStatusResponse = Omit<ClusterStatusInfo, 'primaryTerm'> & {
  primaryTerm?: string
}

/** Inbound WS message types. */
export type WSClientMessage =
  | WSSubscribeMessage
  | WSUnsubscribeMessage
  | WSQueryMessage
  | WSExecuteMessage
  | WSTransactionMessage
  | WSBatchMessage
  | WSLoadMessage

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

/** One SQL statement applied over many parameter sets in one transaction. */
export interface WSBatchMessage {
  type: 'batch'
  id: string
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  writeConcern?: WriteConcern
}

/** Bulk load over the socket, mirroring POST /db/:id/load. */
export interface WSLoadMessage {
  type: 'load'
  id: string
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  durability?: BulkLoadDurability
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
  data: QueryResponse | ExecuteResponse | TransactionResponse | BatchResponse | LoadResponse
}

export interface WSErrorMessage {
  type: 'error'
  id: string
  error: {
    code: string
    message: string
  }
}

/** Convert an ExecuteResult (with possible bigint) to a JSON-safe response. */
export function toExecuteResponse(result: ExecuteResult): ExecuteResponse {
  return {
    changes: result.changes,
    lastInsertRowId:
      typeof result.lastInsertRowId === 'bigint' ? result.lastInsertRowId.toString() : result.lastInsertRowId,
  }
}

/**
 * Validate the optional load durability field shared by both transports.
 * Returns an error message for the client, or null when valid.
 */
export function loadDurabilityValidationError(value: unknown): string | null {
  if (value === undefined) return null
  if (!isBulkLoadDurability(value)) {
    return "Field \"durability\" must be 'off' or 'normal' when provided"
  }
  return null
}

/** Outcome of validating an optional request field shared by both transports. */
export type FieldValidation<T> = { ok: true; value: T | undefined } | { ok: false; message: string }

export function validateReadConcern(value: unknown): FieldValidation<ReadConcern> {
  if (value === undefined) return { ok: true, value: undefined }
  if (!isPlainRecord(value)) {
    return { ok: false, message: 'Field "readConcern" must be an object when provided' }
  }
  const keys = Object.keys(value)
  if (keys.length !== 1 || !keys.includes('level')) {
    return { ok: false, message: 'Field "readConcern" must contain only "level"' }
  }
  if (!isReadConcernLevel(value.level)) {
    return { ok: false, message: 'Field "readConcern.level" is invalid' }
  }
  return { ok: true, value: { level: value.level } }
}

export function validateWriteConcern(value: unknown): FieldValidation<WriteConcern> {
  if (value === undefined) return { ok: true, value: undefined }
  if (!isPlainRecord(value)) {
    return { ok: false, message: 'Field "writeConcern" must be an object when provided' }
  }
  const allowedKeys = new Set(['level', 'timeoutMs'])
  if (!Object.keys(value).every(key => allowedKeys.has(key))) {
    return { ok: false, message: 'Field "writeConcern" contains unsupported keys' }
  }
  if (!isWriteConcernLevel(value.level)) {
    return { ok: false, message: 'Field "writeConcern.level" is invalid' }
  }
  const timeoutMs = value.timeoutMs
  if (
    timeoutMs !== undefined &&
    (typeof timeoutMs !== 'number' || !Number.isSafeInteger(timeoutMs) || timeoutMs <= 0)
  ) {
    return { ok: false, message: 'Field "writeConcern.timeoutMs" must be a positive safe integer' }
  }
  return {
    ok: true,
    value: timeoutMs === undefined ? { level: value.level } : { level: value.level, timeoutMs },
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isReadConcernLevel(value: unknown): value is ReadConcern['level'] {
  return value === 'local' || value === 'majority' || value === 'linearizable'
}

function isWriteConcernLevel(value: unknown): value is WriteConcern['level'] {
  return value === 'local' || value === 'majority' || value === 'all'
}

/**
 * Validate a transaction's statement list identically for both transports, so
 * an input one transport accepts the other cannot silently reject. Returns an
 * error message for the client, or null when every entry carries a string sql
 * and, if present, an object or array params.
 */
export function transactionStatementsValidationError(value: unknown): string | null {
  if (!Array.isArray(value)) {
    return 'Field "statements" is required and must be an array'
  }
  if (value.length === 0) {
    return 'Transaction requires at least one statement'
  }
  for (let i = 0; i < value.length; i++) {
    const stmt = value[i]
    if (typeof stmt !== 'object' || stmt === null || typeof (stmt as { sql?: unknown }).sql !== 'string') {
      return `Statement at index ${i} is missing a valid "sql" field`
    }
    const params = (stmt as { params?: unknown }).params
    if (params !== undefined && params !== null && typeof params !== 'object') {
      return `Statement at index ${i} has invalid "params"`
    }
  }
  return null
}

/**
 * Validate a batch parameter list shared by the HTTP and WebSocket
 * transports. Returns a human-readable error message for the client, or null
 * when the value is a non-empty array whose entries are all objects or arrays.
 */
export function paramsBatchValidationError(value: unknown): string | null {
  if (!Array.isArray(value)) {
    return 'Field "paramsBatch" is required and must be an array'
  }
  if (value.length === 0) {
    return 'Field "paramsBatch" requires at least one parameter set'
  }
  for (let i = 0; i < value.length; i++) {
    const entry = value[i]
    if (typeof entry !== 'object' || entry === null) {
      return `Parameter set at index ${i} must be an object or array`
    }
  }
  return null
}
