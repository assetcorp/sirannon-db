import { isBulkLoadDurability } from '../core/bulk-load.js'
import { decodeTaggedValues } from '../core/cdc/encoding.js'
import type {
  BulkLoadDurability,
  BulkLoadOptions,
  BulkLoadResult,
  ClusterStatusInfo,
  ExecuteResult,
  ReadConcern,
  WriteConcern,
} from '../core/types.js'

export type {
  WSAckMessage,
  WSBatchMessage,
  WSClientMessage,
  WSExecuteMessage,
  WSLoadMessage,
  WSQueryMessage,
  WSSubscribeMessage,
  WSTransactionMessage,
  WSUnsubscribeMessage,
} from './ws-protocol.js'
export type {
  WSChangeMessage,
  WSChangesMessage,
  WSErrorMessage,
  WSLiveMessage,
  WSLiveOp,
  WSResultMessage,
  WSServerMessage,
  WSSubscribedMessage,
  WSUnsubscribedMessage,
  WSWireChangeEvent,
} from './ws-server-messages.js'

/**
 * Body of `POST /db/{id}/query`.
 *
 * @public
 */
export interface QueryRequest {
  /** The statement to execute. */
  sql: string
  /** The values to bind to the statement, by name or by position. */
  params?: Record<string, unknown> | unknown[]
  /** The read concern that this read requires. */
  readConcern?: ReadConcern
}

/**
 * Body of `POST /db/{id}/execute`.
 *
 * @public
 */
export interface ExecuteRequest {
  /** The statement to execute. */
  sql: string
  /** The values to bind to the statement, by name or by position. */
  params?: Record<string, unknown> | unknown[]
  /** The acknowledgements that the server waits for before it confirms this write. */
  writeConcern?: WriteConcern
}

/**
 * One statement inside a transaction or a registered write.
 *
 * @public
 */
export interface TransactionStatement {
  /** The statement to execute. */
  sql: string
  /** The values to bind to the statement, by name or by position. */
  params?: Record<string, unknown> | unknown[]
}

/**
 * Body of `POST /db/{id}/transaction`, whose statements all succeed or all fail.
 *
 * @public
 */
export interface TransactionRequest {
  /** The statements to execute, in order. */
  statements: TransactionStatement[]
  /** The acknowledgements that the server waits for before it confirms the transaction. */
  writeConcern?: WriteConcern
}

/** The body of `POST /db/{id}/batch`, which the server commits atomically in one transaction with one fsync.
 * @public
 */
export interface BatchRequest {
  /** The statement to execute for each parameter set. */
  sql: string
  /** One parameter set for each execution of the statement. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** The acknowledgements that the server waits for before it confirms the batch. */
  writeConcern?: WriteConcern
}

/**
 * The body that a read route returns.
 *
 * @public
 */
export interface QueryResponse {
  /** The rows that the statement returned, with blobs and large integers in their tagged wire form. */
  rows: Record<string, unknown>[]
}

/**
 * The body that a write route returns.
 *
 * @public
 */
export interface ExecuteResponse {
  /** The number of rows that the statement inserted, updated, or deleted. */
  changes: number
  /** The row id that SQLite assigned to the last inserted row, as a decimal string when it exceeds the safe integer range. */
  lastInsertRowId: number | string
}

/**
 * The body that a transaction route returns.
 *
 * @public
 */
export interface TransactionResponse {
  /** One result per statement, in the order that the transaction executed them. */
  results: ExecuteResponse[]
}

/**
 * The body that a batch route returns.
 *
 * @public
 */
export interface BatchResponse {
  /** One result per parameter set, in order. */
  results: ExecuteResponse[]
}

/**
 * The body of `POST /db/{id}/load`, which loads rows while the server relaxes the
 * writer's durability. The server restores the configured durability before it
 * sends the response. When the process crashes during a load, SQLite rolls back the
 * uncommitted rows, so the client can send the load again.
 *
 * @public
 */
export interface LoadRequest {
  /** The statement to execute for each parameter set. */
  sql: string
  /** One parameter set per row. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** The writer's durability level during the load. Defaults to 'off'. */
  durability?: BulkLoadDurability
  /** Whether the server checkpoints the WAL after this load. Defaults to true, so set it to false on every batch except the last of a multi-batch import. */
  checkpoint?: boolean
}

/**
 * How many rows a bulk load applied and how many rows changed.
 *
 * @public
 */
export type LoadResponse = BulkLoadResult

export interface AckResponse {
  acked: boolean
  seq: string
}

/**
 * The body that every failed route returns.
 *
 * @public
 */
export interface ErrorResponse {
  /** A machine-readable code, a human-readable message, and any details that the route adds. */
  error: {
    code: string
    message: string
    details?: Record<string, unknown>
  }
}

export type ClusterStatusResponse = Omit<ClusterStatusInfo, 'primaryTerm'> & {
  primaryTerm?: string
}

/**
 * Converts a write result into its wire form, and encodes a row id beyond the
 * safe integer range as a decimal string.
 *
 * @param result - The result that the local write returns.
 * @returns The result in its wire form.
 *
 * @public
 */
export function toExecuteResponse(result: ExecuteResult): ExecuteResponse {
  return {
    changes: result.changes,
    lastInsertRowId:
      typeof result.lastInsertRowId === 'bigint' ? result.lastInsertRowId.toString() : result.lastInsertRowId,
  }
}

export function loadDurabilityValidationError(value: unknown): string | null {
  if (value === undefined) return null
  if (!isBulkLoadDurability(value)) {
    return "Field \"durability\" must be 'off' or 'normal' when provided"
  }
  return null
}

export function loadCheckpointValidationError(value: unknown): string | null {
  if (value === undefined) return null
  if (typeof value !== 'boolean') {
    return 'Field "checkpoint" must be a boolean when provided'
  }
  return null
}

export function toBulkLoadOptions(source: {
  durability?: BulkLoadDurability
  checkpoint?: boolean
}): BulkLoadOptions | undefined {
  if (source.durability === undefined && source.checkpoint === undefined) return undefined
  const options: BulkLoadOptions = {}
  if (source.durability !== undefined) options.durability = source.durability
  if (source.checkpoint !== undefined) options.checkpoint = source.checkpoint
  return options
}

export type FieldValidation<T> = { ok: true; value: T | undefined } | { ok: false; message: string }

export function decodeBoundParams(value: unknown, field: string): FieldValidation<Record<string, unknown> | unknown[]> {
  if (value === undefined || value === null) return { ok: true, value: undefined }
  try {
    return { ok: true, value: decodeTaggedValues(value) as Record<string, unknown> | unknown[] }
  } catch {
    return { ok: false, message: `Field "${field}" contains an invalid tagged value` }
  }
}

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

export function transactionStatementsValidationError(value: unknown): string | null {
  if (!Array.isArray(value)) {
    return 'Field "statements" is required and must be an array'
  }
  if (value.length === 0) {
    return 'Transaction requires at least one statement'
  }
  for (let i = 0; i < value.length; i++) {
    const stmt = value[i]
    if (typeof stmt !== 'object' || stmt === null) {
      return `Statement at index ${i} is missing a valid "sql" field`
    }
    const sql = (stmt as { sql?: unknown }).sql
    if (typeof sql !== 'string' || sql.length === 0) {
      return `Statement at index ${i} is missing a valid "sql" field`
    }
    const params = (stmt as { params?: unknown }).params
    if (params !== undefined && params !== null && typeof params !== 'object') {
      return `Statement at index ${i} has invalid "params"`
    }
  }
  return null
}

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
