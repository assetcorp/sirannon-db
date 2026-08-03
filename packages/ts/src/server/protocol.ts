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
  WSChangeMessage,
  WSChangesMessage,
  WSClientMessage,
  WSErrorMessage,
  WSExecuteMessage,
  WSLiveMessage,
  WSLiveOp,
  WSLoadMessage,
  WSQueryMessage,
  WSResultMessage,
  WSServerMessage,
  WSSubscribedMessage,
  WSSubscribeMessage,
  WSTransactionMessage,
  WSUnsubscribedMessage,
  WSUnsubscribeMessage,
  WSWireChangeEvent,
} from './ws-protocol.js'

/**
 * Body of `POST /db/{id}/query`.
 *
 * @public
 */
export interface QueryRequest {
  /** The statement to run. */
  sql: string
  /** Values bound to the statement, named or positional. */
  params?: Record<string, unknown> | unknown[]
  /** Currency this read requires. */
  readConcern?: ReadConcern
}

/**
 * Body of `POST /db/{id}/execute`.
 *
 * @public
 */
export interface ExecuteRequest {
  /** The statement to run. */
  sql: string
  /** Values bound to the statement, named or positional. */
  params?: Record<string, unknown> | unknown[]
  /** Acknowledgements this write waits for. */
  writeConcern?: WriteConcern
}

/**
 * One statement inside a transaction or a registered write.
 *
 * @public
 */
export interface TransactionStatement {
  /** The statement to run. */
  sql: string
  /** Values bound to the statement, named or positional. */
  params?: Record<string, unknown> | unknown[]
}

/**
 * Body of `POST /db/{id}/transaction`, whose statements all succeed or all fail.
 *
 * @public
 */
export interface TransactionRequest {
  /** The statements to run, in order. */
  statements: TransactionStatement[]
  /** Acknowledgements the transaction waits for. */
  writeConcern?: WriteConcern
}

/** The whole batch commits atomically in one server-side transaction with one fsync.
 * @public
 */
export interface BatchRequest {
  /** The statement to run for each parameter set. */
  sql: string
  /** One parameter set per run. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** Acknowledgements the batch waits for. */
  writeConcern?: WriteConcern
}

/**
 * What a read route answers with.
 *
 * @public
 */
export interface QueryResponse {
  /** The rows the statement produced, with blobs and large integers in their tagged wire form. */
  rows: Record<string, unknown>[]
}

/**
 * What a write route answers with.
 *
 * @public
 */
export interface ExecuteResponse {
  /** Number of rows the statement inserted, updated, or deleted. */
  changes: number
  /** Row id SQLite assigned to the last inserted row, as a decimal string when it exceeds the safe range. */
  lastInsertRowId: number | string
}

/**
 * What a transaction route answers with.
 *
 * @public
 */
export interface TransactionResponse {
  /** One result per statement, in the order the transaction ran them. */
  results: ExecuteResponse[]
}

/**
 * What a batch route answers with.
 *
 * @public
 */
export interface BatchResponse {
  /** One result per parameter set, in order. */
  results: ExecuteResponse[]
}

/**
 * Loads rows with relaxed writer durability; the configured durability is
 * restored before the response is sent, and a load interrupted by a crash is
 * recovered by re-running it.
 *
 * @public
 */
export interface LoadRequest {
  /** The statement to run for each parameter set. */
  sql: string
  /** One parameter set per row. */
  paramsBatch: (Record<string, unknown> | unknown[])[]
  /** Durability in force while the load runs. Default: 'off'. */
  durability?: BulkLoadDurability
  /** Whether this load ends with a checkpoint. Set it false on every batch but the last of a multi-batch import. */
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
 * The body every failed route answers with.
 *
 * @public
 */
export interface ErrorResponse {
  /** Machine-readable code, human-readable message, and anything else the route attached. */
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
 * Turns a write result into its wire form, encoding a row id beyond the safe
 * integer range as a decimal string.
 *
 * @param result - What the write reported locally.
 * @returns The result as it crosses the wire.
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
