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

export interface QueryRequest {
  sql: string
  params?: Record<string, unknown> | unknown[]
  readConcern?: ReadConcern
}

export interface ExecuteRequest {
  sql: string
  params?: Record<string, unknown> | unknown[]
  writeConcern?: WriteConcern
}

export interface TransactionStatement {
  sql: string
  params?: Record<string, unknown> | unknown[]
}

export interface TransactionRequest {
  statements: TransactionStatement[]
  writeConcern?: WriteConcern
}

/** The whole batch commits atomically in one server-side transaction with one fsync. */
export interface BatchRequest {
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  writeConcern?: WriteConcern
}

export interface QueryResponse {
  rows: Record<string, unknown>[]
}

export interface ExecuteResponse {
  changes: number
  lastInsertRowId: number | string
}

export interface TransactionResponse {
  results: ExecuteResponse[]
}

export interface BatchResponse {
  results: ExecuteResponse[]
}

/**
 * Loads rows with relaxed writer durability; the configured durability is
 * restored before the response is sent, and a load interrupted by a crash is
 * recovered by re-running it.
 */
export interface LoadRequest {
  sql: string
  paramsBatch: (Record<string, unknown> | unknown[])[]
  durability?: BulkLoadDurability
  checkpoint?: boolean
}

export type LoadResponse = BulkLoadResult

export interface AckResponse {
  acked: boolean
  seq: string
}

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
