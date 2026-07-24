import { decodeTaggedValues } from '../core/cdc/encoding.js'
import { SQLITE_USER_VERSION_MAX } from '../core/migrations/baseline.js'
import type { ApplyResult, ReplicationBatch, ReplicationChange } from '../core/sync/types.js'
import { SEQ_STRING_RE } from '../core/sync/validators.js'

export const MAX_SYNC_BATCH_CHANGES = 1000

const NODE_ID_RE = /^[0-9a-f]{32}$/
const TABLE_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/
const OPERATIONS = new Set(['insert', 'update', 'delete'])

export interface ChangesRequest {
  schemaVersion?: number
  batch: {
    sourceNodeId: string
    batchId: string
    fromSeq: string
    toSeq: string
    hlcRange: { min: string; max: string }
    changes: {
      table: string
      operation: string
      rowId: string
      primaryKey: Record<string, unknown>
      hlc: string
      txId: string
      nodeId: string
      newData: Record<string, unknown> | null
      oldData: Record<string, unknown> | null
    }[]
    checksum: string
  }
}

export interface ChangesResponse {
  applied: number
  skipped: number
  conflicts: number
}

export function isValidDeviceId(candidate: unknown): candidate is string {
  return typeof candidate === 'string' && NODE_ID_RE.test(candidate)
}

export function isValidSchemaVersion(candidate: unknown): candidate is number {
  return (
    typeof candidate === 'number' &&
    Number.isSafeInteger(candidate) &&
    candidate >= 0 &&
    candidate <= SQLITE_USER_VERSION_MAX
  )
}

export function schemaVersionValidationError(candidate: unknown): string | null {
  if (candidate === undefined) return null
  if (!isValidSchemaVersion(candidate)) {
    return `"schemaVersion" must be an integer from 0 to ${SQLITE_USER_VERSION_MAX}`
  }
  return null
}

export interface SchemaVersionGateRefusal {
  code: 'MIGRATION_REQUIRED' | 'SCHEMA_AHEAD'
  message: string
}

export function schemaVersionGateRefusal(
  deviceVersion: number,
  serverVersion: number,
): SchemaVersionGateRefusal | null {
  if (deviceVersion < serverVersion) {
    return {
      code: 'MIGRATION_REQUIRED',
      message: `Device schema version ${deviceVersion} is behind server version ${serverVersion}; apply the missing migrations before syncing`,
    }
  }
  if (deviceVersion > serverVersion) {
    return {
      code: 'SCHEMA_AHEAD',
      message: `Device schema version ${deviceVersion} is ahead of server version ${serverVersion}; the server must be migrated before this device can sync`,
    }
  }
  return null
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function changeValidationError(change: unknown, index: number): string | null {
  if (!isPlainObject(change)) return `changes[${index}] must be an object`
  if (typeof change.table !== 'string' || !TABLE_RE.test(change.table)) {
    return `changes[${index}].table must be a valid table name`
  }
  if (typeof change.operation !== 'string' || !OPERATIONS.has(change.operation)) {
    return `changes[${index}].operation must be one of insert, update, delete`
  }
  if (typeof change.rowId !== 'string' || change.rowId.length === 0) {
    return `changes[${index}].rowId must be a non-empty string`
  }
  if (!isPlainObject(change.primaryKey)) return `changes[${index}].primaryKey must be an object`
  if (typeof change.hlc !== 'string' || change.hlc.length === 0) {
    return `changes[${index}].hlc must be a non-empty string`
  }
  if (typeof change.txId !== 'string' || change.txId.length === 0) {
    return `changes[${index}].txId must be a non-empty string`
  }
  if (!isValidDeviceId(change.nodeId)) return `changes[${index}].nodeId must be a 32-hex node id`
  if (change.newData !== null && !isPlainObject(change.newData)) {
    return `changes[${index}].newData must be an object or null`
  }
  if (change.oldData !== null && !isPlainObject(change.oldData)) {
    return `changes[${index}].oldData must be an object or null`
  }
  return null
}

export function syncBatchValidationError(raw: unknown): string | null {
  if (!isPlainObject(raw)) return 'Field "batch" is required and must be an object'
  if (!isValidDeviceId(raw.sourceNodeId)) return '"batch.sourceNodeId" must be a 32-hex node id'
  if (typeof raw.batchId !== 'string' || raw.batchId.length === 0 || raw.batchId.length > 128) {
    return '"batch.batchId" must be a non-empty string of at most 128 characters'
  }
  if (typeof raw.fromSeq !== 'string' || !SEQ_STRING_RE.test(raw.fromSeq)) {
    return '"batch.fromSeq" must be a positive integer string'
  }
  if (typeof raw.toSeq !== 'string' || !SEQ_STRING_RE.test(raw.toSeq)) {
    return '"batch.toSeq" must be a positive integer string'
  }
  if (BigInt(raw.fromSeq) > BigInt(raw.toSeq)) return '"batch.fromSeq" must not exceed "batch.toSeq"'
  if (!isPlainObject(raw.hlcRange) || typeof raw.hlcRange.min !== 'string' || typeof raw.hlcRange.max !== 'string') {
    return '"batch.hlcRange" must carry string "min" and "max" fields'
  }
  if (!Array.isArray(raw.changes) || raw.changes.length === 0) {
    return '"batch.changes" must be a non-empty array'
  }
  if (raw.changes.length > MAX_SYNC_BATCH_CHANGES) {
    return `"batch.changes" must not exceed ${MAX_SYNC_BATCH_CHANGES} changes`
  }
  if (typeof raw.checksum !== 'string' || raw.checksum.length === 0) {
    return '"batch.checksum" must be a non-empty string'
  }
  for (let i = 0; i < raw.changes.length; i++) {
    const changeError = changeValidationError(raw.changes[i], i)
    if (changeError !== null) return changeError
    const change = raw.changes[i] as { nodeId: string }
    if (change.nodeId !== raw.sourceNodeId) {
      return `changes[${i}].nodeId must match "batch.sourceNodeId"`
    }
  }
  return null
}

export function decodeSyncBatch(body: ChangesRequest['batch']): ReplicationBatch {
  const changes: ReplicationChange[] = body.changes.map(change => ({
    table: change.table,
    operation: change.operation as ReplicationChange['operation'],
    rowId: change.rowId,
    primaryKey: decodeTaggedValues(change.primaryKey) as Record<string, unknown>,
    hlc: change.hlc,
    txId: change.txId,
    nodeId: change.nodeId,
    newData: change.newData === null ? null : (decodeTaggedValues(change.newData) as Record<string, unknown>),
    oldData: change.oldData === null ? null : (decodeTaggedValues(change.oldData) as Record<string, unknown>),
  }))

  return {
    sourceNodeId: body.sourceNodeId,
    batchId: body.batchId,
    fromSeq: BigInt(body.fromSeq),
    toSeq: BigInt(body.toSeq),
    hlcRange: { min: body.hlcRange.min, max: body.hlcRange.max },
    changes,
    checksum: body.checksum,
  }
}

export function toChangesResponse(result: ApplyResult): ChangesResponse {
  return { applied: result.applied, skipped: result.skipped, conflicts: result.conflicts }
}
