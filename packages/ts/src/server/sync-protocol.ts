import { decodeTaggedValues } from '../core/cdc/encoding.js'
import { SQLITE_USER_VERSION_MAX } from '../core/migrations/baseline.js'
import { MAX_CHANGE_SEQ } from '../core/sync/change-seqs.js'
import { BatchValidationError } from '../core/sync/errors.js'
import type { ApplyResult, ReplicationBatch, ReplicationChange } from '../core/sync/types.js'
import { SEQ_STRING_RE } from '../core/sync/validators.js'

export const MAX_SYNC_BATCH_CHANGES = 1000

const NODE_ID_RE = /^[0-9a-f]{32}$/
const TABLE_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/
const OPERATIONS = new Set(['insert', 'update', 'delete'])

/**
 * One row change inside a device-sync batch, with each column value in its tagged wire form.
 *
 * @public
 */
export interface SyncChange {
  /** The table of the changed row. */
  table: string
  /** Whether the device inserted, updated, or deleted the row. */
  operation: string
  /** The primary key of the changed row, encoded as a string. */
  rowId: string
  /** The primary key columns of the changed row and their values. */
  primaryKey: Record<string, unknown>
  /** The hybrid logical clock stamp that the device gave this change. */
  hlc: string
  /** The identifier of the device transaction that made this change. */
  txId: string
  /** The 32-character hexadecimal identifier of the device, which equals the batch's `sourceNodeId`. */
  nodeId: string
  /** The row's values after the change, or null for a delete. */
  newData: Record<string, unknown> | null
  /** The row's values before the change, or null for an insert. */
  oldData: Record<string, unknown> | null
  /** The position of this change in the device's change log, as a decimal string. */
  seq?: string
}

/**
 * The batch of row changes that a device pushes, with the range of the device's change log that it covers.
 *
 * @public
 */
export interface SyncBatch {
  /** The 32-character hexadecimal identifier of the device that pushes the batch. */
  sourceNodeId: string
  /** The device's identifier for this batch, of 1 to 128 characters. */
  batchId: string
  /** The first change sequence that the batch covers, as a decimal string. */
  fromSeq: string
  /** The last change sequence that the batch covers, as a decimal string. */
  toSeq: string
  /** The lowest and the highest hybrid logical clock stamp in the batch. */
  hlcRange: {
    /** The lowest stamp in the batch. */
    min: string
    /** The highest stamp in the batch. */
    max: string
  }
  /** The changes, from 1 to 1000 of them, in the order that the device made them. */
  changes: SyncChange[]
  /** The checksum over the batch's changes, which the server recomputes before it applies them. */
  checksum: string
  /** Whether the batch ends part-way through a device transaction that the next batch continues. */
  txContinues?: boolean
}

/**
 * The body of `POST /db/{id}/changes`, the route that applies a device's changes to the server's database.
 *
 * @public
 */
export interface ChangesRequest {
  /** The schema version of the device's database, which the server compares with the schema version of its database before it applies the batch. */
  schemaVersion?: number
  /** The changes to apply. */
  batch: SyncBatch
}

/**
 * How the server applied a device's batch.
 *
 * @public
 */
export interface ChangesResponse {
  /** How many changes the server applied. */
  applied: number
  /** How many changes the server skipped, such as the changes of a batch that it had applied before, an update to a missing row, or a change older than the server's version of the row. */
  skipped: number
  /** How many changes a conflict resolver settled. */
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
  if (change.seq !== undefined && !isStorableSeq(change.seq)) {
    return `changes[${index}].seq must be an integer string from 0 to ${MAX_CHANGE_SEQ}`
  }
  return null
}

function isStorableSeq(candidate: unknown): candidate is string {
  return typeof candidate === 'string' && SEQ_STRING_RE.test(candidate) && BigInt(candidate) <= MAX_CHANGE_SEQ
}

function changeSeqsValidationError(raw: { fromSeq: string; toSeq: string; changes: unknown[] }): string | null {
  const seqs = raw.changes
    .map(change => (isPlainObject(change) ? change.seq : undefined))
    .filter((seq): seq is string => typeof seq === 'string')
    .map(seq => BigInt(seq))
  if (seqs.length === 0) return null
  if (seqs.length !== raw.changes.length) return 'Either every change carries "seq" or none does'
  for (let i = 1; i < seqs.length; i++) {
    if (seqs[i] <= seqs[i - 1]) return 'Each change "seq" must rise strictly'
  }
  if (seqs[0] !== BigInt(raw.fromSeq) || seqs[seqs.length - 1] !== BigInt(raw.toSeq)) {
    return 'The first change "seq" must equal "batch.fromSeq" and the last must equal "batch.toSeq"'
  }
  return null
}

export function syncBatchValidationError(raw: unknown): string | null {
  if (!isPlainObject(raw)) return 'Field "batch" is required and must be an object'
  if (!isValidDeviceId(raw.sourceNodeId)) return '"batch.sourceNodeId" must be a 32-hex node id'
  if (typeof raw.batchId !== 'string' || raw.batchId.length === 0 || raw.batchId.length > 128) {
    return '"batch.batchId" must be a non-empty string of at most 128 characters'
  }
  if (!isStorableSeq(raw.fromSeq)) {
    return `"batch.fromSeq" must be an integer string from 0 to ${MAX_CHANGE_SEQ}`
  }
  if (!isStorableSeq(raw.toSeq)) {
    return `"batch.toSeq" must be an integer string from 0 to ${MAX_CHANGE_SEQ}`
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
  const seqsError = changeSeqsValidationError({ fromSeq: raw.fromSeq, toSeq: raw.toSeq, changes: raw.changes })
  if (seqsError !== null) return seqsError
  if (raw.txContinues !== undefined && typeof raw.txContinues !== 'boolean') {
    return '"batch.txContinues" must be a boolean'
  }
  if (raw.txContinues === true && (raw.changes[0] as { seq?: string }).seq === undefined) {
    return '"batch.txContinues" requires every change to carry "seq"'
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

  const batch: ReplicationBatch = {
    sourceNodeId: body.sourceNodeId,
    batchId: body.batchId,
    fromSeq: BigInt(body.fromSeq),
    toSeq: BigInt(body.toSeq),
    hlcRange: { min: body.hlcRange.min, max: body.hlcRange.max },
    changes,
    checksum: body.checksum,
  }
  if (body.changes[0]?.seq !== undefined) {
    batch.changeSeqs = body.changes.map((change, index) => {
      if (change.seq === undefined) {
        throw new BatchValidationError(
          `changes[${index}] carries no "seq", although the first change of the batch does`,
        )
      }
      return BigInt(change.seq)
    })
  }
  if (body.txContinues === true) {
    batch.txContinues = true
  }
  return batch
}

export function toChangesResponse(result: ApplyResult): ChangesResponse {
  return { applied: result.applied, skipped: result.skipped, conflicts: result.conflicts }
}
