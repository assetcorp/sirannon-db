import type {
  ForwardedTransaction,
  ReplicationAck,
  ReplicationBatch,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
} from '../replication/types.js'

export function isValidBatch(batch: unknown): batch is ReplicationBatch {
  if (typeof batch !== 'object' || batch === null) return false
  const b = batch as Record<string, unknown>
  return (
    typeof b.sourceNodeId === 'string' &&
    typeof b.batchId === 'string' &&
    typeof b.fromSeq === 'bigint' &&
    typeof b.toSeq === 'bigint' &&
    typeof b.checksum === 'string' &&
    Array.isArray(b.changes) &&
    typeof b.hlcRange === 'object' &&
    b.hlcRange !== null &&
    optionalString(b.groupId) &&
    optionalBigint(b.primaryTerm)
  )
}

export function isValidAck(ack: unknown): ack is ReplicationAck {
  if (typeof ack !== 'object' || ack === null) return false
  const a = ack as Record<string, unknown>
  return (
    typeof a.batchId === 'string' &&
    typeof a.ackedSeq === 'bigint' &&
    typeof a.nodeId === 'string' &&
    optionalString(a.groupId) &&
    optionalBigint(a.primaryTerm)
  )
}

export function isValidForwardedTransaction(req: unknown): req is ForwardedTransaction {
  if (typeof req !== 'object' || req === null) return false
  const r = req as Record<string, unknown>
  return (
    typeof r.requestId === 'string' &&
    Array.isArray(r.statements) &&
    optionalString(r.groupId) &&
    optionalBigint(r.primaryTerm)
  )
}

export function isValidSyncRequest(req: unknown): req is SyncRequest {
  if (typeof req !== 'object' || req === null) return false
  const r = req as Record<string, unknown>
  return (
    typeof r.requestId === 'string' &&
    typeof r.joinerNodeId === 'string' &&
    Array.isArray(r.completedTables) &&
    optionalString(r.groupId) &&
    optionalBigint(r.primaryTerm)
  )
}

export function isValidSyncBatch(batch: unknown): batch is SyncBatch {
  if (typeof batch !== 'object' || batch === null) return false
  const b = batch as Record<string, unknown>
  return (
    typeof b.requestId === 'string' &&
    typeof b.table === 'string' &&
    typeof b.batchIndex === 'number' &&
    Array.isArray(b.rows) &&
    typeof b.checksum === 'string' &&
    typeof b.isLastBatchForTable === 'boolean' &&
    optionalString(b.groupId) &&
    optionalBigint(b.primaryTerm)
  )
}

export function isValidSyncComplete(complete: unknown): complete is SyncComplete {
  if (typeof complete !== 'object' || complete === null) return false
  const c = complete as Record<string, unknown>
  return (
    typeof c.requestId === 'string' &&
    typeof c.snapshotSeq === 'bigint' &&
    Array.isArray(c.manifests) &&
    optionalString(c.groupId) &&
    optionalBigint(c.primaryTerm)
  )
}

export function isValidSyncAck(ack: unknown): ack is SyncAck {
  if (typeof ack !== 'object' || ack === null) return false
  const a = ack as Record<string, unknown>
  return (
    typeof a.requestId === 'string' &&
    typeof a.joinerNodeId === 'string' &&
    typeof a.table === 'string' &&
    typeof a.batchIndex === 'number' &&
    typeof a.success === 'boolean' &&
    optionalString(a.groupId) &&
    optionalBigint(a.primaryTerm)
  )
}

function optionalString(value: unknown): boolean {
  return value === undefined || typeof value === 'string'
}

function optionalBigint(value: unknown): boolean {
  return value === undefined || typeof value === 'bigint'
}
