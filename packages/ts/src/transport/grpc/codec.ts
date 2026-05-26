import type {
  ReplicationChange as AppReplicationChange,
  ForwardedTransaction,
  ReplicationAck,
  ReplicationBatch,
  SyncAck,
  SyncBatch,
  SyncComplete,
  SyncRequest,
  SyncTableManifest,
} from '../../replication/types.js'
import type {
  AckPayload,
  BatchPayload,
  ColumnValue,
  ForwardRequest as ProtoForwardRequest,
  ReplicationChange as ProtoReplicationChange,
  SyncTableManifest as ProtoSyncTableManifest,
  RowData,
  Statement,
  SyncAckPayload,
  SyncBatchPayload,
  SyncCompletePayload,
  SyncRequestPayload,
} from './generated/replication.js'

export function toColumnValue(value: unknown): ColumnValue {
  if (value === null || value === undefined) {
    return { nullValue: true }
  }
  if (typeof value === 'string') {
    return { stringValue: value }
  }
  if (typeof value === 'bigint') {
    return { intValue: value }
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return { intValue: BigInt(value) }
    }
    return { floatValue: value }
  }
  if (typeof value === 'boolean') {
    return { boolValue: value }
  }
  if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
    return { blobValue: Buffer.from(value) }
  }
  throw new TypeError(
    `Unsupported value type for ColumnValue conversion: ${typeof value} (${Object.prototype.toString.call(value)})`,
  )
}

export function fromColumnValue(cv: ColumnValue): unknown {
  if (cv.nullValue !== undefined) return null
  if (cv.stringValue !== undefined) return cv.stringValue
  if (cv.intValue !== undefined) return cv.intValue
  if (cv.floatValue !== undefined) return cv.floatValue
  if (cv.blobValue !== undefined) return cv.blobValue
  if (cv.boolValue !== undefined) return cv.boolValue
  return null
}

export function toRowData(record: Record<string, unknown> | null): RowData | undefined {
  if (record === null || record === undefined) return undefined
  const fields: Record<string, ColumnValue> = {}
  for (const [key, value] of Object.entries(record)) {
    fields[key] = toColumnValue(value)
  }
  return { fields }
}

export function fromRowData(rowData: RowData | undefined): Record<string, unknown> | null {
  if (rowData === undefined) return null
  const result: Record<string, unknown> = {}
  for (const [key, cv] of Object.entries(rowData.fields)) {
    result[key] = fromColumnValue(cv)
  }
  return result
}

export function toProtoChange(change: AppReplicationChange): ProtoReplicationChange {
  return {
    table: change.table,
    operation: change.operation,
    rowId: change.rowId,
    primaryKey: toRowData(change.primaryKey as Record<string, unknown>),
    hlc: change.hlc,
    txId: change.txId,
    nodeId: change.nodeId,
    newData: toRowData(change.newData),
    oldData: toRowData(change.oldData),
    ddlStatement: change.ddlStatement ?? '',
  }
}

export function fromProtoChange(proto: ProtoReplicationChange): AppReplicationChange {
  return {
    table: proto.table,
    operation: proto.operation as AppReplicationChange['operation'],
    rowId: proto.rowId,
    primaryKey: (fromRowData(proto.primaryKey) ?? {}) as Record<string, unknown>,
    hlc: proto.hlc,
    txId: proto.txId,
    nodeId: proto.nodeId,
    newData: fromRowData(proto.newData),
    oldData: fromRowData(proto.oldData),
    ddlStatement: proto.ddlStatement || undefined,
  }
}

export function toBatchPayload(batch: ReplicationBatch): BatchPayload {
  return {
    sourceNodeId: batch.sourceNodeId,
    batchId: batch.batchId,
    fromSeq: batch.fromSeq,
    toSeq: batch.toSeq,
    hlcRange: { min: batch.hlcRange.min, max: batch.hlcRange.max },
    changes: batch.changes.map(toProtoChange),
    checksum: batch.checksum,
  }
}

export function fromBatchPayload(payload: BatchPayload): ReplicationBatch {
  return {
    sourceNodeId: payload.sourceNodeId,
    batchId: payload.batchId,
    fromSeq: payload.fromSeq,
    toSeq: payload.toSeq,
    hlcRange: {
      min: payload.hlcRange?.min ?? '',
      max: payload.hlcRange?.max ?? '',
    },
    changes: payload.changes.map(fromProtoChange),
    checksum: payload.checksum,
  }
}

export function toAckPayload(ack: ReplicationAck): AckPayload {
  return {
    batchId: ack.batchId,
    ackedSeq: ack.ackedSeq,
    nodeId: ack.nodeId,
  }
}

export function fromAckPayload(payload: AckPayload): ReplicationAck {
  return {
    batchId: payload.batchId,
    ackedSeq: payload.ackedSeq,
    nodeId: payload.nodeId,
  }
}

export function toSyncRequestPayload(req: SyncRequest): SyncRequestPayload {
  return {
    requestId: req.requestId,
    joinerNodeId: req.joinerNodeId,
    completedTables: req.completedTables,
  }
}

export function fromSyncRequestPayload(p: SyncRequestPayload): SyncRequest {
  return {
    requestId: p.requestId,
    joinerNodeId: p.joinerNodeId,
    completedTables: p.completedTables,
  }
}

export function toSyncBatchPayload(batch: SyncBatch): SyncBatchPayload {
  return {
    requestId: batch.requestId,
    table: batch.table,
    batchIndex: batch.batchIndex,
    rows: batch.rows.map(row => toRowData(row) ?? { fields: {} }),
    schema: batch.schema ?? [],
    checksum: batch.checksum,
    isLastBatchForTable: batch.isLastBatchForTable,
  }
}

export function fromSyncBatchPayload(p: SyncBatchPayload): SyncBatch {
  return {
    requestId: p.requestId,
    table: p.table,
    batchIndex: p.batchIndex,
    rows: p.rows.map(r => (fromRowData(r) ?? {}) as Record<string, unknown>),
    schema: p.schema.length > 0 ? p.schema : undefined,
    checksum: p.checksum,
    isLastBatchForTable: p.isLastBatchForTable,
  }
}

export function toSyncCompletePayload(complete: SyncComplete): SyncCompletePayload {
  return {
    requestId: complete.requestId,
    snapshotSeq: complete.snapshotSeq,
    manifests: complete.manifests.map(m => ({
      table: m.table,
      rowCount: m.rowCount,
      pkHash: m.pkHash,
    })),
  }
}

export function fromSyncCompletePayload(p: SyncCompletePayload): SyncComplete {
  return {
    requestId: p.requestId,
    snapshotSeq: p.snapshotSeq,
    manifests: p.manifests.map(
      (m: ProtoSyncTableManifest): SyncTableManifest => ({
        table: m.table,
        rowCount: m.rowCount,
        pkHash: m.pkHash,
      }),
    ),
  }
}

export function toSyncAckPayload(ack: SyncAck): SyncAckPayload {
  return {
    requestId: ack.requestId,
    joinerNodeId: ack.joinerNodeId,
    table: ack.table,
    batchIndex: ack.batchIndex,
    success: ack.success,
    error: ack.error ?? '',
  }
}

export function fromSyncAckPayload(p: SyncAckPayload): SyncAck {
  return {
    requestId: p.requestId,
    joinerNodeId: p.joinerNodeId,
    table: p.table,
    batchIndex: p.batchIndex,
    success: p.success,
    error: p.error || undefined,
  }
}

export function toForwardRequest(req: ForwardedTransaction): ProtoForwardRequest {
  const statements: Statement[] = req.statements.map(s => {
    const namedParams: Record<string, ColumnValue> = {}
    const positionalParams: ColumnValue[] = []
    if (s.params !== undefined && s.params !== null) {
      if (Array.isArray(s.params)) {
        for (const v of s.params) {
          positionalParams.push(toColumnValue(v))
        }
      } else {
        for (const [k, v] of Object.entries(s.params as Record<string, unknown>)) {
          namedParams[k] = toColumnValue(v)
        }
      }
    }
    return { sql: s.sql, namedParams, positionalParams }
  })
  return { requestId: req.requestId, statements }
}

export function fromForwardRequest(proto: ProtoForwardRequest): ForwardedTransaction {
  const statements = proto.statements.map(s => {
    const hasNamed = Object.keys(s.namedParams).length > 0
    const hasPositional = s.positionalParams.length > 0
    let params: Record<string, unknown> | unknown[] | undefined
    if (hasNamed) {
      const named: Record<string, unknown> = {}
      for (const [k, cv] of Object.entries(s.namedParams)) {
        named[k] = fromColumnValue(cv)
      }
      params = named
    } else if (hasPositional) {
      params = s.positionalParams.map(fromColumnValue)
    }
    return { sql: s.sql, params }
  })
  return { requestId: proto.requestId, statements }
}
