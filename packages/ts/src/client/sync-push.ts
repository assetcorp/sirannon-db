import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { ReplicationBatch } from '../core/sync/types.js'
import type { ChangesRequest, ChangesResponse } from '../server/sync-protocol.js'
import { postJson } from './http-json.js'
import { RemoteError } from './types.js'

export function encodeSyncBatch(batch: ReplicationBatch): ChangesRequest['batch'] {
  return {
    sourceNodeId: batch.sourceNodeId,
    batchId: batch.batchId,
    fromSeq: batch.fromSeq.toString(),
    toSeq: batch.toSeq.toString(),
    hlcRange: { min: batch.hlcRange.min, max: batch.hlcRange.max },
    changes: batch.changes.map(change => ({
      table: change.table,
      operation: change.operation,
      rowId: change.rowId,
      primaryKey: encodeTaggedValues(change.primaryKey) as Record<string, unknown>,
      hlc: change.hlc,
      txId: change.txId,
      nodeId: change.nodeId,
      newData: change.newData === null ? null : (encodeTaggedValues(change.newData) as Record<string, unknown>),
      oldData: change.oldData === null ? null : (encodeTaggedValues(change.oldData) as Record<string, unknown>),
    })),
    checksum: batch.checksum,
  }
}

export async function pushSyncBatch(
  baseUrl: string,
  databaseId: string,
  batch: ReplicationBatch,
  headers?: Record<string, string>,
  timeoutMs?: number,
): Promise<ChangesResponse> {
  const url = `${baseUrl}/db/${encodeURIComponent(databaseId)}/changes`
  const body: ChangesRequest = { batch: encodeSyncBatch(batch) }
  const data = await postJson(url, body, headers, timeoutMs)

  const result = data as Partial<ChangesResponse>
  if (
    typeof result.applied !== 'number' ||
    typeof result.skipped !== 'number' ||
    typeof result.conflicts !== 'number'
  ) {
    throw new RemoteError('INVALID_RESPONSE', 'Changes response must carry applied, skipped, and conflicts counts')
  }
  return { applied: result.applied, skipped: result.skipped, conflicts: result.conflicts }
}
