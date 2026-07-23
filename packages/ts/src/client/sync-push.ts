import { encodeTaggedValues } from '../core/cdc/encoding.js'
import type { ReplicationBatch } from '../core/sync/types.js'
import type { ErrorResponse } from '../server/protocol.js'
import type { ChangesRequest, ChangesResponse } from '../server/sync-protocol.js'
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
): Promise<ChangesResponse> {
  const url = `${baseUrl}/db/${encodeURIComponent(databaseId)}/changes`
  const body: ChangesRequest = { batch: encodeSyncBatch(batch) }

  let response: Response
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify(body),
    })
  } catch (err) {
    throw new RemoteError(
      'CONNECTION_ERROR',
      `Failed to connect to ${url}: ${err instanceof Error ? err.message : String(err)}`,
    )
  }

  let data: unknown
  try {
    data = await response.json()
  } catch {
    throw new RemoteError('INVALID_RESPONSE', `Server returned non-JSON response (HTTP ${response.status})`)
  }

  if (!response.ok) {
    const errorData = data as ErrorResponse
    throw new RemoteError(
      errorData.error?.code ?? 'UNKNOWN_ERROR',
      errorData.error?.message ?? `HTTP ${response.status}`,
    )
  }

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
