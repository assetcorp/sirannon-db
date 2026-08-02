import { describe, expect, it } from 'vitest'
import { computeChecksum } from '../../core/sync/checksum.js'
import type { ReplicationBatch, ReplicationChange } from '../../core/sync/types.js'
import { decodeSyncBatch, syncBatchValidationError } from '../../server/sync-protocol.js'
import { encodeSyncBatch } from '../sync-push.js'

const DEVICE = 'dddd0000dddd0000dddd0000dddd0000'

function deviceBatch(): ReplicationBatch {
  const changes: ReplicationChange[] = [
    {
      table: 'ledger',
      operation: 'insert',
      rowId: '1',
      primaryKey: { id: 1 },
      hlc: '0000018f-0001-dddd0000dddd0000dddd0000dddd0000',
      txId: 'tx-1',
      nodeId: DEVICE,
      newData: { id: 1, amount: 9007199254740993n, blob: new Uint8Array([1, 2, 3]) },
      oldData: null,
    },
    {
      table: 'ledger',
      operation: 'delete',
      rowId: '2',
      primaryKey: { id: 2 },
      hlc: '0000018f-0002-dddd0000dddd0000dddd0000dddd0000',
      txId: 'tx-2',
      nodeId: DEVICE,
      newData: null,
      oldData: { id: 2, amount: 5 },
    },
  ]
  return {
    sourceNodeId: DEVICE,
    batchId: `${DEVICE}-3-4`,
    fromSeq: 3n,
    toSeq: 4n,
    hlcRange: { min: changes[0].hlc, max: changes[1].hlc },
    changes,
    checksum: computeChecksum(changes),
  }
}

describe('encodeSyncBatch', () => {
  it('produces a wire batch the server validator accepts', () => {
    expect(syncBatchValidationError(encodeSyncBatch(deviceBatch()))).toBeNull()
  })

  it('round-trips through the server decode including tagged values', () => {
    const batch = deviceBatch()
    const decoded = decodeSyncBatch(encodeSyncBatch(batch))
    expect(decoded.fromSeq).toBe(3n)
    expect(decoded.toSeq).toBe(4n)
    expect(decoded.checksum).toBe(batch.checksum)
    expect(decoded.changes[0].newData?.amount).toBe(9007199254740993n)
    const blob = decoded.changes[0].newData?.blob
    expect(blob).toBeInstanceOf(Uint8Array)
    expect(Array.from(blob as Uint8Array)).toEqual([1, 2, 3])
    expect(decoded.changes[1].oldData).toEqual({ id: 2, amount: 5 })
    expect(computeChecksum(decoded.changes)).toBe(batch.checksum)
  })
})
