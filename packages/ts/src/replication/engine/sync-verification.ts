import { createHash } from 'node:crypto'
import type { SyncTableManifest } from '../types.js'

export interface TableStreamDigest {
  rowCount: number
  digest: string
}

export function advanceStreamDigest(
  previous: TableStreamDigest | undefined,
  batchChecksum: string,
  rowsInBatch: number,
): TableStreamDigest {
  const digest = createHash('sha256')
    .update(previous?.digest ?? '')
    .update(batchChecksum)
    .digest('hex')
  return { rowCount: (previous?.rowCount ?? 0) + rowsInBatch, digest }
}

export function matchesStreamDigest(manifest: SyncTableManifest, local: TableStreamDigest | undefined): boolean {
  if (manifest.batchDigest === undefined || local === undefined) return false
  return local.rowCount === manifest.rowCount && local.digest === manifest.batchDigest
}
