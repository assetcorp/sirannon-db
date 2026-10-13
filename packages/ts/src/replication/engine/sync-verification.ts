import { createHash } from 'node:crypto'
import type { SyncComplete, SyncTableManifest } from '../types.js'
import { IDENTIFIER_RE } from './constants.js'
import type { ReplicationEngine } from './engine.js'

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

/**
 * Checks a finished first copy against the manifests that its source sent, so that the joiner keeps the copy only when every table it received has a manifest and every manifest matches.
 *
 * @param engine - The joining engine, which holds the digest of each table that it received in this copy.
 * @param complete - The completion message that closes the copy.
 * @returns `true` when every received table has a manifest and every manifest matches by chained digest, or by key hash when it carries no digest.
 */
export async function copyMatchesManifests(engine: ReplicationEngine, complete: SyncComplete): Promise<boolean> {
  const covered = new Set(complete.manifests.map(manifest => manifest.table))
  for (const table of engine.syncTableDigests.keys()) {
    if (!covered.has(table)) return false
  }
  for (const manifest of complete.manifests) {
    const valid =
      manifest.batchDigest !== undefined
        ? matchesStreamDigest(manifest, engine.syncTableDigests.get(manifest.table))
        : await engine.log.verifyManifest(manifest)
    if (!valid) return false
  }
  return true
}

/**
 * Lists the tables that a first copy wrote to, which the joiner wipes when the copy fails verification.
 *
 * @param engine - The joining engine, which holds the digest of each table that it received in this copy.
 * @param complete - The completion message that closes the copy.
 * @returns Every table that the copy received or that a manifest names, keeping only valid identifiers.
 */
export function copiedTables(engine: ReplicationEngine, complete: SyncComplete): string[] {
  const tables = new Set([...engine.syncTableDigests.keys(), ...complete.manifests.map(manifest => manifest.table)])
  return [...tables].filter(table => IDENTIFIER_RE.test(table))
}
