import { createHash } from 'node:crypto'
import type { SyncComplete, SyncTableManifest } from '../types.js'
import { IDENTIFIER_RE } from './constants.js'
import type { ReplicationEngine } from './engine.js'

/**
 * Holds the running digest of one table's copy batches, which the joiner compares with the source's manifest.
 *
 * @internal
 */
export interface TableStreamDigest {
  /** Holds how many rows the batches carried. */
  rowCount: number
  /** Holds the SHA-256 chain over the batch checksums, in hex. */
  digest: string
}

/**
 * Returns the table's digest after one more copy batch, which chains the batch checksum onto the previous digest.
 *
 * @param previous - The digest before the batch, or undefined for the table's first batch.
 * @param batchChecksum - The checksum that the batch carries.
 * @param rowsInBatch - How many rows the batch holds.
 * @returns The new digest and row count.
 *
 * @internal
 */
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

/**
 * Returns true when the joiner's digest of a table equals the digest and row count in the source's manifest.
 *
 * @param manifest - The source's manifest for the table.
 * @param local - The joiner's digest, or undefined when it received no batch for the table.
 * @returns False when either side holds no digest.
 *
 * @internal
 */
export function matchesStreamDigest(manifest: SyncTableManifest, local: TableStreamDigest | undefined): boolean {
  if (manifest.batchDigest === undefined || local === undefined) return false
  return local.rowCount === manifest.rowCount && local.digest === manifest.batchDigest
}

/**
 * Returns true when a finished first copy matches the manifests that its source sent, so that the joiner keeps the
 * copy only when every table that it received has a manifest and every manifest matches.
 *
 * @param engine - The joining engine, which holds the digest of each table that it received in this copy.
 * @param complete - The completion message that closes the copy.
 * @returns True when every received table has a manifest and every manifest matches by chained digest, or by key hash
 * when it carries no digest.
 *
 * @internal
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
 * Returns the tables that a first copy wrote to, which the joiner wipes when the copy fails verification.
 *
 * @param engine - The joining engine, which holds the digest of each table that it received in this copy.
 * @param complete - The completion message that closes the copy.
 * @returns Every table that the copy received or that a manifest names, keeping only valid identifiers.
 *
 * @internal
 */
export function copiedTables(engine: ReplicationEngine, complete: SyncComplete): string[] {
  const tables = new Set([...engine.syncTableDigests.keys(), ...complete.manifests.map(manifest => manifest.table)])
  return [...tables].filter(table => IDENTIFIER_RE.test(table))
}
