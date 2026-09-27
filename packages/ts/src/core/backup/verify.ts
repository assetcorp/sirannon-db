import { SirannonError } from '../errors.js'
import type { BackupChain, BackupChainRecord } from './chain.js'
import type { BackupDestination } from './destination.js'
import { fetchStoredFile, listStoredFilePieces } from './restore-fetch.js'

/** The result of reading one stored backup from a destination and checking it against its record.
 * @public
 */
export interface BackupVerifyResult {
  /** The name that Sirannon stores the pieces under. */
  name: string
  /** The chain that holds this record. */
  chainId: string
  /** Whether this is the full copy at the head of that chain or one change piece along it. */
  kind: 'full' | 'change'
  /** The number of pieces at the destination. */
  pieceCount: number
  /** The total size of those pieces, in bytes. */
  bytesRead: number
  /** The SHA-256 that Sirannon computes over the bytes that it reads, present where the record holds a fingerprint to compare against. */
  fingerprint?: string
}

function findRecord(chains: readonly BackupChain[], name: string): BackupChainRecord | undefined {
  for (const chain of chains) {
    if (chain.base?.name === name) return chain.base
    const change = chain.changes.find(piece => piece.name === name)
    if (change) return change
  }
  return undefined
}

/**
 * Reads one backup from the destination and checks it against its record in
 * the chain.
 *
 * A restore finds a damaged piece only after it has begun, so call this
 * beforehand to find the damage early. Sirannon fetches every piece in order
 * and computes a SHA-256 over the bytes as it reads them, then compares that
 * fingerprint and the byte count against the record. Sirannon holds one piece
 * in memory at a time and writes nothing to disk, so a check of a large full
 * copy needs no local storage.
 *
 * A missing piece, a byte count that differs from the record, and a
 * fingerprint that differs from the record each throw
 * `BACKUP_DESTINATION_ERROR`. Where fingerprinting was off for the backup,
 * Sirannon compares only the piece listing and the byte count, and the result
 * has no fingerprint. A name that no chain records throws
 * `BACKUP_CHAIN_BROKEN`.
 *
 * @param destination - The destination that holds the pieces.
 * @param chains - The chains at that destination, as {@link readBackupChains} returns them.
 * @param name - The name that Sirannon stores the backup under, which its chain record states.
 * @returns The number of pieces read, their total size in bytes, and the fingerprint where the record holds one.
 *
 * @public
 */
export async function verifyBackupRecord(
  destination: BackupDestination,
  chains: readonly BackupChain[],
  name: string,
): Promise<BackupVerifyResult> {
  const record = findRecord(chains, name)
  if (!record) {
    throw new SirannonError(
      `No backup named '${name}' is recorded in any chain at this destination`,
      'BACKUP_CHAIN_BROKEN',
    )
  }

  const file = {
    name: record.name,
    pieceCount: record.pieceCount,
    pieceBytes: record.pieceBytes,
    bytesWritten: record.bytesWritten,
    ...(record.fingerprint === undefined ? {} : { fingerprint: record.fingerprint }),
  }
  const pieces = await listStoredFilePieces(destination, file)
  const fetched = await fetchStoredFile(destination, file, pieces, async () => {})

  return {
    name: record.name,
    chainId: record.chainId,
    kind: record.kind,
    pieceCount: fetched.pieceCount,
    bytesRead: fetched.bytesFetched,
    ...(fetched.fingerprint === undefined ? {} : { fingerprint: fetched.fingerprint }),
  }
}
