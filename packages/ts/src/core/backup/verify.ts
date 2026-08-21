import { SirannonError } from '../errors.js'
import type { BackupChain, BackupChainRecord } from './chain.js'
import type { BackupDestination } from './destination.js'
import { fetchStoredFile, listStoredFilePieces } from './restore-fetch.js'

/** What Sirannon found when it read one stored backup back out of a destination.
 * @public
 */
export interface BackupVerifyResult {
  /** Name the pieces are stored under. */
  name: string
  /** The chain this record belongs to. */
  chainId: string
  /** Whether this is the full copy at the head of that chain or one change piece along it. */
  kind: 'full' | 'change'
  /** How many pieces the destination stored. */
  pieceCount: number
  /** How many bytes those pieces add up to. */
  bytesRead: number
  /** The SHA-256 Sirannon computed over what it read, where the backup recorded one to compare it against. */
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
 * Reads one backup back out of the destination and compares it against the
 * record the backup that wrote it left behind.
 *
 * A restore would fail on a damaged piece only once that restore had already
 * begun, so an operator calls this beforehand. Sirannon fetches every piece in
 * order and folds a SHA-256 over the bytes as they arrive, then compares that
 * digest and the byte count against the record. Only one piece is in memory at
 * any moment, and Sirannon writes none of them to disk, so a check over a large
 * full copy needs no local storage of its own.
 *
 * A missing piece, a byte count that differs from the recorded one, and a
 * digest that differs from the recorded one will each fail with
 * `BACKUP_DESTINATION_ERROR`. Where the backup turned fingerprinting off, the
 * piece listing and the byte count are the whole comparison, and the result
 * reports no fingerprint.
 *
 * @param destination - Where the pieces are stored.
 * @param chains - The chains that destination stores, as `readBackupChains` returns them.
 * @param name - Name the backup is stored under, which every chain record states.
 * @returns The pieces read, the bytes they add up to, and the digest where the backup recorded one.
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
