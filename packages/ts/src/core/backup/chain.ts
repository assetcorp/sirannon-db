import { SirannonError } from '../errors.js'
import { isBackupChainBase, isBackupChainChange, isBackupChainHead } from './chain-records.js'
import type { BackupDestination } from './destination.js'

/** The default name that Sirannon stores the list of chains under at a destination.
 * @public
 */
export const DEFAULT_CHAIN_NAME = 'sirannon-backup-chain'

/** The range of write-ahead log frames that one change piece holds.
 * @public
 */
export interface BackupChainPosition {
  /** The checkpoint sequence of the log that holds these frames. SQLite adds one to it each time it restarts the log. */
  logSequence: number
  /** The first salt of that log. Together with `salt2`, it identifies the generation of the log that holds the frames. */
  salt1: number
  /** The second salt of that log. */
  salt2: number
  /** The first frame in the piece, counted from one. */
  firstFrame: number
  /** The last frame in the piece. */
  lastFrame: number
}

/**
 * The full copy at the head of a chain. Sirannon restores from this copy and
 * then applies the change pieces on top of it.
 *
 * @public
 */
export interface BackupChainBase {
  /** The kind of record. */
  kind: 'full'
  /** The chain that this copy begins. */
  chainId: string
  /** The name that Sirannon stores the pieces of this copy under at the destination. */
  name: string
  /** The identifier that Sirannon reports the progress of this copy under. */
  runId: string
  /** The moment, in epoch milliseconds, that the copy finished. A restore to any earlier moment needs an older chain. */
  finishedAt: number
  /** The number of pieces that Sirannon stores the copy in. */
  pieceCount: number
  /** The size of one whole piece, in bytes. */
  pieceBytes: number
  /** The number of bytes that Sirannon stores at the destination. */
  bytesWritten: number
  /** The SHA-256 of the copy, present while fingerprinting is on. */
  fingerprint?: string
}

/**
 * One change piece, which holds the log frames that a database writes between
 * the previous capture and this one.
 *
 * @public
 */
export interface BackupChainChange {
  /** The kind of record. */
  kind: 'change'
  /** The chain that this piece extends, which identifies the full copy that the piece applies on top of. */
  chainId: string
  /** The name that Sirannon stores this piece under at the destination. */
  name: string
  /** The identifier that Sirannon reports the progress of this capture under. */
  runId: string
  /** The position of this piece in its chain, counted from one. */
  sequence: number
  /** The range of log frames that this piece holds. */
  position: BackupChainPosition
  /** The moment, in epoch milliseconds, that Sirannon captured the frames. A restore stops at a capture time such as this one. */
  capturedAt: number
  /** The number of log frames in this piece. */
  frameCount: number
  /** The number of destination pieces that Sirannon stores these frames in. */
  pieceCount: number
  /** The size of one whole piece, in bytes. */
  pieceBytes: number
  /** The number of bytes that Sirannon stores at the destination. */
  bytesWritten: number
  /** Whether the checkpoint after this capture emptied the log. */
  checkpointed: boolean
  /** The SHA-256 of the frames, present while fingerprinting is on. */
  fingerprint?: string
}

/** Either the full copy at the head of a chain or one change piece along it.
 * @public
 */
export type BackupChainRecord = BackupChainBase | BackupChainChange

/** One full copy and every change piece that Sirannon captures after it.
 * @public
 */
export interface BackupChain {
  /** The identifier of this chain. */
  chainId: string
  /** The moment, in epoch milliseconds, that Sirannon started the chain. */
  startedAt: number
  /** The identifier of the previous chain, where this chain replaces one. */
  previousChainId?: string
  /**
   * The full copy that the chain starts from. After someone deletes that
   * record, this field is absent, so no restore can use the chain.
   */
  base?: BackupChainBase
  /** The change pieces, oldest first. */
  changes: BackupChainChange[]
}

/** One entry in the list of chains at a destination.
 * @internal
 */
export interface BackupChainHead {
  chainId: string
  startedAt: number
  previousChainId?: string
}

const HEAD_APPEND_ATTEMPTS = 8

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function destinationError(message: string, err?: unknown): SirannonError {
  if (err instanceof SirannonError) return err
  const detail = err instanceof Error ? `: ${err.message}` : ''
  return new SirannonError(`${message}${detail}`, 'BACKUP_DESTINATION_ERROR')
}

/**
 * Returns the name that Sirannon stores the records of one chain under. Each
 * chain has a name of its own, so deleting an old chain leaves every other
 * chain in place.
 *
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param chainId - The identifier of the chain.
 * @returns The name to read and write the records of that chain under.
 *
 * @public
 */
export function chainLogName(chainName: string, chainId: string): string {
  return `${chainName}.${chainId}`
}

type ClaimPiece = (name: string, index: number, bytes: Uint8Array) => Promise<boolean>

async function storeRecord<T>(
  store: (bytes: Uint8Array) => Promise<T>,
  name: string,
  index: number,
  record: unknown,
): Promise<T> {
  try {
    return await store(encoder.encode(JSON.stringify(record)))
  } catch (err) {
    throw destinationError(`The destination refused record ${index} of '${name}'`, err)
  }
}

function appendRecord(destination: BackupDestination, name: string, index: number, record: unknown): Promise<void> {
  return storeRecord(bytes => destination.writePiece(name, index, bytes), name, index, record)
}

function claimRecord(claim: ClaimPiece, name: string, index: number, record: unknown): Promise<boolean> {
  return storeRecord(bytes => claim(name, index, bytes), name, index, record)
}

async function listRecordIndices(destination: BackupDestination, name: string): Promise<number[]> {
  try {
    return (await destination.listPieces(name)).map(piece => piece.index)
  } catch (err) {
    throw destinationError(`The destination could not list the records of '${name}'`, err)
  }
}

async function readRecord(destination: BackupDestination, name: string, index: number): Promise<unknown> {
  let bytes: Uint8Array
  try {
    bytes = await destination.readPiece(name, index)
  } catch (err) {
    throw destinationError(`The destination could not return record ${index} of '${name}'`, err)
  }
  try {
    return JSON.parse(decoder.decode(bytes))
  } catch (err) {
    throw destinationError(`Record ${index} of '${name}' is not a record Sirannon wrote`, err)
  }
}

async function readRecords(destination: BackupDestination, name: string): Promise<unknown[]> {
  const ordered = (await listRecordIndices(destination, name)).sort((left, right) => left - right)
  const records: unknown[] = []
  for (const index of ordered) {
    records.push(await readRecord(destination, name, index))
  }
  return records
}

function chainRecords(records: readonly unknown[], name: string): BackupChainRecord[] {
  const kept: BackupChainRecord[] = []
  for (const record of records) {
    const kind = (record as { kind?: unknown } | null)?.kind
    if (kind !== 'full' && kind !== 'change') continue
    if (kind === 'full' ? isBackupChainBase(record) : isBackupChainChange(record)) {
      kept.push(record as BackupChainRecord)
      continue
    }
    const label = kind === 'full' ? 'full copy' : 'change piece'
    throw destinationError(
      `A ${label} record of '${name}' is missing fields Sirannon writes into every record it stores, so no restore can use this chain until you put that record back`,
    )
  }
  return kept
}

/**
 * Adds one chain to the list of chains at a destination. Sirannon can then find
 * the chain through that list without being given its identifier, whether for
 * a later backup or for a restore on a machine that has never opened this
 * database.
 *
 * During a failover, two nodes of a replication group can choose the same index
 * at the same moment, in which case the second write replaces the first. Where
 * the destination implements `writePieceIfAbsent`, Sirannon claims each index
 * through it and tries the next index whenever the claim fails. Otherwise
 * Sirannon writes the record and reads it back, which detects the write of the
 * other node unless that node writes between those two calls.
 *
 * @param destination - The destination that holds the list.
 * @param chainName - The name that Sirannon stores the list under.
 * @param head - The chain to add.
 * @returns The index of the chain in the list, counted from zero.
 */
export async function appendChainHead(
  destination: BackupDestination,
  chainName: string,
  head: BackupChainHead,
): Promise<number> {
  const taken = await listRecordIndices(destination, chainName)
  let index = taken.reduce((next, piece) => Math.max(next, piece + 1), 0)

  const claim = destination.writePieceIfAbsent?.bind(destination)

  for (let attempt = 0; attempt < HEAD_APPEND_ATTEMPTS; attempt++) {
    if (claim) {
      if (await claimRecord(claim, chainName, index, head)) return index
      index++
      continue
    }
    await appendRecord(destination, chainName, index, head)
    const stored = await readRecord(destination, chainName, index)
    if (isBackupChainHead(stored) && stored.chainId === head.chainId) return index
    index++
  }

  throw destinationError(
    `Another chain took every one of the ${HEAD_APPEND_ATTEMPTS} places Sirannon tried for chain '${head.chainId}' in '${chainName}'. Point one replication group at this chain name, and give any other group a chainName of its own.`,
  )
}

/**
 * Adds one record to a chain. The full copy takes index zero, and each change
 * piece takes the next index in order.
 *
 * @param destination - The destination that holds the records of the chain.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param record - The record to add.
 * @param index - The index of the record in the chain, counted from zero.
 */
export async function appendChainRecord(
  destination: BackupDestination,
  chainName: string,
  record: BackupChainRecord,
  index: number,
): Promise<void> {
  await appendRecord(destination, chainLogName(chainName, record.chainId), index, record)
}

/**
 * Reads the entry at one index in the list of chains, so that a cycle can
 * check the entry that it claimed without reading the whole list.
 *
 * @param destination - The destination that holds the list.
 * @param chainName - The name that Sirannon stores the list under.
 * @param index - The index to read, counted from zero.
 * @returns The chain at that index, or null where the record at that index is not a valid entry.
 */
export async function readChainHeadAt(
  destination: BackupDestination,
  chainName: string,
  index: number,
): Promise<BackupChainHead | null> {
  const record = await readRecord(destination, chainName, index)
  return isBackupChainHead(record) ? record : null
}

/**
 * Reads the list of chains at a destination.
 *
 * @param destination - The destination that holds the list.
 * @param chainName - The name that Sirannon stores the list under.
 * @returns One entry per chain, newest first.
 */
export async function readChainHeads(destination: BackupDestination, chainName: string): Promise<BackupChainHead[]> {
  const records = await readRecords(destination, chainName)
  return records.filter(isBackupChainHead).reverse()
}

/**
 * Returns every chain at a destination, with its full copy and its change
 * pieces. It reads only the destination, so you can call it on a fresh machine
 * before any database exists.
 *
 * @param destination - The destination that holds the backups and their records.
 * @param chainName - The name that Sirannon stores the list of chains under. Defaults to `sirannon-backup-chain`.
 * @returns The chains, newest first, each with its own records oldest first.
 *
 * @public
 */
export async function readBackupChains(
  destination: BackupDestination,
  chainName: string = DEFAULT_CHAIN_NAME,
): Promise<BackupChain[]> {
  const heads = await readChainHeads(destination, chainName)
  const chains: BackupChain[] = []
  for (const head of heads) {
    const logName = chainLogName(chainName, head.chainId)
    const records = chainRecords(await readRecords(destination, logName), logName)
    const changes = records
      .filter((record): record is BackupChainChange => record.kind === 'change')
      .sort((left, right) => left.sequence - right.sequence)
    const base = records.find((record): record is BackupChainBase => record.kind === 'full')
    chains.push({
      chainId: head.chainId,
      startedAt: head.startedAt,
      ...(head.previousChainId ? { previousChainId: head.previousChainId } : {}),
      ...(base ? { base } : {}),
      changes,
    })
  }
  return chains
}
