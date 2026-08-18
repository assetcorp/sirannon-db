import { SirannonError } from '../errors.js'
import type { BackupDestination } from './destination.js'

/** Name a destination stores the list of chains under, unless you set another.
 * @public
 */
export const DEFAULT_CHAIN_NAME = 'sirannon-backup-chain'

/** The stretch of a database's write-ahead log that one change piece covers.
 * @public
 */
export interface BackupChainPosition {
  /** Checkpoint sequence of the log these frames came from. SQLite adds one to it at every restart of the log. */
  logSequence: number
  /** First salt of that log. The two salts together tell one run of the log from the next. */
  salt1: number
  /** Second salt of it. */
  salt2: number
  /** First frame in the piece, counted from one. */
  firstFrame: number
  /** Last frame in it. */
  lastFrame: number
}

/**
 * The full copy at the head of a chain. A restore starts from this copy and
 * replays the change pieces on top of it.
 *
 * @public
 */
export interface BackupChainBase {
  /** Which kind of record this is. */
  kind: 'full'
  /** The chain this copy begins. */
  chainId: string
  /** Name its pieces are stored under at the destination. */
  name: string
  /** Identifier the run reported its progress under. */
  runId: string
  /** Epoch milliseconds the copy finished. Any earlier moment needs an older chain. */
  finishedAt: number
  /** How many pieces the copy came to. */
  pieceCount: number
  /** Size of one whole piece, in bytes. */
  pieceBytes: number
  /** How many bytes reached the destination. */
  bytesWritten: number
  /** SHA-256 of the copy, unless fingerprinting was turned off. */
  fingerprint?: string
}

/**
 * One capture: the log frames a database wrote between the piece before this
 * one and this one.
 *
 * @public
 */
export interface BackupChainChange {
  /** Which kind of record this is. */
  kind: 'change'
  /** The chain this piece extends, which names the full copy underneath it. */
  chainId: string
  /** Name it is stored under at the destination. */
  name: string
  /** Identifier the capture reported its progress under. */
  runId: string
  /** Where this piece comes in its chain, counted from one. */
  sequence: number
  /** The stretch of log it covers. */
  position: BackupChainPosition
  /** Epoch milliseconds the capture took the frames. A restore aims at moments like this one. */
  capturedAt: number
  /** How many frames it holds. */
  frameCount: number
  /** How many pieces the transfer came to. */
  pieceCount: number
  /** Size of one whole piece, in bytes. */
  pieceBytes: number
  /** How many bytes reached the destination. */
  bytesWritten: number
  /** Whether the checkpoint after this capture emptied the log. */
  checkpointed: boolean
  /** SHA-256 of the frames, unless fingerprinting was turned off. */
  fingerprint?: string
}

/** Either the full copy at the head of a chain or one change piece along it.
 * @public
 */
export type BackupChainRecord = BackupChainBase | BackupChainChange

/** One full copy and every change piece taken from it.
 * @public
 */
export interface BackupChain {
  /** Identifier of this chain. */
  chainId: string
  /** Epoch milliseconds it started. */
  startedAt: number
  /** The chain this one replaced, where it replaced one. */
  previousChainId?: string
  /**
   * The full copy underneath. Once someone deletes that record the field is
   * absent, and no restore can use the chain any more.
   */
  base?: BackupChainBase
  /** The change pieces, oldest first. */
  changes: BackupChainChange[]
}

/** One line in the list of chains a destination holds.
 * @internal
 */
export interface BackupChainHead {
  chainId: string
  startedAt: number
  previousChainId?: string
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

function destinationError(message: string, err?: unknown): SirannonError {
  const detail = err instanceof Error ? `: ${err.message}` : ''
  return new SirannonError(`${message}${detail}`, 'BACKUP_DESTINATION_ERROR')
}

/**
 * Names the file one chain stores its own records under. Each chain gets a name
 * of its own, so deleting an old chain leaves the rest of the destination
 * alone.
 *
 * @param chainName - Name the list of chains is stored under.
 * @param chainId - Identifier of the chain.
 * @returns The name to read and write that chain's records under.
 *
 * @public
 */
export function chainLogName(chainName: string, chainId: string): string {
  return `${chainName}.${chainId}`
}

async function appendRecord(
  destination: BackupDestination,
  name: string,
  index: number,
  record: unknown,
): Promise<void> {
  try {
    await destination.writePiece(name, index, encoder.encode(JSON.stringify(record)))
  } catch (err) {
    throw destinationError(`The destination refused record ${index} of '${name}'`, err)
  }
}

async function readRecords(destination: BackupDestination, name: string): Promise<unknown[]> {
  let listed: { index: number }[]
  try {
    listed = await destination.listPieces(name)
  } catch (err) {
    throw destinationError(`The destination could not list the records of '${name}'`, err)
  }

  const ordered = [...listed].sort((left, right) => left.index - right.index)
  const records: unknown[] = []
  for (const piece of ordered) {
    let bytes: Uint8Array
    try {
      bytes = await destination.readPiece(name, piece.index)
    } catch (err) {
      throw destinationError(`The destination could not return record ${piece.index} of '${name}'`, err)
    }
    try {
      records.push(JSON.parse(decoder.decode(bytes)))
    } catch (err) {
      throw destinationError(`Record ${piece.index} of '${name}' is not a record Sirannon wrote`, err)
    }
  }
  return records
}

function isChainHead(value: unknown): value is BackupChainHead {
  const head = value as BackupChainHead
  return typeof head?.chainId === 'string' && typeof head.startedAt === 'number'
}

function isChainBase(value: unknown): value is BackupChainBase {
  const base = value as BackupChainBase
  return base?.kind === 'full' && typeof base.chainId === 'string' && typeof base.name === 'string'
}

function isChainChange(value: unknown): value is BackupChainChange {
  const change = value as BackupChainChange
  return (
    change?.kind === 'change' &&
    typeof change.chainId === 'string' &&
    typeof change.name === 'string' &&
    typeof change.sequence === 'number' &&
    typeof change.capturedAt === 'number'
  )
}

/**
 * Adds one chain to the list a destination holds. A later run, or a restore on
 * a machine that has never seen this database, finds the chain through that
 * list without being told its identifier.
 *
 * @param destination - Where the list is stored.
 * @param chainName - Name the list is stored under.
 * @param head - The chain to add.
 * @param index - Where it goes in the list, counted from zero.
 */
export async function appendChainHead(
  destination: BackupDestination,
  chainName: string,
  head: BackupChainHead,
  index: number,
): Promise<void> {
  await appendRecord(destination, chainName, index, head)
}

/**
 * Adds one record to a chain. The full copy takes position zero and each change
 * piece follows it in order.
 *
 * @param destination - Where the chain's records are stored.
 * @param chainName - Name the list of chains is stored under.
 * @param record - The record to add.
 * @param index - Where it goes in the chain, counted from zero.
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
 * Reads the list of chains a destination holds.
 *
 * @param destination - Where the list is stored.
 * @param chainName - Name the list is stored under.
 * @returns One line per chain, newest first.
 */
export async function readChainHeads(destination: BackupDestination, chainName: string): Promise<BackupChainHead[]> {
  const records = await readRecords(destination, chainName)
  return records.filter(isChainHead).reverse()
}

/**
 * Tells you what a destination holds: every chain, its full copy, and every
 * change piece taken from it. This reads nothing but the destination, so a
 * restore on a fresh machine can call it before any database exists.
 *
 * @param destination - Where the backups and their records are stored.
 * @param chainName - Name the list of chains is stored under. Defaults to `sirannon-backup-chain`.
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
    const records = await readRecords(destination, chainLogName(chainName, head.chainId))
    const changes = records.filter(isChainChange).sort((left, right) => left.sequence - right.sequence)
    const base = records.find(isChainBase)
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
