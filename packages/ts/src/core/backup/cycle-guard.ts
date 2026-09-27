import { stat } from 'node:fs/promises'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { readChainHeadAt, readChainHeads } from './chain.js'
import { checkpointLog } from './checkpoint.js'
import type { BackupDestination } from './destination.js'
import {
  type BackupGroupSource,
  type BackupNodePreference,
  type BackupSkip,
  preferredBackupNode,
} from './preferred-node.js'

/** Whether one node takes the turn that it is about to start, and the reason where it skips.
 * @internal
 */
export interface BackupTurnDecision {
  /** Whether this node takes the backup. */
  runs: boolean
  /** The reason for the skip, where the node takes no backup. */
  skip?: BackupSkip
}

const TAKES_THE_TURN: BackupTurnDecision = { runs: true }

const STARTS_A_FRESH_CHAIN = ['BACKUP_LOG_REWOUND', 'BACKUP_CHAIN_BROKEN']

/**
 * Returns whether an error leaves a chain that no later piece can extend, in
 * which case the cycle starts a new chain with a full copy.
 *
 * @param err - The error that stops the turn.
 * @returns Whether the error is a `BACKUP_LOG_REWOUND` or a `BACKUP_CHAIN_BROKEN`.
 *
 * @internal
 */
export function startsAFreshChain(err: unknown): boolean {
  return err instanceof SirannonError && STARTS_A_FRESH_CHAIN.includes(err.code)
}

/**
 * Returns an Error for any value that a turn throws, so that the operator
 * always receives an Error.
 *
 * @param value - The value that the turn throws.
 * @returns The value itself when it is an Error, or otherwise a `BACKUP_ERROR` whose message is the value where it is a string, or a general message where it is not.
 *
 * @internal
 */
export function toBackupError(value: unknown): Error {
  if (value instanceof Error) return value
  return new SirannonError(typeof value === 'string' ? value : 'The backup cycle failed', 'BACKUP_ERROR')
}

function causeOf(err: unknown): string {
  if (err instanceof Error) return err.message
  return typeof err === 'string' ? err : 'the read failed without saying why'
}

/**
 * Returns the skip for a turn that the cycle drops because its previous turn is
 * still in progress.
 *
 * @returns The skip to pass to the callback of the cycle.
 *
 * @internal
 */
export function previousRunStillActive(): BackupSkip {
  return {
    reason: 'previous-run-active',
    message: 'The cycle is still running its previous turn. It skipped this one rather than queueing behind it.',
  }
}

/**
 * Returns whether this node is the one that its replication group takes
 * backups from at this moment, which a turn checks before it copies anything.
 *
 * Without a group source, this always returns that the node takes the turn, so
 * a single-node deployment runs the same cycle as a replicated one.
 *
 * @param group - The source of the identity of this node and the membership of its group.
 * @param preference - The node that the operator wants the backups taken on.
 * @returns Whether this node takes the turn, and the reason for the skip where it takes none.
 *
 * @internal
 */
export async function decideBackupTurn(
  group: BackupGroupSource | undefined,
  preference: BackupNodePreference,
): Promise<BackupTurnDecision> {
  if (!group) {
    return TAKES_THE_TURN
  }

  const nodeId = group.nodeId
  if (typeof preference !== 'string') {
    return decide(nodeId, preference.nodeId)
  }

  let preferredNodeId: string | null
  try {
    preferredNodeId = preferredBackupNode(await group.readMembership(), preference)
  } catch (err) {
    return {
      runs: false,
      skip: {
        reason: 'group-unavailable',
        message: `Node '${nodeId}' could not read its replication group's membership: ${causeOf(err)}. It ran no backup this turn.`,
        nodeId,
      },
    }
  }

  return decide(nodeId, preferredNodeId)
}

function decide(nodeId: string, preferredNodeId: string | null): BackupTurnDecision {
  if (preferredNodeId === nodeId) {
    return TAKES_THE_TURN
  }
  if (preferredNodeId === null) {
    return {
      runs: false,
      skip: {
        reason: 'not-preferred',
        message: `This replication group currently names no node to back it up. Node '${nodeId}' ran none this turn.`,
        nodeId,
      },
    }
  }
  return {
    runs: false,
    skip: {
      reason: 'not-preferred',
      message: `Node '${preferredNodeId}' takes this replication group's backups, so node '${nodeId}' ran none this turn.`,
      nodeId,
      preferredNodeId,
    },
  }
}

/**
 * Checkpoints the write-ahead log into the database file on a node that takes
 * none of the backups of its group.
 *
 * Sirannon turns off the automatic checkpoints of SQLite in every database that
 * it backs up, so a node that captures nothing must still empty its log, or the
 * log would grow for as long as the process runs.
 *
 * @param request - The function that holds the writer lock, and the function that returns the writer connection.
 *
 * @internal
 */
export async function checkpointWithoutCapturing(request: {
  runExclusive: (op: () => Promise<void>) => Promise<void>
  acquireWriter: () => SQLiteConnection
}): Promise<void> {
  await request.runExclusive(async () => {
    await checkpointLog(request.acquireWriter())
  })
}

/**
 * Looks for a chain in the list of chains at its destination, and returns a
 * `BACKUP_CHAIN_BROKEN` error where the list no longer names it.
 *
 * Another node that writes its own chain at the same moment can replace the
 * entry for this chain, which puts every record under this chain out of reach
 * of a restore.
 *
 * Where the cycle recorded the index of its chain, this reads that one entry,
 * so the check takes a single read however many chains the list holds. Where
 * that entry names another chain, this reports the chain as lost. Where the
 * read fails or the record there is not a valid entry, this reads the whole
 * list.
 *
 * @param destination - The destination that holds the list of chains.
 * @param chainName - The name that Sirannon stores that list under.
 * @param chainId - The identifier of the chain to look for.
 * @param databaseId - The identifier of the database, which the error message quotes.
 * @param headIndex - The index of the chain in the list, where the cycle recorded one.
 * @returns The error to report, or null while the list still names the chain.
 *
 * @internal
 */
export async function chainMissingFromList(
  destination: BackupDestination,
  chainName: string,
  chainId: string,
  databaseId: string,
  headIndex?: number,
): Promise<SirannonError | null> {
  if (headIndex !== undefined) {
    const listed = await readChainHeadAt(destination, chainName, headIndex).catch(() => undefined)
    if (listed?.chainId === chainId) return null
    if (listed) return chainLost(chainName, chainId, databaseId)
  }

  const heads = await readChainHeads(destination, chainName)
  if (heads.some(head => head.chainId === chainId)) return null
  return chainLost(chainName, chainId, databaseId)
}

function chainLost(chainName: string, chainId: string, databaseId: string): SirannonError {
  return new SirannonError(
    `The list of chains in '${chainName}' no longer names chain '${chainId}', so no restore can reach what that chain captured of database '${databaseId}'. ` +
      'The next turn starts a fresh chain with a full copy.',
    'BACKUP_CHAIN_BROKEN',
  )
}

/**
 * Returns a `BACKUP_DESTINATION_ERROR` for a replication group whose
 * destination lacks `writePieceIfAbsent`, since that is the one arrangement
 * where two nodes that start a chain at the same moment can lose one of the two
 * chains.
 *
 * @param request - The settings of the operator, and the database that the cycle backs up.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @returns The error to report, or null where the database has no replication group or the destination implements `writePieceIfAbsent`.
 *
 * @internal
 */
export function unclaimableChainList(
  request: { destination: BackupDestination; replicationGroup?: unknown; databaseId: string },
  chainName: string,
): SirannonError | null {
  if (!request.replicationGroup || request.destination.writePieceIfAbsent) return null

  return new SirannonError(
    `The backups of database '${request.databaseId}' belong to a replication group, and this destination offers no writePieceIfAbsent. ` +
      `Two nodes starting a chain at the same moment would each replace the record listing the other's in '${chainName}'. ` +
      'Give the destination that function, or give each node a chainName of its own.',
    'BACKUP_DESTINATION_ERROR',
  )
}

/**
 * Returns the size of the write-ahead log on a node, which the skip report
 * states on each turn that backs nothing up.
 *
 * @param logPath - The path of the write-ahead log.
 * @returns The size of the log in bytes, or undefined where Sirannon cannot read the size of the file.
 *
 * @internal
 */
export async function uncapturedLogBytes(logPath: string): Promise<number | undefined> {
  try {
    return (await stat(logPath)).size
  } catch {
    return undefined
  }
}

/**
 * Measures the write-ahead log of a database whose cycle captures nothing this
 * turn, and returns a `BACKUP_CHAIN_BROKEN` error where that log has grown past
 * the limit that the operator sets.
 *
 * Sirannon empties the log when this returns an error. PostgreSQL limits a
 * replication slot the same way through `max_slot_wal_keep_size`, and neither
 * sets a limit by default.
 *
 * @param logPath - The path of the write-ahead log.
 * @param maxBytes - The largest size in bytes that the operator allows, or undefined for no limit.
 * @param databaseId - The identifier of the database, which the error message quotes.
 * @returns The error to report, or null while the log is within the limit or absent.
 *
 * @internal
 */
export async function logGrownPastLimit(
  logPath: string,
  maxBytes: number | undefined,
  databaseId: string,
): Promise<SirannonError | null> {
  if (maxBytes === undefined) return null

  let bytes: number
  try {
    bytes = (await stat(logPath)).size
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return null
    throw err
  }
  if (bytes <= maxBytes) return null

  return new SirannonError(
    `The write-ahead log of database '${databaseId}' holds ${bytes} bytes that no backup does, against a maxUncapturedLogBytes of ${maxBytes}. ` +
      'Sirannon has emptied that log to keep the database writable, so those writes are in no backup and the next turn it can run starts a fresh chain with a full copy.',
    'BACKUP_CHAIN_BROKEN',
  )
}

/**
 * Checks whether the list of chains at the destination still names the chain
 * in the state file of the cycle.
 *
 * @param request - The destination, and the database that the cycle backs up.
 * @param chainName - The name that Sirannon stores the list of chains under.
 * @param state - The chain that the cycle extends, and its index in that list.
 * @returns The error where the list omits the chain, or null where the list still names it.
 *
 * @internal
 */
export async function chainLostFromList(
  request: { destination: BackupDestination; databaseId: string },
  chainName: string,
  state: { chainId: string; headIndex?: number },
): Promise<SirannonError | null> {
  return chainMissingFromList(request.destination, chainName, state.chainId, request.databaseId, state.headIndex)
}
