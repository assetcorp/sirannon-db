import { stat } from 'node:fs/promises'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { readChainHeads } from './chain.js'
import { checkpointLog } from './checkpoint.js'
import type { BackupDestination } from './destination.js'
import {
  type BackupGroupSource,
  type BackupNodePreference,
  type BackupSkip,
  preferredBackupNode,
} from './preferred-node.js'

/** What one node decided about the turn it was about to take.
 * @internal
 */
export interface BackupTurnDecision {
  /** Whether this node takes the backup. */
  runs: boolean
  /** What it skipped for, where it took none. */
  skip?: BackupSkip
}

const TAKES_THE_TURN: BackupTurnDecision = { runs: true }

function causeOf(err: unknown): string {
  if (err instanceof Error) return err.message
  return typeof err === 'string' ? err : 'the read failed without saying why'
}

/**
 * Describes the turn a cycle drops because its previous turn is still going.
 *
 * @returns The skip, ready for the cycle's own callback.
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
 * Works out whether this node is the one its replication group backs up from
 * right now, which a scheduled turn settles before it copies anything.
 *
 * A database with no group source answers yes every time, so a single-node
 * deployment runs the same cycle as a replicated one.
 *
 * @param group - Where this node reads its identity and its group's membership.
 * @param preference - Which node the operator wants the backups taken on.
 * @returns Whether this node takes the turn, and what it skipped for otherwise.
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
 * Folds the write-ahead log back into the database file on a node that takes
 * none of its group's backups.
 *
 * Sirannon turns SQLite's own checkpointing off in every database it backs up,
 * so a node capturing nothing still has to empty its log. Left alone, that log
 * would grow for as long as the process runs.
 *
 * @param request - The writer lock this runs inside, and the connection that writes.
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
 * Looks for a chain in the list its destination holds, and reports the loss
 * where the list no longer names it.
 *
 * Another node writing its own chain at the same moment can replace the record
 * that lists this one, and a record appended under a chain no listing names is
 * a record no restore reaches.
 *
 * @param destination - Where the list of chains is stored.
 * @param chainName - Name that list is stored under.
 * @param chainId - Identifier of the chain to look for.
 * @param databaseId - Identifier the report names the database by.
 * @returns The error to report, or null while the list still holds the chain.
 *
 * @internal
 */
export async function chainMissingFromList(
  destination: BackupDestination,
  chainName: string,
  chainId: string,
  databaseId: string,
): Promise<SirannonError | null> {
  const heads = await readChainHeads(destination, chainName)
  if (heads.some(head => head.chainId === chainId)) return null

  return new SirannonError(
    `The list of chains in '${chainName}' no longer names chain '${chainId}', so no restore can reach what that chain captured of database '${databaseId}'. ` +
      'The next turn starts a fresh chain with a full copy.',
    'BACKUP_CHAIN_BROKEN',
  )
}

/**
 * Measures the write-ahead log of a database whose cycle captured nothing this
 * turn, and reports the loss where that log has grown past the operator's limit.
 *
 * Sirannon empties the log on the strength of this report. PostgreSQL bounds a
 * replication slot the same way through `max_slot_wal_keep_size`, and both
 * default to leaving the log alone.
 *
 * @param logPath - Path of the write-ahead log.
 * @param maxBytes - How large the operator lets it grow, or undefined for no limit.
 * @param databaseId - Identifier the report names the database by.
 * @returns The error to report, or null while the log is inside the limit.
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
