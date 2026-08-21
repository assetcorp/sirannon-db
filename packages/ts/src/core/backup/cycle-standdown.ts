import type { SirannonError } from '../errors.js'
import { chainLostFromList, checkpointWithoutCapturing, logGrownPastLimit } from './cycle-guard.js'
import type { BackupCycleRequest } from './cycle-options.js'
import type { BackupCycleState } from './cycle-state.js'

/**
 * What a cycle hands the stand-down path, so that path lets go of a chain
 * without reaching into the cycle's own state.
 *
 * @internal
 */
export interface StandDownRequest {
  /** The operator's settings, plus the database the cycle runs against. */
  request: BackupCycleRequest
  /** Path of the write-ahead log behind that database. */
  logPath: string
  /** Whether the cycle still holds a chain. */
  holdsChain: () => boolean
  /** Sends the capture staged against that chain. */
  sendStagedCapture: () => Promise<unknown>
  /** Forgets the chain, the state file naming it, and the check behind it. */
  forgetChain: () => Promise<void>
  /** Passes an error to the operator. */
  report: (err: unknown) => void
}

async function sendBeforeStandDown(cycle: StandDownRequest): Promise<boolean> {
  try {
    await cycle.sendStagedCapture()
    return true
  } catch (err) {
    cycle.report(err)
    return false
  }
}

async function letGoOfChain(cycle: StandDownRequest): Promise<void> {
  if (cycle.holdsChain()) await cycle.forgetChain()
  await checkpointWithoutCapturing(cycle.request)
}

/**
 * Lets go of the chain a node was building, once the capture staged against it
 * has reached the destination. A piece already read off the log is a piece that
 * chain can still use, so a destination refusing it leaves the chain and the
 * log where they are for the turn after this one to send.
 *
 * The turn that brings the group's backups back to this node starts a fresh
 * chain, since a chain of physical pieces from one node continues on no other.
 *
 * @param cycle - What the cycle offers the stand-down path.
 * @returns Whether the node let go of the chain and emptied its log.
 *
 * @internal
 */
export async function standDownFromChain(cycle: StandDownRequest): Promise<boolean> {
  if (cycle.holdsChain() && !(await sendBeforeStandDown(cycle))) return false
  await letGoOfChain(cycle)
  return true
}

/**
 * Empties a log the cycle has left behind, where the operator set a limit and
 * the turn behind this call captured nothing. The chain ends there, and the
 * report names the writes that reach no backup.
 *
 * A staged capture the destination refuses goes with it, because an operator
 * setting that limit puts a writable database ahead of an unbroken chain.
 *
 * @param cycle - What the cycle offers the stand-down path.
 * @returns Whether the log had grown past the limit and was emptied.
 *
 * @internal
 */
export async function releaseChainPastLogLimit(cycle: StandDownRequest): Promise<boolean> {
  const lost = await logGrownPastLimit(cycle.logPath, cycle.request.maxUncapturedLogBytes, cycle.request.databaseId)
  if (!lost) return false

  if (cycle.holdsChain()) await sendBeforeStandDown(cycle)
  await letGoOfChain(cycle)
  cycle.report(lost)
  return true
}

/** What a cycle supplies to the chain check, so that check reads no cycle state of its own.
 * @internal
 */
export interface ChainGrip {
  /** The operator's settings, plus the database the cycle runs against. */
  request: BackupCycleRequest
  /** Name the list of chains is stored under. */
  chainName: string
  /** The chain the cycle is extending, or null where it has none. */
  state: BackupCycleState | null
  /** Whether an earlier turn already found that chain in the list. */
  verified: boolean
  /** Discards the chain and the capture staged against it. */
  discardState: () => Promise<void>
  /** Passes an error to the operator. */
  report: (err: SirannonError) => void
}

/**
 * Checks that the destination still lists the chain the cycle's state names,
 * including a chain that cycle started itself.
 *
 * No restore ever reads a record appended under a chain the listing omits, so
 * this check comes before the first record of every turn. A check that cannot
 * read the destination leaves the chain unverified, and the next turn tries
 * again before it appends anything.
 *
 * @param grip - The chain the cycle is extending, and what it does where that chain is absent.
 * @returns Whether the chain is still listed, which the cycle records until it discards that chain.
 *
 * @internal
 */
export async function confirmChainStillListed(grip: ChainGrip): Promise<boolean> {
  if (grip.verified || !grip.state) return grip.verified
  const lost = await chainLostFromList(grip.request, grip.chainName, grip.state)
  if (!lost) return true
  await grip.discardState()
  grip.report(lost)
  return false
}
