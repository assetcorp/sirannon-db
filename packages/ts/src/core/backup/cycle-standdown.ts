import type { SirannonError } from '../errors.js'
import { chainLostFromList, checkpointWithoutCapturing, logGrownPastLimit } from './cycle-guard.js'
import type { BackupCycleRequest } from './cycle-options.js'
import type { BackupCycleState } from './cycle-state.js'

/**
 * The callbacks and paths that a cycle passes to the stand-down code, so that
 * the code can release a chain without reading the state of the cycle.
 *
 * @internal
 */
export interface StandDownRequest {
  /** The settings of the operator, and the database that the cycle backs up. */
  request: BackupCycleRequest
  /** The path of the write-ahead log of that database. */
  logPath: string
  /** Whether the cycle still holds a chain. */
  holdsChain: () => boolean
  /** Sends the capture staged against that chain. */
  sendStagedCapture: () => Promise<unknown>
  /** Discards the chain and its staged capture, deletes the state file, and resets the check that the list names the chain. */
  forgetChain: () => Promise<void>
  /** Reports an error to the operator. */
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
 * Sends the capture staged against the chain of a node, and then releases the
 * chain and checkpoints the log. The chain can still use a capture that
 * Sirannon has already read from the log, so when the destination refuses it,
 * Sirannon keeps the chain and the log as they are for the next turn to send.
 *
 * When Sirannon picks this node again, its next turn starts a new chain, since
 * the physical pieces from one node can extend no chain on another node.
 *
 * @param cycle - The callbacks and paths that the cycle passes in.
 * @returns Whether the node releases the chain and checkpoints its log.
 *
 * @internal
 */
export async function standDownFromChain(cycle: StandDownRequest): Promise<boolean> {
  if (cycle.holdsChain() && !(await sendBeforeStandDown(cycle))) return false
  await letGoOfChain(cycle)
  return true
}

/**
 * Checkpoints the log to empty it when the operator sets a limit, the log grows
 * past that limit, and the turn that calls this captures nothing. The chain
 * ends there, and Sirannon reports the writes that no backup holds.
 *
 * When the destination refuses the staged capture, Sirannon discards that
 * capture as well, because an operator who sets the limit ranks a writable
 * database above an unbroken chain.
 *
 * @param cycle - The callbacks and paths that the cycle passes in.
 * @returns Whether the log has grown past the limit, in which case Sirannon releases the chain and checkpoints the log.
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

/** The state and callbacks that a cycle passes to the chain check, so that the check reads only what the cycle passes in.
 * @internal
 */
export interface ChainGrip {
  /** The settings of the operator, and the database that the cycle backs up. */
  request: BackupCycleRequest
  /** The name that Sirannon stores the list of chains under. */
  chainName: string
  /** The state of the chain that the cycle extends, or null where the cycle holds no chain. */
  state: BackupCycleState | null
  /** Whether an earlier turn confirmed that the list names that chain. */
  verified: boolean
  /** Discards the chain and the capture staged against it. */
  discardState: () => Promise<void>
  /** Reports an error to the operator. */
  report: (err: SirannonError) => void
}

/**
 * Checks that the list of chains at the destination still names the chain in
 * the state of the cycle, including a chain that the cycle started itself.
 *
 * A restore finds records only through a chain in the list, so the cycle runs
 * this check before it appends the first record of every turn. When the check
 * cannot read the destination, the chain stays unverified, and the next turn
 * checks again before it appends anything.
 *
 * @param grip - The chain that the cycle extends, and the callbacks that run where the list omits it.
 * @returns Whether the list still names the chain, which the cycle keeps until it discards that chain.
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
