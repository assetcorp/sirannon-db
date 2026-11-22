import type { CoordinatorLease, CoordinatorWatchDisposer } from '../coordinator/types.js'

/**
 * Holds the controller lease that this node holds or watches, and the timer that drives its controller loop.
 *
 * @internal
 */
export class ControllerRuntime {
  /** Identifies the controller lease that this node holds, or is null while it holds none. */
  leaseId: string | null = null
  /** Stops the watch over the controller lease, or is null while no watch is open. */
  leaseWatchDisposer: CoordinatorWatchDisposer | null = null
  /** Holds the controller lease that the watch last reported, or null while no node holds it. */
  observedLease: CoordinatorLease | null = null
  /** Holds the time, in milliseconds since the Unix epoch, until which the observed lease counts as live. */
  observedLeaseLiveUntilMs = 0
  /** Is true while this node bids for the controller lease, so that it sends one bid at a time. */
  bidding = false
  /** Drives the controller loop, or is null while the loop is stopped. */
  timer: ReturnType<typeof setInterval> | null = null
  /** Holds the time, in milliseconds since the Unix epoch, at which this node last took the controller lease. */
  heldSinceMs = 0
  /** Lists the nodes whose live session this node has read since it last took the controller lease. */
  readonly seenSessionNodeIds = new Set<string>()

  /** Records that this node has just taken the controller lease, which starts the wait for every member's session. */
  noteLeaseTaken(): void {
    this.heldSinceMs = Date.now()
    this.seenSessionNodeIds.clear()
  }
}
