import type { ChangeEvent } from '../types.js'
import { ChangePoller } from './change-poller.js'
import type { DatabaseChangeSignal } from './change-signal.js'
import type { ChangeTracker } from './change-tracker.js'
import { PruneSchedule } from './prune-schedule.js'
import type { SubscriptionManager } from './subscription.js'

const DELIVERIES_PER_DISPATCH_SLICE = 20_000

/**
 * The settings of {@link startChangeFeed}.
 *
 * @internal
 */
export interface ChangeFeedOptions {
  tracker: ChangeTracker
  manager: SubscriptionManager
  signal: DatabaseChangeSignal
  /** The shortest time between two reads of the change log, in milliseconds. */
  intervalMs: number
  /** Reads the changes after the tracker's cursor, through `tracker.poll`, on a connection that the caller picks. */
  readChanges: () => Promise<ChangeEvent[]>
  /**
   * Prunes the change log and the expired device cursors, and returns the time in milliseconds since the Unix epoch
   * at which the next prune can remove more, or `null`, as `pruneChangeLog` does.
   */
  prune: () => Promise<number | null>
  /** Receives each failed read with the number of reads in a row that have failed, and returns `false` to stop. */
  onFailure: (error: unknown, consecutiveFailures: number) => boolean
}

/**
 * The poller behind one {@link SubscriptionManager}, which reads the change log only while the manager has a
 * subscription.
 *
 * @internal
 */
export interface ChangeFeed {
  /** Stops reading, until {@link ChangeFeed.resume} or until the manager gains its first subscription again. */
  pause(): Promise<void>
  /** Starts reading again after {@link ChangeFeed.pause}, when the manager has a subscription. */
  resume(): void
  /** Asks for a prune within one prune cadence after a write changes a device cursor. */
  requestPrune(): void
  /** Stops reading for good. */
  stop(): void
}

/**
 * Starts the poller that delivers a database's change log to a {@link SubscriptionManager}.
 *
 * The poller reads the log when the database's {@link DatabaseChangeSignal} fires, at most once per interval, and only
 * while the manager has a subscription, so a database without subscribers or without writes costs no reads. After a
 * read that found changes, the feed hands them to the manager in slices of about 20,000
 * deliveries, and lets other work run between two slices, so that a large batch to many subscribers never holds the
 * event loop for the whole batch. It ends each batch that carried changes, and each read that follows a batch that
 * ended inside a transaction, with {@link SubscriptionManager.endBatch}. A {@link PruneSchedule} decides when the
 * feed prunes the log.
 *
 * @param options - The tracker, the manager, the signal, and the read, prune, and failure calls.
 * @returns The controls of the feed.
 * @internal
 */
export function startChangeFeed(options: ChangeFeedOptions): ChangeFeed {
  const { tracker, manager, signal } = options
  let lastBatchEndedAtTxBoundary = true
  let paused = false
  let stopped = false
  let pruning = false

  const poller = new ChangePoller({
    signal,
    intervalMs: options.intervalMs,
    onFailure: options.onFailure,
    poll: async () => {
      const events = await options.readChanges()
      if (events.length > 0) await dispatchInSlices(manager, events)
      if (events.length > 0 || !lastBatchEndedAtTxBoundary) {
        lastBatchEndedAtTxBoundary = tracker.pollEndedAtTxBoundary
        manager.endBatch(lastBatchEndedAtTxBoundary)
      }
      if (events.length > 0 || !signal.detectsEveryWrite) prunes.requestSoon()
      if (prunes.takeDue()) {
        pruning = true
        try {
          prunes.pruned(await options.prune())
        } catch (err) {
          prunes.requestSoon()
          throw err
        } finally {
          pruning = false
        }
      }
      return events.length > 0
    },
  })
  const prunes = new PruneSchedule(options.intervalMs, poller.wake)
  const removeBoundaryListener = tracker.onPruneBoundaryChange(() => {
    if (!pruning) prunes.requestSoon()
  })

  const follow = (active: boolean): void => {
    if (stopped || paused) return
    if (active) {
      poller.start()
      prunes.requestSoon()
      prunes.resume()
      return
    }
    poller.stop()
    prunes.pause()
  }
  const removeActivityListener = manager.onActivityChange(follow)
  follow(manager.size > 0)

  return {
    async pause() {
      paused = true
      poller.stop()
      prunes.pause()
      await poller.settled()
    },
    resume() {
      paused = false
      follow(manager.size > 0)
    },
    requestPrune() {
      prunes.requestSoon()
    },
    stop() {
      stopped = true
      removeActivityListener()
      removeBoundaryListener()
      poller.stop()
      prunes.pause()
    },
  }
}

async function dispatchInSlices(manager: SubscriptionManager, events: ChangeEvent[]): Promise<void> {
  const subscribedBefore = manager.nextSubscriptionId
  const eventsPerSlice = Math.max(1, Math.floor(DELIVERIES_PER_DISPATCH_SLICE / Math.max(1, manager.size)))
  if (events.length <= eventsPerSlice) {
    manager.dispatch(events, subscribedBefore)
    return
  }
  for (let start = 0; start < events.length; start += eventsPerSlice) {
    if (start > 0) await nextMacrotask()
    manager.dispatch(events.slice(start, start + eventsPerSlice), subscribedBefore)
  }
}

function nextMacrotask(): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, 0))
}
