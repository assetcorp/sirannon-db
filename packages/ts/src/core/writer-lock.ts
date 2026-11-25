import type { WriterContext } from './driver/types.js'
import { SirannonError } from './errors.js'

const UNTRACKED: WriterContext = {
  run: operation => operation(),
  isActive: () => false,
  exit: operation => operation(),
}

/**
 * The code of the error that a call through the database raises when it needs the writer while a transaction
 * callback holds it, on a driver that has no async context to tell the calls that the callback makes from the calls of other callers.
 *
 * @internal
 */
export const WRITER_HELD_BY_CALLBACK_CODE = 'WRITER_HELD_BY_CALLBACK'

export class WriterLock {
  private tail: Promise<unknown> = Promise.resolve()
  private readonly context: WriterContext
  private readonly tracksCallers: boolean
  private callbacksHolding = 0
  private repair: (() => Promise<void>) | null = null
  private readonly releaseListeners = new Set<() => void>()

  /**
   * @param context - The driver's async context, which tells the calls that a holder makes from those of other
   * callers.
   * @param prepareHold - Called at the start of every hold, before its operation, and returns `null` when the hold
   * can start at once or the promise that the hold waits for first, such as the reopen of a writer that the pool
   * closed while idle.
   */
  constructor(
    context?: WriterContext,
    private readonly prepareHold: () => Promise<void> | null = noPreparation,
  ) {
    this.context = context ?? UNTRACKED
    this.tracksCallers = context !== undefined
  }

  /**
   * Holds the writer for a callback that the application supplies, such as a transaction callback, and returns what
   * the callback returns.
   *
   * @param operation - The work that calls the application's callback.
   * @returns What `operation` returns.
   */
  runCallback<T>(operation: () => Promise<T>): Promise<T> {
    if (this.tracksCallers) return this.run(operation)
    return this.run(async () => {
      this.callbacksHolding++
      try {
        return await operation()
      } finally {
        this.callbacksHolding--
      }
    })
  }

  /**
   * Returns a `WRITER_HELD_BY_CALLBACK` error when an application callback holds the writer and the driver cannot
   * tell whether this caller belongs to it, because waiting would never end for a call that the callback awaits, and
   * returns `null` when the caller can wait for the writer.
   *
   * @returns The error to reject the call with, or `null`.
   */
  callerRefusal(): SirannonError | null {
    if (this.callbacksHolding === 0) return null
    return new SirannonError(
      'A transaction callback holds the writer, and this driver has no async context to tell its calls from other callers, so a call through the database fails until the callback returns; make the call through the Transaction handle inside the callback, or after the transaction ends',
      WRITER_HELD_BY_CALLBACK_CODE,
    )
  }

  /**
   * Holds the writer for an operation, after every earlier hold has ended, and calls each release listener once the
   * operation settles, because the operation may have committed a write.
   *
   * @param operation - The work to do while holding the writer.
   * @returns What `operation` returns.
   */
  run<T>(operation: () => Promise<T>): Promise<T> {
    return this.hold(operation, this.notifyReleased)
  }

  /**
   * Holds the writer as {@link WriterLock.run} does, without calling the release listeners, for an operation that
   * commits nothing to the change log, such as a read of the change log or a write to an internal table.
   *
   * @param operation - The work to do while holding the writer.
   * @returns What `operation` returns.
   */
  runWithoutChangeSignal<T>(operation: () => Promise<T>): Promise<T> {
    return this.hold(operation, swallow)
  }

  /**
   * Registers a function that the lock calls each time a hold through {@link WriterLock.run} ends, so that a change
   * poller can read the change log once the write commits. The lock calls it after the hold settles, so the call adds
   * no time to the hold, and it ignores anything that the function throws.
   *
   * @param listener - The function to call, which must return at once.
   * @returns A function that removes the registration.
   */
  onRelease(listener: () => void): () => void {
    this.releaseListeners.add(listener)
    return () => {
      this.releaseListeners.delete(listener)
    }
  }

  private hold<T>(operation: () => Promise<T>, released: () => void): Promise<T> {
    if (this.context.isActive()) {
      return operation()
    }
    const enter = () => this.context.run(() => this.enter(operation))
    const ticket = this.tail.then(enter, enter)
    this.tail = ticket.then(released, released)
    return ticket
  }

  private enter<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.repair === null ? operation : () => this.repairThen(operation)
    let preparing: Promise<void> | null
    try {
      preparing = this.prepareHold()
    } catch (err) {
      return Promise.reject(err)
    }
    return preparing === null ? run() : preparing.then(run)
  }

  private readonly notifyReleased = (): void => {
    for (const listener of this.releaseListeners) {
      try {
        listener()
      } catch {}
    }
  }

  repairBeforeNextHold(repair: () => Promise<void>): void {
    this.repair = repair
  }

  private async repairThen<T>(operation: () => Promise<T>): Promise<T> {
    const repair = this.repair
    if (repair !== null) {
      await repair()
      if (this.repair === repair) this.repair = null
    }
    return operation()
  }

  isHeld(): boolean {
    return this.context.isActive()
  }

  async settle(): Promise<void> {
    if (this.context.isActive()) return
    await this.tail
  }

  detached<T>(operation: () => T): T {
    return this.context.exit(operation)
  }
}

function swallow(): void {}

function noPreparation(): null {
  return null
}
