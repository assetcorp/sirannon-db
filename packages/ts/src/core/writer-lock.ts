import type { WriterContext } from './driver/types.js'
import { SirannonError } from './errors.js'

const UNTRACKED: WriterContext = {
  run: operation => operation(),
  isActive: () => false,
  exit: operation => operation(),
}

/**
 * The code of the error that a call through the database raises when it needs the writer while a transaction
 * callback holds it, on a driver that has no async context to tell the callback's own calls from other callers.
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

  constructor(context?: WriterContext) {
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

  run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.context.isActive()) {
      return operation()
    }
    const enter = () => this.context.run(this.repair === null ? operation : () => this.repairThen(operation))
    const ticket = this.tail.then(enter, enter)
    this.tail = ticket.then(swallow, swallow)
    return ticket
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
