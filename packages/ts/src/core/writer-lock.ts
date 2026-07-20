import type { WriterContext } from './driver/types.js'

/**
 * Stands in for a runtime that cannot track async context. Reporting every
 * caller as an outsider keeps a stranger's writes out of an open transaction;
 * the cost is that a writer method called inside a transaction callback waits
 * on the transaction that is waiting on it.
 */
const UNTRACKED: WriterContext = {
  run: operation => operation(),
  isActive: () => false,
  exit: operation => operation(),
}

/**
 * Serialises writes onto the pool's single writer connection, which SQLite
 * limits to one open transaction at a time.
 */
export class WriterLock {
  private tail: Promise<unknown> = Promise.resolve()
  private readonly context: WriterContext

  constructor(context?: WriterContext) {
    this.context = context ?? UNTRACKED
  }

  run<T>(operation: () => Promise<T>): Promise<T> {
    // A call already inside a held operation (a writer method used within a
    // transaction callback) runs inline instead of queueing behind itself.
    if (this.context.isActive()) {
      return operation()
    }
    const enter = () => this.context.run(operation)
    const ticket = this.tail.then(enter, enter)
    // Swallow on the tail only, so a rejection reaches its caller without
    // poisoning the queue for the operations behind it.
    this.tail = ticket.then(swallow, swallow)
    return ticket
  }

  isHeld(): boolean {
    return this.context.isActive()
  }

  async settle(): Promise<void> {
    await this.run(() => Promise.resolve())
  }

  /**
   * Work scheduled from inside a held operation inherits it, so shared
   * machinery must detach or it runs every caller's work inside one caller's
   * transaction.
   */
  detached<T>(operation: () => T): T {
    return this.context.exit(operation)
  }
}

function swallow(): void {}
