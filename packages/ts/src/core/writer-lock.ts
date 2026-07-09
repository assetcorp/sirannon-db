import { AsyncLocalStorage } from 'node:async_hooks'

/**
 * Serialises writes onto the pool's single writer connection, which SQLite
 * limits to one open transaction at a time.
 */
export class WriterLock {
  private tail: Promise<unknown> = Promise.resolve()
  private readonly held = new AsyncLocalStorage<true>()

  run<T>(operation: () => Promise<T>): Promise<T> {
    // A call already inside a held operation (a writer method used within a
    // transaction callback) runs inline instead of queueing behind itself.
    if (this.held.getStore()) {
      return operation()
    }
    const enter = () => this.held.run(true, operation)
    const ticket = this.tail.then(enter, enter)
    // Swallow on the tail only, so a rejection reaches its caller without
    // poisoning the queue for the operations behind it.
    this.tail = ticket.then(swallow, swallow)
    return ticket
  }
}

function swallow(): void {}
