/**
 * Serialises operations on the single shared writer connection.
 *
 * SQLite allows one open transaction per connection, and the connection pool
 * exposes exactly one writer. Without serialisation, two writer operations
 * interleave at their `await` points: a second `BEGIN` fails with 'cannot
 * start a transaction within a transaction', a concurrent write commits under
 * a bulk load's relaxed `PRAGMA synchronous`, and a CDC poll reads rows a
 * still-open transaction has not committed. Chaining every writer operation
 * onto one promise turns each caller into a strict follow-on.
 *
 * The replication engine's `LocalExecutor` applies the same pattern to its own
 * connection for the same reason.
 */
export class WriterLock {
  private tail: Promise<unknown> = Promise.resolve()

  /**
   * Run `operation` once every previously queued operation has settled, and
   * return its result to the caller. A rejection reaches the caller that
   * queued it but never poisons the queue, so a failed operation does not
   * block the operations behind it.
   */
  run<T>(operation: () => Promise<T>): Promise<T> {
    const ticket = this.tail.then(operation, operation)
    this.tail = ticket.then(swallow, swallow)
    return ticket
  }
}

function swallow(): void {}
