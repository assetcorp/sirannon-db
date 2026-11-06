import type { SirannonError } from './errors.js'
import { ReplicationError } from './sync/errors.js'

/**
 * Counts the replication engines that write to one database through a separate writer connection, so that the
 * database can refuse an operation that would replace its file under those connections.
 *
 * @internal
 */
export class RegisteredReplicationEngines {
  private count = 0
  private readonly replacements = new Set<() => SirannonError>()

  constructor(private readonly databaseId: string) {}

  /**
   * Records one running engine, and returns the call that removes the record, which does nothing after its first
   * call.
   *
   * @returns The call that removes the record.
   * @throws The error of a file replacement under way, which would leave the engine on the replaced file.
   */
  register(): () => void {
    const replacement = this.replacements.values().next()
    if (replacement.done !== true) throw replacement.value()
    this.count++
    let registered = true
    return () => {
      if (!registered) return
      registered = false
      this.count--
    }
  }

  /**
   * Throws when an engine runs, because the engine's writer connection would stay on the replaced file.
   *
   * @param operation - The operation that would replace the file, as the error message states it.
   * @throws A `ReplicationError` with `REPLICATION_ACTIVE` while an engine runs.
   */
  assertNone(operation: string): void {
    if (this.count === 0) return
    throw new ReplicationError(
      `A replication engine writes to database '${this.databaseId}' through a separate writer connection, and a ${operation} would leave that connection on the replaced file. Stop the engine, run the ${operation}, and then start the engine again.`,
      'REPLICATION_ACTIVE',
    )
  }

  /**
   * Refuses the operation while an engine runs, and otherwise refuses every engine that starts before the returned
   * call ends the replacement.
   *
   * @param operation - The operation that replaces the file, as the error message states it.
   * @param refuseEngine - Builds the error that an engine which starts during the replacement receives.
   * @returns The call that ends the replacement.
   * @throws A `ReplicationError` with `REPLICATION_ACTIVE` while an engine runs.
   */
  beginReplacement(operation: string, refuseEngine: () => SirannonError): () => void {
    this.assertNone(operation)
    const refusal = () => refuseEngine()
    this.replacements.add(refusal)
    return () => {
      this.replacements.delete(refusal)
    }
  }
}
