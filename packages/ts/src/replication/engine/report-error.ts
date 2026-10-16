import type { ReplicationEngine } from './engine.js'

/**
 * Reports a failed operation on the engine's `replication-error` event, and wraps a thrown value that is not an
 * `Error` in one.
 *
 * @param engine - The engine that emits the event.
 * @param err - The value that the failed operation threw.
 * @param operation - The name of the failed operation, which the event carries.
 * @param recoverable - True when the engine tries the operation again or carries on without it.
 *
 * @internal
 */
export function reportEngineError(
  engine: ReplicationEngine,
  err: unknown,
  operation: string,
  recoverable: boolean,
): void {
  const error = err instanceof Error ? err : new Error(String(err))
  engine.emitError({ error, operation, recoverable })
}
