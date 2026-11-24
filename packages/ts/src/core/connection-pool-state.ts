import type { SQLiteConnection } from './driver/types.js'
import { EncryptionError } from './encryption/errors.js'
import { ConnectionPoolError, SirannonError } from './errors.js'

/**
 * Returns the error that a call on a pool fails with: closed, waiting for a key change to replace the file, or left
 * without connections when the reopen after a key change failed, and `null` when the pool can serve the call.
 *
 * @param closed - Whether the pool is closed.
 * @param suspended - Whether a key change has closed the readers and not reopened the pool yet.
 * @param reopenFailure - The error of the last failed reopen, or `null`.
 * @returns The error, or `null`.
 * @internal
 */
export function poolUnavailableError(closed: boolean, suspended: boolean, reopenFailure: unknown): Error | null {
  if (closed) return new ConnectionPoolError('Connection pool is closed')
  if (suspended && reopenFailure !== null) {
    if (reopenFailure instanceof SirannonError) return reopenFailure
    return new ConnectionPoolError(
      'The database could not open its connections again after a key change replaced its file; close it and open it again under the new key',
    )
  }
  if (suspended) {
    return new EncryptionError(
      'The database is replacing its file to finish a key change, so its connections reopen once the replacement ends; retry the call',
      'ENCRYPTION_CHANGE_IN_PROGRESS',
    )
  }
  return null
}

/**
 * Returns `true` once `register` reports that every read has settled, or `false` when the timeout passes first.
 *
 * @param register - Receives the function to call once every read has settled, or `null` to remove it.
 * @param timeoutMs - How long to wait.
 * @returns Whether the reads settled in time.
 * @internal
 */
export async function readsSettleWithin(
  register: (settled: (() => void) | null) => void,
  timeoutMs: number,
): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const drained = await Promise.race([
    new Promise<boolean>(resolve => {
      register(() => resolve(true))
    }),
    new Promise<boolean>(resolve => {
      timer = setTimeout(() => resolve(false), timeoutMs)
    }),
  ])
  clearTimeout(timer)
  register(null)
  return drained
}

/**
 * Closes each connection in order, and returns how many of the closes failed.
 *
 * @param connections - The connections to close.
 * @returns The number of failed closes.
 * @internal
 */
export async function closeCountingFailures(connections: readonly SQLiteConnection[]): Promise<number> {
  let failures = 0
  for (const conn of connections) {
    try {
      await conn.close()
    } catch {
      failures++
    }
  }
  return failures
}

/**
 * Returns a promise that settles once both closes settle, so that a pool can wait for every idle close under way.
 *
 * @param previous - The close already under way, or `null`.
 * @param next - The close that starts now.
 * @returns The promise of both.
 * @internal
 */
export function bothClosed(previous: Promise<void> | null, next: Promise<void>): Promise<void> {
  return Promise.all([previous, next]).then(() => undefined)
}
