import { invokeCallerCallback } from '../caller-callbacks.js'

/**
 * Passes one value to a callback the caller supplied, dropping whatever that
 * callback throws and whatever a promise it returns rejects with.
 *
 * Every report the cycle makes, whether of a run, a skip, a failure, or the
 * progress of a copy, reaches the caller from inside the turn that produced it.
 * A callback that throws would otherwise abort that turn, so the backup would
 * fail for a fault in the reporting alone. The cycle never waits for what the
 * callback returns, so an asynchronous callback finishes on its own.
 *
 * @param callback - The callback the caller supplied, where they supplied one.
 * @param value - What the cycle has to report.
 */
export function reportQuietly<T>(callback: ((value: T) => void) | undefined, value: T): void {
  if (!callback) return
  invokeCallerCallback(() => callback(value))
}
