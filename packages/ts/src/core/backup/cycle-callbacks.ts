import { invokeCallerCallback } from '../caller-callbacks.js'

/**
 * Passes one value to a callback that the caller supplies, and discards any
 * error that the callback throws or that its returned promise rejects with.
 *
 * Sirannon delivers every report of the cycle, whether of a run, a skip, a
 * failure, or the progress of a copy, from inside the turn that produces it. A
 * callback that throws would otherwise abort that turn, so a fault in the
 * reporting alone would fail the backup. The cycle never waits for a promise
 * that the callback returns, so an asynchronous callback completes
 * independently of the turn.
 *
 * @param callback - The callback that the caller supplies, if any.
 * @param value - The value to report.
 */
export function reportQuietly<T>(callback: ((value: T) => void) | undefined, value: T): void {
  if (!callback) return
  invokeCallerCallback(() => callback(value))
}
