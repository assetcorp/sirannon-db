function isThenable(value: unknown): value is PromiseLike<unknown> {
  return value != null && typeof (value as { then?: unknown }).then === 'function'
}

/**
 * Returns an Error for whatever value a callback threw or rejected with.
 *
 * @param reason - The value that the callback threw or rejected with.
 * @returns The value itself when it is an Error, or otherwise a new Error whose message is the value as a string.
 */
export function toError(reason: unknown): Error {
  return reason instanceof Error ? reason : new Error(String(reason))
}

/**
 * Passes a failure to the reporter that the caller supplied, and discards any error that the reporter throws.
 *
 * @param onFailure - The reporter that the caller supplied, or undefined when the caller supplied none.
 * @param reason - The value that the callback threw or rejected with.
 */
export function reportCallerFailure(onFailure: ((error: Error) => void) | undefined, reason: unknown): void {
  if (!onFailure) return
  try {
    onFailure(toError(reason))
  } catch {}
}

/**
 * Calls a callback that the caller supplied and catches anything that it throws or rejects with.
 *
 * This function returns as soon as the callback returns, without awaiting a promise
 * that the callback returns, so two calls to an asynchronous callback can overlap. It
 * passes a throw or a rejection to `onFailure` when the caller supplied one, and
 * discards the error otherwise.
 *
 * @param callback - The callback to call with no arguments, so it takes any values that it needs from its closure.
 * @param onFailure - Called with whatever the callback throws or rejects with.
 */
export function invokeCallerCallback(callback: () => unknown, onFailure?: (error: Error) => void): void {
  let returned: unknown
  try {
    returned = callback()
  } catch (err) {
    reportCallerFailure(onFailure, err)
    return
  }
  if (isThenable(returned)) {
    Promise.resolve(returned).then(undefined, reason => reportCallerFailure(onFailure, reason))
  }
}
