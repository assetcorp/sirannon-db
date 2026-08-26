function isThenable(value: unknown): value is PromiseLike<unknown> {
  return value != null && typeof (value as { then?: unknown }).then === 'function'
}

/**
 * Turns whatever a callback threw or rejected with into an Error a reporter can read.
 *
 * @param reason - The value a throw or a rejection carried.
 * @returns The value itself where it is already an Error, and an Error carrying its text otherwise.
 */
export function toError(reason: unknown): Error {
  return reason instanceof Error ? reason : new Error(String(reason))
}

/**
 * Passes one failure to a reporter the caller supplied, and drops what that reporter throws.
 *
 * Reporting a failure must never raise a second one, so this absorbs anything the
 * reporter itself throws.
 *
 * @param onFailure - The reporter the caller supplied, where they supplied one.
 * @param reason - The value a throw or a rejection carried.
 */
export function reportCallerFailure(onFailure: ((error: Error) => void) | undefined, reason: unknown): void {
  if (!onFailure) return
  try {
    onFailure(toError(reason))
  } catch {}
}

/**
 * Calls a callback the caller supplied, so that neither a throw nor a rejection escapes.
 *
 * The call returns as soon as the callback returns, without waiting for a promise the
 * callback hands back, so two calls to an asynchronous callback can overlap. Sirannon
 * passes a throw, and a rejection of the promise the callback returned, to `onFailure`
 * where the caller supplied one, and it drops each of them otherwise.
 *
 * @param callback - The callback to call, which takes its own arguments from the closure.
 * @param onFailure - Receives whatever the callback throws or rejects with.
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
