interface OnlineEventTarget {
  addEventListener(type: 'online', listener: () => void): void
  removeEventListener(type: 'online', listener: () => void): void
}

/**
 * The device's own report of its network, which a browser or a worker gives through `navigator.onLine` and the `online` event.
 *
 * @internal
 */
export interface NetworkSignal {
  /** Returns true only when the runtime reports that the device has no network at all. */
  reportsOffline(): boolean
  /**
   * Calls `onOnline` each time the runtime reports that the network is back.
   *
   * @param onOnline - The function to call.
   * @returns A function that stops the calls.
   */
  watchOnline(onOnline: () => void): () => void
}

function onlineEventTarget(): OnlineEventTarget | null {
  const candidate = globalThis as Partial<OnlineEventTarget>
  if (typeof candidate.addEventListener !== 'function' || typeof candidate.removeEventListener !== 'function') {
    return null
  }
  return candidate as OnlineEventTarget
}

/**
 * Returns the network signal of the current runtime. Node.js and React Native report no network state, so there the
 * signal never reports offline and never calls back.
 *
 * @returns The signal.
 *
 * @internal
 */
export function deviceNetworkSignal(): NetworkSignal {
  return {
    reportsOffline: () => (globalThis as { navigator?: { onLine?: unknown } }).navigator?.onLine === false,
    watchOnline(onOnline) {
      const target = onlineEventTarget()
      if (target === null) return () => {}
      const listener = (): void => onOnline()
      const removeEventListener = target.removeEventListener.bind(target)
      target.addEventListener('online', listener)
      return () => removeEventListener('online', listener)
    },
  }
}
