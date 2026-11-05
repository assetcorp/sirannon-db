import type { GrpcReplicationTransport } from './transport.js'

export const RECONNECT_MIN_DELAY_MS = 250
export const RECONNECT_MAX_DELAY_MS = 5_000

interface EndpointState {
  timers: Map<string, ReturnType<typeof setTimeout>>
  delays: Map<string, number>
  dialled: Map<string, () => void>
  wanted: Set<string> | null
}

const stateByTransport = new WeakMap<GrpcReplicationTransport, EndpointState>()

function stateFor(transport: GrpcReplicationTransport): EndpointState {
  const existing = stateByTransport.get(transport)
  if (existing) return existing
  const created: EndpointState = { timers: new Map(), delays: new Map(), dialled: new Map(), wanted: null }
  stateByTransport.set(transport, created)
  return created
}

function isWanted(state: EndpointState, endpoint: string): boolean {
  return state.wanted === null || state.wanted.has(endpoint)
}

/**
 * Returns the addresses that the transport still has to dial after it replaces the set of addresses that it keeps
 * connections to. The function closes every connection to an address outside the new set and cancels its redial.
 *
 * @param transport - The transport whose dialling the set controls.
 * @param endpoints - The replication addresses that the transport keeps connections to.
 * @returns The addresses in the set that the transport neither dials nor waits to redial.
 *
 * @internal
 */
export function replaceWantedEndpoints(transport: GrpcReplicationTransport, endpoints: Iterable<string>): string[] {
  const state = stateFor(transport)
  const wanted = new Set(endpoints)
  state.wanted = wanted
  for (const [endpoint, timer] of state.timers) {
    if (wanted.has(endpoint)) continue
    clearTimeout(timer)
    state.timers.delete(endpoint)
    state.delays.delete(endpoint)
  }
  const aborts: Array<() => void> = []
  for (const [endpoint, abort] of state.dialled) {
    if (!wanted.has(endpoint)) aborts.push(abort)
  }
  for (const abort of aborts) {
    abort()
  }
  return [...wanted].filter(endpoint => !state.dialled.has(endpoint) && !state.timers.has(endpoint))
}

export function claimEndpoint(transport: GrpcReplicationTransport, endpoint: string): boolean {
  const state = stateFor(transport)
  if (state.dialled.has(endpoint)) return false
  state.dialled.set(endpoint, () => undefined)
  return true
}

export function registerEndpointAbort(transport: GrpcReplicationTransport, endpoint: string, abort: () => void): void {
  const state = stateFor(transport)
  if (!state.dialled.has(endpoint)) return
  state.dialled.set(endpoint, abort)
}

export function releaseEndpoint(transport: GrpcReplicationTransport, endpoint: string): void {
  stateFor(transport).dialled.delete(endpoint)
}

export function noteEndpointReachable(transport: GrpcReplicationTransport, endpoint: string): void {
  stateFor(transport).delays.delete(endpoint)
}

export function scheduleEndpointRedial(
  transport: GrpcReplicationTransport,
  endpoint: string,
  redial: () => void,
): void {
  if (!transport.connected) return
  const state = stateFor(transport)
  if (state.timers.has(endpoint) || !isWanted(state, endpoint)) return

  const delayMs = state.delays.get(endpoint) ?? RECONNECT_MIN_DELAY_MS
  state.delays.set(endpoint, Math.min(delayMs * 2, RECONNECT_MAX_DELAY_MS))

  const timer = setTimeout(() => {
    state.timers.delete(endpoint)
    if (!transport.connected || !isWanted(state, endpoint)) return
    redial()
  }, delayMs)
  state.timers.set(endpoint, timer)
}

export function stopEndpointDialling(transport: GrpcReplicationTransport): void {
  const state = stateByTransport.get(transport)
  if (!state) return
  for (const timer of state.timers.values()) {
    clearTimeout(timer)
  }
  state.timers.clear()
  state.delays.clear()
  state.wanted = null

  const aborts = [...state.dialled.values()]
  state.dialled.clear()
  for (const abort of aborts) {
    abort()
  }
}
