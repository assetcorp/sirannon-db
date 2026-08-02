import type { GrpcReplicationTransport } from './transport.js'

export const RECONNECT_MIN_DELAY_MS = 250
export const RECONNECT_MAX_DELAY_MS = 5_000

type UnreffableTimeout = ReturnType<typeof setTimeout> & { unref?: () => void }

interface EndpointState {
  timers: Map<string, UnreffableTimeout>
  delays: Map<string, number>
  dialled: Map<string, () => void>
}

const stateByTransport = new WeakMap<GrpcReplicationTransport, EndpointState>()

function stateFor(transport: GrpcReplicationTransport): EndpointState {
  const existing = stateByTransport.get(transport)
  if (existing) return existing
  const created: EndpointState = { timers: new Map(), delays: new Map(), dialled: new Map() }
  stateByTransport.set(transport, created)
  return created
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
  if (state.timers.has(endpoint)) return

  const delayMs = state.delays.get(endpoint) ?? RECONNECT_MIN_DELAY_MS
  state.delays.set(endpoint, Math.min(delayMs * 2, RECONNECT_MAX_DELAY_MS))

  const timer = setTimeout(() => {
    state.timers.delete(endpoint)
    if (!transport.connected) return
    redial()
  }, delayMs) as UnreffableTimeout
  timer.unref?.()
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

  const aborts = [...state.dialled.values()]
  state.dialled.clear()
  for (const abort of aborts) {
    abort()
  }
}
