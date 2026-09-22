import { afterEach, describe, expect, it, vi } from 'vitest'
import { scheduleEndpointRedial, stopEndpointDialling } from '../../client-reconnect.js'
import type { GrpcReplicationTransport } from '../../transport.js'

const ENDPOINT = 'localhost:65001'

function fakeTransport(connected: boolean): GrpcReplicationTransport {
  return { connected } as GrpcReplicationTransport
}

afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

describe('endpoint redial', () => {
  it('holds the event loop open while a redial waits', () => {
    const transport = fakeTransport(true)
    const spy = vi.spyOn(globalThis, 'setTimeout')

    scheduleEndpointRedial(transport, ENDPOINT, () => {})

    const timer = spy.mock.results[0]?.value as { hasRef(): boolean }
    expect(timer.hasRef()).toBe(true)
    stopEndpointDialling(transport)
  })

  it('runs no redial once the transport stops dialling', async () => {
    vi.useFakeTimers()
    const transport = fakeTransport(true)
    const redial = vi.fn()

    scheduleEndpointRedial(transport, ENDPOINT, redial)
    stopEndpointDialling(transport)
    await vi.advanceTimersByTimeAsync(10_000)

    expect(redial).not.toHaveBeenCalled()
  })
})
