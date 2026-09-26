import type { Server } from '@grpc/grpc-js'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { shutdownGrpcServer } from '../../server-shutdown.js'

afterEach(() => {
  vi.useRealTimers()
})

function fakeServer(tryShutdown: (callback: (err?: Error) => void) => void) {
  const forceShutdown = vi.fn()
  const server = { tryShutdown, forceShutdown } as unknown as Server
  return { server, forceShutdown }
}

describe('shutting a gRPC server down', () => {
  it('returns as soon as the graceful shutdown finishes', async () => {
    const { server, forceShutdown } = fakeServer(callback => callback())

    await shutdownGrpcServer(server, 5_000)

    expect(forceShutdown).not.toHaveBeenCalled()
  })

  it('forces the shutdown once a stream holds the graceful one past its deadline', async () => {
    vi.useFakeTimers()
    const { server, forceShutdown } = fakeServer(() => {})

    const shutdown = shutdownGrpcServer(server, 2_000)
    await vi.advanceTimersByTimeAsync(2_000)
    await shutdown

    expect(forceShutdown).toHaveBeenCalledTimes(1)
  })

  it('forces the shutdown when the graceful one reports a failure', async () => {
    const { server, forceShutdown } = fakeServer(callback => callback(new Error('server is not running')))

    await shutdownGrpcServer(server, 5_000)

    expect(forceShutdown).toHaveBeenCalledTimes(1)
  })
})
