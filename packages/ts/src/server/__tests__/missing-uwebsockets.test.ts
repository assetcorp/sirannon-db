import { afterEach, describe, expect, it, vi } from 'vitest'

afterEach(() => {
  vi.doUnmock('uWebSockets.js')
  vi.resetModules()
})

describe('server without uWebSockets.js', () => {
  it('builds the server and fails listen with SERVER_DEPENDENCY_MISSING naming the install command', async () => {
    vi.doMock('uWebSockets.js', () => {
      throw new Error("Cannot find package 'uWebSockets.js'")
    })
    vi.resetModules()

    const { createServer } = await import('../server.js')
    const server = createServer({ databases: () => new Map(), get: () => undefined } as never, { port: 0 })

    await expect(server.listen()).rejects.toMatchObject({
      code: 'SERVER_DEPENDENCY_MISSING',
      message: expect.stringContaining('pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"'),
    })
    expect(server.listeningPort).toBe(-1)
    await expect(server.close()).resolves.toBeUndefined()
  })
})
