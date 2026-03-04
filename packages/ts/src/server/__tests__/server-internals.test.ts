import { afterEach, describe, expect, it, vi } from 'vitest'

interface MockAppState {
  posts: Record<string, (res: unknown, req: unknown) => void>
  wsConfig: Record<string, (ws: unknown, payload?: unknown, context?: unknown) => void> | null
}

interface ServerMocks {
  queryRouteHandler: ReturnType<typeof vi.fn>
  initAbortHandler: ReturnType<typeof vi.fn>
  readBody: ReturnType<typeof vi.fn>
  sendError: ReturnType<typeof vi.fn>
}

function createMockUws() {
  const state: MockAppState = {
    posts: {},
    wsConfig: null,
  }

  const app = {
    options() {
      return app
    },
    get() {
      return app
    },
    post(path: string, handler: (res: unknown, req: unknown) => void) {
      state.posts[path] = handler
      return app
    },
    ws(_path: string, config: Record<string, (ws: unknown, payload?: unknown, context?: unknown) => void>) {
      state.wsConfig = config
      return app
    },
    any() {
      return app
    },
    listen(_host: string, _port: number, cb: (socket: unknown) => void) {
      cb({ socket: true })
      return app
    },
  }

  const uws = {
    App: () => app,
    us_listen_socket_close: vi.fn(),
    us_socket_local_port: vi.fn(() => 1234),
  }

  return { state, uws }
}

async function loadServerModule(options?: {
  initAbortValue?: { aborted: boolean; onAbort: (fn: () => void) => void }
  readBodyImpl?: () => Promise<Buffer>
  sendErrorImpl?: (...args: unknown[]) => void
}) {
  vi.resetModules()
  const { state, uws } = createMockUws()
  const queryRouteHandler = vi.fn()
  const executeRouteHandler = vi.fn()
  const transactionRouteHandler = vi.fn()

  const initAbortHandler = vi.fn(() => {
    return (
      options?.initAbortValue ?? {
        aborted: false,
        onAbort: () => {},
      }
    )
  })
  const readBody = vi.fn(() => options?.readBodyImpl?.() ?? Promise.resolve(Buffer.from('{}')))
  const sendError = vi.fn((...args: unknown[]) => {
    options?.sendErrorImpl?.(...args)
  })

  vi.doMock('uWebSockets.js', () => ({
    default: uws,
    ...uws,
  }))
  vi.doMock('../http-handler.js', () => ({
    handleQuery: () => queryRouteHandler,
    handleExecute: () => executeRouteHandler,
    handleTransaction: () => transactionRouteHandler,
    initAbortHandler,
    readBody,
    sendError,
  }))

  const module = await import('../server.js')
  const mocks: ServerMocks = {
    queryRouteHandler,
    initAbortHandler,
    readBody,
    sendError,
  }
  return { state, mocks, module }
}

afterEach(() => {
  vi.doUnmock('uWebSockets.js')
  vi.doUnmock('../http-handler.js')
  vi.resetModules()
})

describe('server internals', () => {
  it('handles aborted upgrade without hook and missing path parameter', async () => {
    const { state, module } = await loadServerModule()
    const sirannon = {
      databases: () => new Map(),
      get: () => undefined,
    }
    module.createServer(sirannon as never, { port: 0 })
    const wsConfig = state.wsConfig
    expect(wsConfig).toBeTruthy()

    let upgraded = false
    const res = {
      onAborted(fn: () => void) {
        fn()
      },
      getRemoteAddressAsText() {
        return Buffer.from('127.0.0.1')
      },
      upgrade() {
        upgraded = true
      },
    }
    const req = {
      getParameter: () => undefined,
      getUrl: () => '/db',
      getMethod: () => 'get',
      getHeader: () => '',
      forEach: () => {},
    }

    wsConfig?.upgrade(res as never, req as never, {} as never)
    expect(upgraded).toBe(false)
  })

  it('ignores ws message and close callbacks when conn is missing', async () => {
    const { state, module } = await loadServerModule()
    const sirannon = {
      databases: () => new Map(),
      get: () => undefined,
    }
    module.createServer(sirannon as never, { port: 0 })
    const wsConfig = state.wsConfig
    expect(wsConfig).toBeTruthy()

    const ws = {
      getUserData: () => ({ databaseId: 'db', conn: undefined }),
    }

    expect(() => wsConfig?.message(ws as never, Buffer.from('test'))).not.toThrow()
    expect(() => wsConfig?.close(ws as never)).not.toThrow()
  })

  it('skips DB handler when request is marked aborted before body resolution', async () => {
    const { state, mocks, module } = await loadServerModule({
      initAbortValue: { aborted: true, onAbort: () => {} },
      readBodyImpl: () => Promise.resolve(Buffer.from('{}')),
    })
    const sirannon = {
      databases: () => new Map(),
      get: () => undefined,
    }
    module.createServer(sirannon as never, { port: 0 })

    const postQuery = state.posts['/db/:id/query']
    expect(postQuery).toBeTruthy()
    const req = {
      getParameter: () => undefined,
      getMethod: () => 'post',
      getUrl: () => '/db/query',
      forEach: () => {},
    }
    postQuery?.({} as never, req as never)
    await Promise.resolve()

    expect(mocks.initAbortHandler).toHaveBeenCalled()
    expect(mocks.readBody).toHaveBeenCalled()
    expect(mocks.queryRouteHandler).not.toHaveBeenCalled()
  })

  it('swallows readBody rejections without hook', async () => {
    const { state, mocks, module } = await loadServerModule({
      readBodyImpl: () => Promise.reject(new Error('body failed')),
    })
    const sirannon = {
      databases: () => new Map(),
      get: () => undefined,
    }
    module.createServer(sirannon as never, { port: 0 })

    const postQuery = state.posts['/db/:id/query']
    const req = {
      getParameter: () => 'db',
      getMethod: () => 'post',
      getUrl: () => '/db/db/query',
      forEach: () => {},
    }
    postQuery?.({} as never, req as never)
    await Promise.resolve()

    expect(mocks.queryRouteHandler).not.toHaveBeenCalled()
  })

  it('swallows Promise.all rejections when hook is enabled', async () => {
    const { state, mocks, module } = await loadServerModule({
      readBodyImpl: () => Promise.reject(new Error('body failed')),
    })
    const sirannon = {
      databases: () => new Map(),
      get: () => undefined,
    }
    module.createServer(sirannon as never, {
      port: 0,
      onRequest: () => undefined,
    })

    const postQuery = state.posts['/db/:id/query']
    const req = {
      getParameter: () => 'db',
      getMethod: () => 'post',
      getUrl: () => '/db/db/query',
      forEach: () => {},
    }
    postQuery?.({ getRemoteAddressAsText: () => Buffer.from('127.0.0.1') } as never, req as never)
    await Promise.resolve()

    expect(mocks.queryRouteHandler).not.toHaveBeenCalled()
  })

  it('swallows runOnRequest promise rejection in ws upgrade chain', async () => {
    const { state, mocks, module } = await loadServerModule({
      sendErrorImpl: () => {
        throw new Error('sendError failure')
      },
    })
    const sirannon = {
      databases: () => new Map(),
      get: () => undefined,
    }
    module.createServer(sirannon as never, {
      port: 0,
      onRequest: () => {
        throw new Error('hook failed')
      },
    })
    const wsConfig = state.wsConfig
    expect(wsConfig).toBeTruthy()

    let upgraded = false
    const res = {
      onAborted() {},
      getRemoteAddressAsText() {
        return Buffer.from('127.0.0.1')
      },
      upgrade() {
        upgraded = true
      },
    }
    const req = {
      getParameter: () => 'db',
      getUrl: () => '/db/db',
      getMethod: () => 'get',
      getHeader: () => '',
      forEach: () => {},
    }

    wsConfig?.upgrade(res as never, req as never, {} as never)
    await Promise.resolve()
    await Promise.resolve()

    expect(upgraded).toBe(false)
    expect(mocks.sendError).toHaveBeenCalled()
  })
})
