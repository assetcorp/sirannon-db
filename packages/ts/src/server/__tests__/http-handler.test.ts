import type { HttpResponse } from 'uWebSockets.js'
import { describe, expect, it } from 'vitest'
import { SirannonError } from '../../core/errors.js'
import type { Sirannon } from '../../core/sirannon.js'
import { handleExecute, handleQuery, handleTransaction, initAbortHandler, readBody } from '../http-handler.js'

interface MockResponseState {
  status: string | undefined
  headers: Record<string, string>
  body: string | undefined
}

function createMockResponse() {
  const state: MockResponseState = {
    status: undefined,
    headers: {},
    body: undefined,
  }

  let abortHandler: (() => void) | undefined
  let dataHandler: ((chunk: ArrayBuffer, isLast: boolean) => void) | undefined

  const res = {
    onAborted(fn: () => void) {
      abortHandler = fn
      return res
    },
    onData(fn: (chunk: ArrayBuffer, isLast: boolean) => void) {
      dataHandler = fn
      return res
    },
    cork(fn: () => void) {
      fn()
      return res
    },
    writeStatus(status: string) {
      state.status = status
      return res
    },
    writeHeader(name: string, value: string) {
      state.headers[name.toLowerCase()] = value
      return res
    },
    end(payload?: string) {
      state.body = payload ?? ''
      return res
    },
  }

  return {
    res: res as unknown as HttpResponse,
    state,
    abort() {
      abortHandler?.()
    },
    data(payload: string, isLast: boolean) {
      dataHandler?.(Buffer.from(payload), isLast)
    },
  }
}

describe('http-handler helpers', () => {
  it('notifies registered abort listeners and immediately invokes late listeners', () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)

    let firstCount = 0
    abort.onAbort(() => {
      firstCount++
    })

    mock.abort()
    expect(firstCount).toBe(1)
    expect(abort.aborted).toBe(true)

    let lateCount = 0
    abort.onAbort(() => {
      lateCount++
    })
    expect(lateCount).toBe(1)
  })

  it('readBody rejects when already aborted before reading starts', async () => {
    const mock = createMockResponse()
    const abort = {
      aborted: true,
      onAbort: () => {},
    }

    await expect(readBody(mock.res, 1024, abort)).rejects.toThrow('Request aborted')
  })

  it('readBody rejects when aborted after listener registration', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 1024, abort)

    mock.abort()

    await expect(pending).rejects.toThrow('Request aborted')
  })

  it('readBody returns 413 and rejects when payload exceeds limit', async () => {
    const mock = createMockResponse()
    const abort = initAbortHandler(mock.res)
    const pending = readBody(mock.res, 3, abort)

    mock.data('abcd', true)

    await expect(pending).rejects.toThrow('Payload too large')
    expect(mock.state.status).toBe('413')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('PAYLOAD_TOO_LARGE')
  })
})

describe('http-handler status mapping and catch branches', () => {
  it('maps SirannonError codes to expected HTTP statuses for query', () => {
    const scenarios = [
      { code: 'DATABASE_NOT_FOUND', expectedStatus: '404' },
      { code: 'READ_ONLY', expectedStatus: '403' },
      { code: 'HOOK_DENIED', expectedStatus: '403' },
      { code: 'SHUTDOWN', expectedStatus: '503' },
      { code: 'UNKNOWN_CODE', expectedStatus: '500' },
    ] as const

    for (const scenario of scenarios) {
      const mock = createMockResponse()
      const db = {
        query: () => {
          throw new SirannonError('failure', scenario.code as never)
        },
      }
      const sirannon = {
        get: () => db,
      } as unknown as Sirannon
      const handler = handleQuery(sirannon)

      handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'SELECT 1' })))
      expect(mock.state.status).toBe(scenario.expectedStatus)
    }
  })

  it('returns INTERNAL_ERROR for unexpected query errors', () => {
    const mock = createMockResponse()
    const db = {
      query: () => {
        throw new Error('unexpected')
      },
    }
    const sirannon = {
      get: () => db,
    } as unknown as Sirannon
    const handler = handleQuery(sirannon)

    handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'SELECT 1' })))

    expect(mock.state.status).toBe('500')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INTERNAL_ERROR')
  })

  it('maps SirannonError in execute handler', () => {
    const mock = createMockResponse()
    const db = {
      execute: () => {
        throw new SirannonError('readonly', 'READ_ONLY')
      },
    }
    const sirannon = {
      get: () => db,
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'INSERT INTO t VALUES (1)' })))

    expect(mock.state.status).toBe('403')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('READ_ONLY')
  })

  it('returns INTERNAL_ERROR for unexpected execute errors', () => {
    const mock = createMockResponse()
    const db = {
      execute: () => {
        throw new Error('unexpected')
      },
    }
    const sirannon = {
      get: () => db,
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'INSERT INTO t VALUES (1)' })))

    expect(mock.state.status).toBe('500')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INTERNAL_ERROR')
  })

  it('returns INTERNAL_ERROR for unexpected transaction errors', () => {
    const mock = createMockResponse()
    const db = {
      transaction: () => {
        throw new Error('unexpected')
      },
    }
    const sirannon = {
      get: () => db,
    } as unknown as Sirannon
    const handler = handleTransaction(sirannon)

    handler(
      mock.res,
      'db1',
      Buffer.from(
        JSON.stringify({
          statements: [{ sql: 'UPDATE users SET name = ? WHERE id = ?', params: ['Alice', 1] }],
        }),
      ),
    )

    expect(mock.state.status).toBe('500')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INTERNAL_ERROR')
  })
})
