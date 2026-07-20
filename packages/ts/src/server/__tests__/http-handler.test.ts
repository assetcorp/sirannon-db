import { describe, expect, it, vi } from 'vitest'
import { SirannonError } from '../../core/errors.js'
import type { Sirannon } from '../../core/sirannon.js'
import type { ServerExecutionTarget } from '../../core/types.js'
import { handleExecute, handleQuery, handleTransaction } from '../http-handler.js'
import { createMockResponse } from './helpers.js'

describe('http-handler status mapping and catch branches', () => {
  it('maps SirannonError codes to expected HTTP statuses for query', async () => {
    const scenarios = [
      { code: 'DATABASE_NOT_FOUND', expectedStatus: '404' },
      { code: 'READ_ONLY', expectedStatus: '403' },
      { code: 'HOOK_DENIED', expectedStatus: '403' },
      { code: 'TRANSACTION_ERROR', expectedStatus: '400' },
      { code: 'DATABASE_CLOSED', expectedStatus: '503' },
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
        resolve: () => Promise.resolve(db),
      } as unknown as Sirannon
      const handler = handleQuery(sirannon)

      await handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'SELECT 1' })), {
        aborted: false,
        onAbort: () => {},
      })
      expect(mock.state.status).toBe(scenario.expectedStatus)
    }
  })

  it('returns INTERNAL_ERROR for unexpected query errors', async () => {
    const mock = createMockResponse()
    const db = {
      query: () => {
        throw new Error('unexpected')
      },
    }
    const sirannon = {
      resolve: () => Promise.resolve(db),
    } as unknown as Sirannon
    const handler = handleQuery(sirannon)

    await handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'SELECT 1' })), {
      aborted: false,
      onAbort: () => {},
    })

    expect(mock.state.status).toBe('500')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INTERNAL_ERROR')
  })

  it('maps SirannonError in execute handler', async () => {
    const mock = createMockResponse()
    const db = {
      execute: () => {
        throw new SirannonError('readonly', 'READ_ONLY')
      },
    }
    const sirannon = {
      resolve: () => Promise.resolve(db),
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    await handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'INSERT INTO t VALUES (1)' })), {
      aborted: false,
      onAbort: () => {},
    })

    expect(mock.state.status).toBe('403')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('READ_ONLY')
  })

  it('returns INTERNAL_ERROR for unexpected execute errors', async () => {
    const mock = createMockResponse()
    const db = {
      execute: () => {
        throw new Error('unexpected')
      },
    }
    const sirannon = {
      resolve: () => Promise.resolve(db),
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    await handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'INSERT INTO t VALUES (1)' })), {
      aborted: false,
      onAbort: () => {},
    })

    expect(mock.state.status).toBe('500')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INTERNAL_ERROR')
  })

  it('returns INVALID_JSON for malformed execute body', async () => {
    const mock = createMockResponse()
    const sirannon = {
      resolve: () =>
        Promise.resolve({
          execute: () => ({ changes: 1, lastInsertRowId: 1 }),
        }),
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    await handler(mock.res, 'db1', Buffer.from('not json'), { aborted: false, onAbort: () => {} })

    expect(mock.state.status).toBe('400')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INVALID_JSON')
  })

  it('rejects invalid read concern before query execution', async () => {
    const mock = createMockResponse()
    const db = {
      query: () => {
        throw new Error('query should not run')
      },
    }
    const sirannon = {
      resolve: () => Promise.resolve(db),
    } as unknown as Sirannon
    const handler = handleQuery(sirannon)

    await handler(
      mock.res,
      'db1',
      Buffer.from(JSON.stringify({ sql: 'SELECT 1', readConcern: { level: 'eventual' } })),
      { aborted: false, onAbort: () => {} },
    )

    expect(mock.state.status).toBe('400')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INVALID_REQUEST')
  })

  it('rejects invalid write concern before execute', async () => {
    const mock = createMockResponse()
    const db = {
      execute: () => {
        throw new Error('execute should not run')
      },
    }
    const sirannon = {
      resolve: () => Promise.resolve(db),
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    await handler(
      mock.res,
      'db1',
      Buffer.from(
        JSON.stringify({ sql: 'INSERT INTO t VALUES (1)', writeConcern: { level: 'majority', timeoutMs: 0 } }),
      ),
      { aborted: false, onAbort: () => {} },
    )

    expect(mock.state.status).toBe('400')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INVALID_REQUEST')
  })

  it('returns INTERNAL_ERROR for unexpected transaction errors', async () => {
    const mock = createMockResponse()
    const db = {
      transaction: () => {
        throw new Error('unexpected')
      },
    }
    const sirannon = {
      resolve: () => Promise.resolve(db),
    } as unknown as Sirannon
    const handler = handleTransaction(sirannon)

    await handler(
      mock.res,
      'db1',
      Buffer.from(
        JSON.stringify({
          statements: [{ sql: 'UPDATE users SET name = ? WHERE id = ?', params: ['Alice', 1] }],
        }),
      ),
      { aborted: false, onAbort: () => {} },
    )

    expect(mock.state.status).toBe('500')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INTERNAL_ERROR')
  })

  it('returns INVALID_JSON for malformed transaction body', async () => {
    const mock = createMockResponse()
    const sirannon = {
      resolve: () =>
        Promise.resolve({
          transaction: () => [],
        }),
    } as unknown as Sirannon
    const handler = handleTransaction(sirannon)

    await handler(mock.res, 'db1', Buffer.from('not json'), { aborted: false, onAbort: () => {} })

    expect(mock.state.status).toBe('400')
    const body = JSON.parse(mock.state.body ?? '{}') as { error?: { code?: string } }
    expect(body.error?.code).toBe('INVALID_JSON')
  })
})

describe('http-handler execution target routing', () => {
  it('routes queries through the configured execution target', async () => {
    const mock = createMockResponse()
    const query = vi.fn(async (_sql: string, _params?: unknown, _options?: unknown) => [{ id: 1 }])
    const target: ServerExecutionTarget = {
      query: async <T = Record<string, unknown>>(...args: Parameters<ServerExecutionTarget['query']>) =>
        (await query(...args)) as T[],
      execute: vi.fn(),
      transaction: vi.fn(),
    }
    const sirannon = {
      resolve: vi.fn(async () => {
        throw new Error('raw database should not resolve')
      }),
    } as unknown as Sirannon
    const handler = handleQuery(sirannon, () => target)

    await handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'SELECT 1' })), {
      aborted: false,
      onAbort: () => {},
    })

    expect(query).toHaveBeenCalledWith('SELECT 1', undefined, undefined)
    expect(mock.state.status).toBe('200 OK')
    const body = JSON.parse(mock.state.body ?? '{}') as { rows?: unknown[] }
    expect(body.rows).toEqual([{ id: 1 }])
  })

  it('routes execute through the configured execution target with write concern', async () => {
    const mock = createMockResponse()
    const target: ServerExecutionTarget = {
      query: vi.fn(),
      execute: vi.fn(async () => ({ changes: 1, lastInsertRowId: 5n })),
      transaction: vi.fn(),
    }
    const sirannon = {
      resolve: vi.fn(async () => {
        throw new Error('raw database should not resolve')
      }),
    } as unknown as Sirannon
    const handler = handleExecute(sirannon, () => target)

    await handler(
      mock.res,
      'db1',
      Buffer.from(
        JSON.stringify({
          sql: 'INSERT INTO users (name) VALUES (?)',
          params: ['Alice'],
          writeConcern: { level: 'majority', timeoutMs: 1000 },
        }),
      ),
      { aborted: false, onAbort: () => {} },
    )

    expect(target.execute).toHaveBeenCalledWith('INSERT INTO users (name) VALUES (?)', ['Alice'], {
      writeConcern: { level: 'majority', timeoutMs: 1000 },
    })
    expect(mock.state.status).toBe('200 OK')
    const body = JSON.parse(mock.state.body ?? '{}') as { changes?: number; lastInsertRowId?: string }
    expect(body).toMatchObject({ changes: 1, lastInsertRowId: '5' })
  })

  it('routes transactions through the configured execution target with write concern', async () => {
    const mock = createMockResponse()
    const tx = {
      execute: vi.fn(async () => ({ changes: 1, lastInsertRowId: 0 })),
    }
    const target: ServerExecutionTarget = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(async fn => fn(tx as never)),
    }
    const sirannon = {
      resolve: vi.fn(async () => {
        throw new Error('raw database should not resolve')
      }),
    } as unknown as Sirannon
    const handler = handleTransaction(sirannon, () => target)

    await handler(
      mock.res,
      'db1',
      Buffer.from(
        JSON.stringify({
          statements: [{ sql: 'UPDATE users SET name = ?', params: ['Alice'] }],
          writeConcern: { level: 'majority' },
        }),
      ),
      { aborted: false, onAbort: () => {} },
    )

    expect(target.transaction).toHaveBeenCalledWith(expect.any(Function), { writeConcern: { level: 'majority' } })
    expect(tx.execute).toHaveBeenCalledWith('UPDATE users SET name = ?', ['Alice'])
    expect(mock.state.status).toBe('200 OK')
  })

  it('routes transactions through executeTransaction when the target groups them', async () => {
    const mock = createMockResponse()
    const target: ServerExecutionTarget = {
      query: vi.fn(),
      execute: vi.fn(),
      transaction: vi.fn(),
      executeTransaction: vi.fn(async () => [
        { changes: 1, lastInsertRowId: 7 },
        { changes: 2, lastInsertRowId: 0 },
      ]),
    }
    const sirannon = { resolve: vi.fn(async () => target) } as unknown as Sirannon
    const handler = handleTransaction(sirannon)

    await handler(
      mock.res,
      'db1',
      Buffer.from(
        JSON.stringify({
          statements: [
            { sql: 'INSERT INTO orders (ref) VALUES (?)', params: ['abc'] },
            { sql: 'UPDATE items SET qty = qty - 1 WHERE id = ?', params: [3] },
          ],
          writeConcern: { level: 'majority' },
        }),
      ),
      { aborted: false, onAbort: () => {} },
    )

    expect(target.executeTransaction).toHaveBeenCalledWith(
      [
        { sql: 'INSERT INTO orders (ref) VALUES (?)', params: ['abc'] },
        { sql: 'UPDATE items SET qty = qty - 1 WHERE id = ?', params: [3] },
      ],
      { writeConcern: { level: 'majority' } },
    )
    expect(target.transaction).not.toHaveBeenCalled()
    expect(mock.state.status).toBe('200 OK')
    const body = JSON.parse(mock.state.body ?? '{}') as { results?: unknown[] }
    expect(body.results).toHaveLength(2)
  })

  it('does not answer an aborted request when the target resolver rejects', async () => {
    const mock = createMockResponse()
    const sirannon = {
      resolve: vi.fn(async () => {
        throw new Error('resolver exploded')
      }),
    } as unknown as Sirannon
    const handler = handleExecute(sirannon)

    await handler(mock.res, 'db1', Buffer.from(JSON.stringify({ sql: 'SELECT 1' })), {
      aborted: true,
      onAbort: () => {},
    })

    expect(mock.state.status).toBeUndefined()
    expect(mock.state.body).toBeUndefined()
  })
})
