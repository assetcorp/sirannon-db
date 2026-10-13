import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { HookDeniedError, RequestDeniedError, SirannonError } from '../../../core/errors.js'
import type { Migration } from '../../../core/migrations/types.js'
import {
  armControlStatementFault,
  type ControlStatementFault,
  controlStatementFaultArmed,
  controlStatementFaultDriver,
} from '../../helpers/fault-drivers/control-statement-fault-driver.js'
import {
  countSqlite,
  errorCodeOf,
  postJson,
  RawSocket,
  releaseServedRegistries,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
  waitUntil,
} from '../lib/served-registry.js'

const HIDDEN_DETAIL = 'the ledger password is hunter2'
const GENERIC_MESSAGE = 'An unexpected error occurred'

let dir: string

beforeEach(() => {
  dir = scratchDirectory('error-statuses')
})

afterEach(releaseServedRegistries)

describe('the status of each error', () => {
  it('maps every error that a request causes to its status, with the same code over WebSocket', async () => {
    const sirannon = trackedRegistry()
    sirannon.onBeforeQuery(ctx => {
      if (ctx.sql.includes('/* denied */')) throw new HookDeniedError('onBeforeQuery', 'payroll is closed')
      if (ctx.sql.includes('/* teapot */')) throw new RequestDeniedError(418, 'TEAPOT', 'The ledger brews tea')
      if (ctx.sql.includes('/* locked */')) throw new SirannonError('The ledger is locked for audit', 'LEDGER_LOCKED')
      if (ctx.sql.includes('/* broken */')) throw new Error(HIDDEN_DETAIL)
    })
    const orders = await sirannon.open('orders', join(dir, 'orders.db'))
    await orders.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')
    const setup = await sirannon.open('archive-setup', join(dir, 'archive.db'))
    await setup.execute('CREATE TABLE entries (id INTEGER PRIMARY KEY)')
    await sirannon.close('archive-setup')
    await sirannon.open('archive', join(dir, 'archive.db'), { readOnly: true })
    const stale = await sirannon.open('stale', join(dir, 'stale.db'))
    await stale.close()
    const served = await serveRegistry(sirannon, {
      acceptSql: true,
      resolveExecutionTarget: id => (id === 'stale' ? stale : sirannon.resolve(id)),
    })
    const ordersSocket = await RawSocket.open(`${served.wsBase}/db/orders`)
    const archiveSocket = await RawSocket.open(`${served.wsBase}/db/archive`)

    const cases: Array<[label: string, database: string, route: string, body: string, status: number, code: string]> = [
      ['a database that is not open', 'refunds', 'query', '{"sql":"SELECT 1"}', 404, 'DATABASE_NOT_FOUND'],
      [
        'a write to a read-only database',
        'archive',
        'execute',
        '{"sql":"INSERT INTO entries DEFAULT VALUES"}',
        403,
        'READ_ONLY',
      ],
      ['a statement that a hook denies', 'orders', 'query', '{"sql":"SELECT 1 /* denied */"}', 403, 'HOOK_DENIED'],
      ['an internal table', 'orders', 'query', '{"sql":"SELECT * FROM _sirannon_meta"}', 403, 'FORBIDDEN_SQL'],
      ['a hook that sets its own status', 'orders', 'query', '{"sql":"SELECT 1 /* teapot */"}', 418, 'TEAPOT'],
      ['a code with no status of its own', 'orders', 'query', '{"sql":"SELECT 1 /* locked */"}', 500, 'LEDGER_LOCKED'],
      [
        'an error that is not a SirannonError',
        'orders',
        'execute',
        '{"sql":"SELECT 1 /* broken */"}',
        500,
        'INTERNAL_ERROR',
      ],
      [
        'a constraint that fails',
        'orders',
        'execute',
        '{"sql":"INSERT INTO orders (total) VALUES (NULL)"}',
        400,
        'QUERY_ERROR',
      ],
      ['body text that is not JSON', 'orders', 'query', '{"sql":', 400, 'INVALID_JSON'],
      ['an empty body', 'orders', 'query', '', 400, 'EMPTY_BODY'],
      [
        'an unknown read concern',
        'orders',
        'query',
        '{"sql":"SELECT 1","readConcern":{"level":"eventual"}}',
        400,
        'INVALID_REQUEST',
      ],
      [
        'a write concern that waits for no time',
        'orders',
        'execute',
        '{"sql":"SELECT 1","writeConcern":{"level":"majority","timeoutMs":0}}',
        400,
        'INVALID_REQUEST',
      ],
      ['a database closed behind its execution target', 'stale', 'query', '{"sql":"SELECT 1"}', 503, 'DATABASE_CLOSED'],
    ]

    for (const [label, database, route, body, status, code] of cases) {
      const answer = await sendRaw(served.base, 'POST', `/db/${database}/${route}`, body)
      expect([answer.status, errorCodeOf(answer.body)], label).toEqual([status, code])
      expect(answer.headers.get('content-type'), label).toBe('application/json')
      expect(Object.keys((answer.body as { error: object }).error).sort(), label).toEqual(['code', 'message'])
      if (code === 'INTERNAL_ERROR') {
        expect((answer.body as { error: { message: string } }).error.message, label).toBe(GENERIC_MESSAGE)
        expect(answer.text, label).not.toContain('hunter2')
      }

      const socket = database === 'archive' ? archiveSocket : database === 'orders' ? ordersSocket : null
      if (
        socket === null ||
        body.startsWith('{"sql":') === false ||
        code === 'INVALID_REQUEST' ||
        code === 'INVALID_JSON'
      )
        continue
      const reply = await socket.request({ id: label, type: route, ...JSON.parse(body) })
      expect([reply.type, reply.error?.code], `${label} over WebSocket`).toEqual(['error', code])
      if (code === 'INTERNAL_ERROR') expect(reply.error?.message, label).toBe(GENERIC_MESSAGE)
    }

    const success = await postJson(served.base, '/db/orders/query', { sql: 'SELECT count(*) AS n FROM orders' })
    expect([success.status, success.headers.get('content-type'), success.body]).toEqual([
      200,
      'application/json',
      { rows: [{ n: 0 }] },
    ])
  })

  it('answers 500 INTERNAL_ERROR when the disk refuses to begin a transaction and 400 TRANSACTION_ERROR when it refuses the commit', async () => {
    const faults: Array<[fault: ControlStatementFault, status: number, code: string, rowsKept: number]> = [
      ['begin-refused', 500, 'INTERNAL_ERROR', 0],
      ['commit-applies-then-fails', 400, 'TRANSACTION_ERROR', 2],
    ]

    for (const [fault, status, code, rowsKept] of faults) {
      const config = { fault, armFile: join(dir, `${fault}.armed`) }
      const path = join(dir, `${fault}.db`)
      const sirannon = trackedRegistry({ driver: controlStatementFaultDriver(config) })
      const db = await sirannon.open('parcels', path)
      await db.execute('CREATE TABLE parcels (id INTEGER PRIMARY KEY, label TEXT NOT NULL)')
      const served = await serveRegistry(sirannon, { acceptSql: true })
      const ws = await RawSocket.open(`${served.wsBase}/db/parcels`)
      const body = { sql: 'INSERT INTO parcels (label) VALUES (?)', paramsBatch: [['north'], ['south']] }

      armControlStatementFault(config)
      const answer = await postJson(served.base, '/db/parcels/batch', body)
      const keptAfterHttp = countSqlite(path, 'parcels')
      armControlStatementFault(config)
      const reply = await ws.request({ id: fault, type: 'batch', ...body })
      const afterwards = await postJson(served.base, '/db/parcels/batch', body)

      expect([answer.status, errorCodeOf(answer.body)], fault).toEqual([status, code])
      expect([reply.type, reply.error?.code], fault).toEqual(['error', code])
      expect(controlStatementFaultArmed(config), fault).toBe(false)
      expect(keptAfterHttp, fault).toBe(rowsKept)
      expect(afterwards.status, fault).toBe(200)
      expect(countSqlite(path, 'parcels'), fault).toBe(rowsKept * 2 + 2)
    }
  })

  it('answers a path that matches no route with 404 NOT_FOUND, whatever the method', async () => {
    const sirannon = trackedRegistry()
    await sirannon.open('orders', join(dir, 'orders.db'))
    const served = await serveRegistry(sirannon, { acceptSql: true })
    const misses: Array<[method: string, path: string]> = [
      ['GET', '/nonexistent'],
      ['GET', '/'],
      ['POST', '/db/orders/nonsense'],
      ['POST', '/db/orders'],
      ['GET', '/db/orders/query'],
      ['PUT', '/db/orders/execute'],
      ['DELETE', '/health'],
      ['POST', '/db/orders/query/extra/segment'],
    ]

    for (const [method, path] of misses) {
      const answer = await sendRaw(served.base, method, path, method === 'GET' ? undefined : '{}')
      expect([answer.status, errorCodeOf(answer.body)], `${method} ${path}`).toEqual([404, 'NOT_FOUND'])
    }
  })

  it('answers 503 SHUTDOWN to a request that opens a database while the registry shuts down', async () => {
    let entered = false
    let release: () => void = () => {}
    const gate = new Promise<void>(resolve => {
      release = resolve
    })
    const migrations = async (): Promise<Migration[]> => {
      entered = true
      await gate
      return []
    }
    const sirannon = trackedRegistry({
      migrations,
      lifecycle: { autoOpen: { resolver: id => ({ path: join(dir, `${id}.db`) }) } },
    })
    const served = await serveRegistry(sirannon, { acceptSql: true })

    const pending = postJson(served.base, '/db/tenant-a/query', { sql: 'SELECT 1' })
    await waitUntil(() => entered, 'the request opening the tenant database')
    const shuttingDown = sirannon.shutdown()
    release()
    const answer = await pending
    await shuttingDown

    expect([answer.status, errorCodeOf(answer.body)]).toEqual([503, 'SHUTDOWN'])
    const afterwards = await postJson(served.base, '/db/tenant-b/query', { sql: 'SELECT 1' })
    expect([afterwards.status, errorCodeOf(afterwards.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
  })
})
