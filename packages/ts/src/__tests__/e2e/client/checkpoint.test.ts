import { existsSync, statSync } from 'node:fs'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { RequestDeniedError } from '../../../core/errors.js'
import { errorCodeOf, errorMessageOf, scratchDirectory, sendRaw } from '../lib/served-registry.js'
import {
  closeLater,
  DATABASE_ID,
  INSERT_ORDER,
  recordingProxy,
  releaseClientServers,
  servedShop,
  storedCustomers,
} from './harness.js'

const EMPTIED = { busy: false, framesInLog: 0, framesCheckpointed: 0, emptied: true }
const TRANSPORTS = ['http', 'websocket'] as const

afterEach(releaseClientServers)

function logBytes(path: string): number {
  const walPath = `${path}-wal`
  return existsSync(walPath) ? statSync(walPath).size : 0
}

function caught(promise: Promise<unknown>): Promise<{ name: string; code?: string; message: string }> {
  return promise.then(
    () => ({ name: 'resolved', message: '' }),
    (err: Error & { code?: string }) => ({ name: err.name, code: err.code, message: err.message }),
  )
}

describe.each(TRANSPORTS)('checkpoint through the published client over %s', transport => {
  it('empties the log on the server, reports busy while a reader holds it, and refuses the pragma through statements', async () => {
    const shop = await servedShop({ label: `checkpoint-${transport}` })
    const client = new SirannonClient(shop.served.base, { transport, autoReconnect: false })
    closeLater(() => client.close())
    const db = client.database(DATABASE_ID)
    await db.batch(INSERT_ORDER, [
      ['north', 30],
      ['south', 40],
    ])
    expect(logBytes(shop.path)).toBeGreaterThan(0)

    expect(await db.checkpoint()).toEqual(EMPTIED)
    expect(logBytes(shop.path)).toBe(0)

    const reader = new BetterSqlite3(shop.path, { readonly: true, fileMustExist: true })
    try {
      reader.exec('BEGIN')
      reader.prepare('SELECT count(*) FROM orders').get()
      await db.execute(INSERT_ORDER, ['east', 50])
      const held = await db.checkpoint()
      expect(held.busy).toBe(true)
      expect(held.emptied).toBe(false)
      expect(held.framesInLog).toBeGreaterThan(0)
      expect(logBytes(shop.path)).toBeGreaterThan(0)
      reader.exec('COMMIT')
    } finally {
      reader.close()
    }
    expect(await db.checkpoint()).toEqual(EMPTIED)
    expect(storedCustomers(shop.path)).toEqual(expect.arrayContaining(['north', 'south', 'east']))

    await db.execute(INSERT_ORDER, ['west', 60])
    const framesBefore = logBytes(shop.path)
    for (const sql of ['PRAGMA wal_checkpoint(TRUNCATE)', 'pragma "WAL_CHECKPOINT"', 'PRAGMA main.wal_checkpoint']) {
      const asQuery = await caught(db.query(sql))
      const asExecute = await caught(db.execute(sql))
      expect([asQuery.code, asExecute.code], sql).toEqual(['FORBIDDEN_SQL', 'FORBIDDEN_SQL'])
      expect(asQuery.message, sql).toContain('checkpoint()')
    }
    expect(logBytes(shop.path)).toBe(framesBefore)
  })

  it('refuses a checkpoint answer that holds no checkpoint result or contradicts itself', async () => {
    const shop = await servedShop({ label: `checkpoint-hostile-${transport}` })
    const proxy = await recordingProxy(shop.served.port)
    const client = new SirannonClient(proxy.base, { transport, autoReconnect: false })
    closeLater(() => client.close())
    const db = client.database(DATABASE_ID)
    const bodies = [
      '{}',
      'null',
      '[]',
      '{"busy":"no","framesInLog":0,"framesCheckpointed":0,"emptied":true}',
      '{"busy":false,"framesInLog":-1,"framesCheckpointed":0,"emptied":false}',
      '{"busy":false,"framesInLog":1.5,"framesCheckpointed":0,"emptied":false}',
      '{"busy":false,"framesInLog":1e300,"framesCheckpointed":0,"emptied":false}',
      '{"busy":false,"framesInLog":"4","framesCheckpointed":0,"emptied":false}',
      '{"busy":false,"framesInLog":3,"framesCheckpointed":4,"emptied":false}',
      '{"busy":true,"framesInLog":0,"framesCheckpointed":0,"emptied":true}',
      '{"busy":false,"framesInLog":9,"framesCheckpointed":0,"emptied":true}',
    ]

    for (const text of bodies) {
      proxy.answerWith(request =>
        request.path.endsWith('/checkpoint') ? { kind: 'reply', status: 200, text } : undefined,
      )
      const failure = await caught(db.checkpoint())
      expect([failure.name, failure.code], text).toEqual(['RemoteError', 'INVALID_RESPONSE'])
    }
    proxy.answerWith(() => undefined)
    expect(await db.checkpoint()).toEqual(EMPTIED)
    expect(proxy.requestsTo('POST', /\/db\/shop\/checkpoint$/)).toHaveLength(bodies.length + 1)
  })

  it('fails with TRANSPORT_ERROR when the client closes while the server has not answered, and sends nothing afterwards', async () => {
    const shop = await servedShop({ label: `checkpoint-closed-${transport}` })
    const proxy = await recordingProxy(shop.served.port)
    proxy.answerWith(request => (request.path.endsWith('/checkpoint') ? { kind: 'stall' } : undefined))
    const client = new SirannonClient(proxy.base, { transport, autoReconnect: false })
    const db = client.database(DATABASE_ID)

    const pending = caught(db.checkpoint())
    await expect.poll(() => proxy.requestsTo('POST', /\/db\/shop\/checkpoint$/).length).toBe(1)
    client.close()

    expect((await pending).code).toBe('TRANSPORT_ERROR')
    expect(proxy.requestsTo('POST', /\/db\/shop\/checkpoint$/)).toHaveLength(1)
    const queryAfterClose = await caught(db.query('SELECT 1'))
    const checkpointAfterClose = await caught(db.checkpoint())
    expect(queryAfterClose.name).not.toBe('resolved')
    expect([checkpointAfterClose.name, checkpointAfterClose.code]).toEqual([queryAfterClose.name, queryAfterClose.code])
    expect(proxy.requestsTo('POST', /\/db\/shop\/checkpoint$/)).toHaveLength(1)
  })

  it('fails with TIMEOUT when the server leaves a checkpoint unanswered, then accepts a later checkpoint', async () => {
    const shop = await servedShop({ label: `checkpoint-timeout-${transport}`, customers: [] })
    const proxy = await recordingProxy(shop.served.port)
    proxy.answerWith(request => (request.path.endsWith('/checkpoint') ? { kind: 'stall' } : undefined))
    const client = new SirannonClient(proxy.base, { transport, autoReconnect: false, requestTimeout: 1000 })
    closeLater(() => client.close())
    const db = client.database(DATABASE_ID)

    expect((await caught(db.checkpoint())).code).toBe('TIMEOUT')
    expect(proxy.requestsTo('POST', /\/db\/shop\/checkpoint$/)).toHaveLength(1)

    proxy.answerWith(() => undefined)
    expect(await db.checkpoint()).toEqual(EMPTIED)
    expect(logBytes(shop.path)).toBe(0)
    expect(storedCustomers(shop.path)).toEqual([])
  })
})

describe('the checkpoint route', () => {
  it('refuses malformed bodies, unknown and closed databases, and read-only databases, and accepts an empty body', async () => {
    const shop = await servedShop({ label: 'checkpoint-route' })
    const dir = scratchDirectory('checkpoint-route-extra')
    await shop.sirannon.open('archive', shop.path, { readOnly: true })
    await shop.sirannon.open('closing', join(dir, 'closing.db'))
    await shop.sirannon.close('closing')
    const base = shop.served.base

    const unknown = await sendRaw(base, 'POST', '/db/refunds/checkpoint', '{}')
    const closed = await sendRaw(base, 'POST', '/db/closing/checkpoint', '{}')
    const readOnly = await sendRaw(base, 'POST', '/db/archive/checkpoint', '{}')
    expect([unknown.status, errorCodeOf(unknown.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect([closed.status, errorCodeOf(closed.body)]).toEqual([404, 'DATABASE_NOT_FOUND'])
    expect([readOnly.status, errorCodeOf(readOnly.body)]).toEqual([403, 'READ_ONLY'])

    const bodies: Array<[body: string, code: string]> = [
      ['0', 'INVALID_REQUEST'],
      ['[]', 'INVALID_REQUEST'],
      ['"x"', 'INVALID_REQUEST'],
      ['null', 'INVALID_REQUEST'],
      ['true', 'INVALID_REQUEST'],
      ['{"busy":', 'INVALID_JSON'],
      ['busy=1', 'INVALID_JSON'],
    ]
    for (const [body, code] of bodies) {
      const answer = await sendRaw(base, 'POST', `/db/${DATABASE_ID}/checkpoint`, body)
      expect([answer.status, errorCodeOf(answer.body)], body).toEqual([400, code])
    }

    await shop.database.execute(INSERT_ORDER, ['north', 30])
    const empty = await sendRaw(base, 'POST', `/db/${DATABASE_ID}/checkpoint`)
    expect([empty.status, empty.body]).toEqual([200, EMPTIED])
    expect(logBytes(shop.path)).toBe(0)

    const pragma = await sendRaw(
      base,
      'POST',
      `/db/${DATABASE_ID}/execute`,
      '{"sql":"PRAGMA wal_checkpoint(TRUNCATE)"}',
    )
    expect([pragma.status, errorCodeOf(pragma.body)]).toEqual([403, 'FORBIDDEN_SQL'])
    expect(errorMessageOf(pragma.body)).toContain('checkpoint()')
  })

  it('runs the authenticate hook first, so a refused caller leaves the log in place, and serves an operator with statements turned off', async () => {
    const seen: string[] = []
    const shop = await servedShop({
      label: 'checkpoint-route-hook',
      server: {
        acceptSql: false,
        authenticate: ctx => {
          seen.push(`${ctx.method} ${ctx.path}`)
          if (ctx.headers.authorization !== 'Bearer operator') {
            throw new RequestDeniedError(403, 'HOOK_DENIED', 'Only an operator may checkpoint')
          }
          return { operator: true }
        },
      },
    })
    await shop.database.execute(INSERT_ORDER, ['north', 30])
    const held = logBytes(shop.path)
    const route = `/db/${DATABASE_ID}/checkpoint`

    const refused = await sendRaw(shop.served.base, 'POST', route, '{}', { Authorization: 'Bearer clerk' })
    expect([refused.status, errorCodeOf(refused.body)]).toEqual([403, 'HOOK_DENIED'])
    expect(logBytes(shop.path)).toBe(held)

    const accepted = await sendRaw(shop.served.base, 'POST', route, '{}', { Authorization: 'Bearer operator' })
    expect([accepted.status, accepted.body]).toEqual([200, EMPTIED])
    expect(logBytes(shop.path)).toBe(0)
    expect(seen.map(entry => entry.toUpperCase())).toEqual([
      `POST ${route}`.toUpperCase(),
      `POST ${route}`.toUpperCase(),
    ])
  })
})
