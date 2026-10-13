import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  connectRaw,
  countSqlite,
  httpRequestText,
  postJson,
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  statusLinesIn,
  trackedRegistry,
  waitUntil,
  within,
} from '../lib/served-registry.js'

const INSERT_VISIT = 'INSERT INTO visits (visitor) VALUES (?)'
const HOOK_DELAY_MS = 80
const ABANDONED_BATCH_ROWS = 100
const UPGRADE_REQUEST = [
  'GET /db/visits HTTP/1.1',
  'Host: 127.0.0.1',
  'Upgrade: websocket',
  'Connection: Upgrade',
  'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==',
  'Sec-WebSocket-Version: 13',
  '',
  '',
].join('\r\n')

let path: string

async function serveVisits(slowHook = false): Promise<ServedRegistry> {
  const sirannon = trackedRegistry()
  const db = await sirannon.open('visits', path)
  await db.execute('CREATE TABLE IF NOT EXISTS visits (id INTEGER PRIMARY KEY, visitor TEXT NOT NULL)')
  return serveRegistry(sirannon, {
    acceptSql: true,
    ...(slowHook
      ? {
          authenticate: async () => {
            await new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS))
            return { user: 'guide' }
          },
        }
      : {}),
  })
}

async function expectServing(served: ServedRegistry): Promise<void> {
  const answer = await postJson(served.base, '/db/visits/query', { sql: 'SELECT count(*) AS n FROM visits' })
  expect(answer.body).toEqual({ rows: [{ n: countSqlite(path, 'visits') }] })
  const ws = await RawSocket.open(`${served.wsBase}/db/visits`)
  const reply = await ws.request({ id: 'still', type: 'query', sql: 'SELECT count(*) AS n FROM visits' })
  expect(reply.data).toEqual({ rows: [{ n: countSqlite(path, 'visits') }] })
  ws.close()
}

beforeEach(() => {
  path = join(scratchDirectory('connection-lifecycle'), 'visits.db')
})

afterEach(releaseServedRegistries)

describe('connections that end early or never belonged', () => {
  it('sends DATABASE_NOT_FOUND and closes with 1008 to a WebSocket for a database that is not open', async () => {
    const served = await serveVisits()

    for (const id of ['refunds', 'VISITS', 'visits.db']) {
      const ws = await RawSocket.open(`${served.wsBase}/db/${id}`)
      const first = await ws.next()
      const closed = await within(ws.closed, `the socket for ${id} closing`)

      expect([first.type, first.id, first.error?.code], id).toEqual(['error', '', 'DATABASE_NOT_FOUND'])
      expect(closed, id).toEqual({ code: 1008, reason: 'Database not found' })
    }
    await expectServing(served)
  })

  it('answers DATABASE_CLOSED on a WebSocket whose database closes while the socket stays open', async () => {
    const served = await serveVisits()
    const ws = await RawSocket.open(`${served.wsBase}/db/visits`)
    const before = await ws.request({ id: 'before', type: 'execute', sql: INSERT_VISIT, params: ['ama'] })

    await served.sirannon.close('visits')
    const after = await ws.request({ id: 'after', type: 'execute', sql: INSERT_VISIT, params: ['kofi'] })
    const overHttp = await postJson(served.base, '/db/visits/execute', { sql: INSERT_VISIT, params: ['esi'] })

    expect(before.type).toBe('result')
    expect([after.type, after.error?.code]).toEqual(['error', 'DATABASE_CLOSED'])
    expect(overHttp.status).toBe(404)
    expect(countSqlite(path, 'visits')).toBe(1)
  })

  it('keeps serving after clients drop a WebSocket upgrade halfway, with and without a slow authenticate hook', async () => {
    for (const slowHook of [false, true]) {
      const served = await serveVisits(slowHook)
      for (let attempt = 0; attempt < 5; attempt++) {
        const connection = await connectRaw(served.port)
        connection.socket.write(attempt % 2 === 0 ? UPGRADE_REQUEST : UPGRADE_REQUEST.slice(0, 40))
        connection.socket.destroy()
      }
      await new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS * 2))

      await expectServing(served)
      await within(served.server.close(), 'closing after the dropped upgrades')
    }
  })

  it('keeps serving after malformed HTTP and refuses an unreadable length with 400', async () => {
    const served = await serveVisits()
    const malformed = [
      'GARBAGE\r\n\r\n',
      'POST /db/visits/query HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n{}',
      'POST /db/visits/query HTTP/1.1\r\nHost: x\r\nContent-Length: -5\r\n\r\n{}',
      'POST /db/visits/query HTTP/1.1\r\nthis header has no colon\r\n\r\n',
      `POST /db/visits/query HTTP/1.1\r\nHost: x\r\nX-Padding: ${'p'.repeat(64 * 1024)}\r\n\r\n`,
      'POST /db/visits/execute HTTP/9.9\r\nHost: x\r\nContent-Length: 2\r\n\r\n{}',
      '\u0000\u0001\u0002\u0003\r\n\r\n',
    ]
    const answers: string[] = []

    for (const request of malformed) {
      const connection = await connectRaw(served.port)
      connection.socket.write(request)
      await new Promise(resolve => setTimeout(resolve, 50))
      answers.push(connection.received())
      connection.socket.destroy()
    }

    expect(statusLinesIn(answers[1] ?? '')).toEqual([400])
    for (const answer of answers) {
      for (const status of statusLinesIn(answer)) expect(status, answer.slice(0, 80)).toBeGreaterThanOrEqual(400)
    }
    expect(countSqlite(path, 'visits')).toBe(0)
    await expectServing(served)
  })

  it('writes nothing for a request whose client leaves mid-body or while the hook decides, and still closes promptly', async () => {
    const served = await serveVisits(true)
    const body = JSON.stringify({ sql: INSERT_VISIT, params: ['ghost'] })

    const midBody = await connectRaw(served.port)
    const request = httpRequestText('POST', '/db/visits/execute', body)
    midBody.socket.write(request.slice(0, request.length - 10))
    await new Promise(resolve => setTimeout(resolve, 20))
    midBody.socket.destroy()

    const duringHook = await connectRaw(served.port)
    duringHook.socket.write(request)
    await new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS / 4))
    duringHook.socket.destroy()
    await new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS * 2))

    expect(countSqlite(path, 'visits')).toBe(0)
    await expectServing(served)
    await within(served.server.close(), 'closing after the abandoned requests')
  })

  it('commits a batch whole or not at all when its WebSocket client disconnects right after sending it', async () => {
    const served = await serveVisits()

    for (let round = 0; round < 3; round++) {
      const ws = await RawSocket.open(`${served.wsBase}/db/visits`)
      ws.sendText(
        JSON.stringify({
          id: `abandoned-${round}`,
          type: 'batch',
          sql: INSERT_VISIT,
          paramsBatch: Array.from({ length: ABANDONED_BATCH_ROWS }, (_, index) => [`round-${round}-${index}`]),
        }),
      )
      ws.close()
      await within(ws.closed, 'the abandoned socket closing')
    }
    await waitUntil(() => countSqlite(path, 'visits') % ABANDONED_BATCH_ROWS === 0, 'whole batches')

    expect(countSqlite(path, 'visits') % ABANDONED_BATCH_ROWS).toBe(0)
    await expectServing(served)
  })
})
