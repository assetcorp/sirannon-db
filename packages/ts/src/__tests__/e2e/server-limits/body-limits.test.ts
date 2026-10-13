import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { RequestDeniedError, SirannonError } from '../../../core/errors.js'
import type { ServerOptions } from '../../../core/types.js'
import { createServer } from '../../../server/server.js'
import {
  connectRaw,
  countSqlite,
  errorCodeOf,
  httpRequestText,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  statusLinesIn,
  trackedRegistry,
  waitUntil,
  within,
} from '../lib/served-registry.js'

const DEFAULT_BODY_LIMIT = 1_048_576
const UINT32_MAX = 4_294_967_295
const INSERT_DOCUMENT = 'INSERT INTO documents (content) VALUES (?)'
const HOOK_DELAY_MS = 60
const VERDICT_HEADER = 'x-verdict'

let path: string

function insertBodyOfLength(bytes: number, label: string): string {
  const prefix = `{"sql":"${INSERT_DOCUMENT}","params":["${label}:`
  const suffix = '"]}'
  return `${prefix}${'x'.repeat(bytes - prefix.length - suffix.length)}${suffix}`
}

function insertFrameOfLength(bytes: number, label: string): string {
  const prefix = `{"type":"execute","id":"${label}","sql":"${INSERT_DOCUMENT}","params":["${label}:`
  const suffix = '"]}'
  return `${prefix}${'x'.repeat(bytes - prefix.length - suffix.length)}${suffix}`
}

function storedLabels(): string[] {
  return readSqlite<{ label: string }>(
    path,
    "SELECT substr(content, 1, instr(content, ':') - 1) AS label FROM documents ORDER BY id",
  ).map(row => row.label)
}

async function serveDocuments(options?: ServerOptions): Promise<ServedRegistry> {
  const sirannon = trackedRegistry()
  const db = await sirannon.open('docs', path)
  await db.execute('CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, content TEXT NOT NULL)')
  return serveRegistry(sirannon, { acceptSql: true, ...options })
}

async function sendOverRaw(served: ServedRegistry, request: string, settleMs: number) {
  const connection = await connectRaw(served.port)
  connection.socket.write(request)
  await waitUntil(() => statusLinesIn(connection.received()).length > 0 || connection.isClosed(), 'a response')
  await new Promise(resolve => setTimeout(resolve, settleMs))
  return connection
}

function codeOf(thrown: () => unknown): string | null {
  try {
    thrown()
    return null
  } catch (err) {
    return err instanceof SirannonError ? err.code : 'not a SirannonError'
  }
}

beforeEach(() => {
  path = join(scratchDirectory('body-limits'), 'docs.db')
})

afterEach(releaseServedRegistries)

describe('the size limits of a server', () => {
  it('refuses each limit that is not a positive integer or that uWebSockets.js would wrap, and serves at the largest limit it accepts', async () => {
    const sirannon = trackedRegistry()
    const invalid = [
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      UINT32_MAX + 1,
      5 * 2 ** 30,
      Number.MAX_SAFE_INTEGER,
    ]

    for (const value of invalid) {
      expect(
        codeOf(() => createServer(sirannon, { maxBodyBytes: value })),
        `maxBodyBytes ${value}`,
      ).toBe('INVALID_MAX_BODY_BYTES')
      expect(
        codeOf(() => createServer(sirannon, { maxWebSocketBackpressureBytes: value })),
        `maxWebSocketBackpressureBytes ${value}`,
      ).toBe('INVALID_WS_BACKPRESSURE')
    }
    expect(
      codeOf(() => createServer(sirannon, { maxBodyBytes: 2_097_152, maxWebSocketBackpressureBytes: 1_048_576 })),
    ).toBe('INVALID_WS_BACKPRESSURE')

    const largest = await serveDocuments({ maxBodyBytes: UINT32_MAX, maxWebSocketBackpressureBytes: UINT32_MAX })
    const twoMegabytes = insertBodyOfLength(2 * DEFAULT_BODY_LIMIT, 'largest')
    const answer = await sendRaw(largest.base, 'POST', '/db/docs/execute', twoMegabytes)
    const ws = await RawSocket.open(`${largest.wsBase}/db/docs`)
    const reply = await ws.request({
      id: 'frame',
      type: 'execute',
      sql: INSERT_DOCUMENT,
      params: [`frame:${'x'.repeat(2 * DEFAULT_BODY_LIMIT)}`],
    })

    expect([answer.status, answer.body]).toEqual([200, { changes: 1, lastInsertRowId: 1 }])
    expect(reply.data).toEqual({ changes: 1, lastInsertRowId: 2 })
    expect(storedLabels()).toEqual(['largest', 'frame'])
  })

  it('accepts a body of exactly the limit, answers one byte more with 413, and keeps serving on the same connection', async () => {
    for (const limit of [undefined, 2 * DEFAULT_BODY_LIMIT, 256]) {
      const bytes = limit ?? DEFAULT_BODY_LIMIT
      const served = await serveDocuments(limit === undefined ? {} : { maxBodyBytes: limit })
      const before = storedLabels()

      const atLimit = await sendRaw(served.base, 'POST', '/db/docs/execute', insertBodyOfLength(bytes, `at-${bytes}`))
      const pastLimit = await sendOverRaw(
        served,
        `${httpRequestText('POST', '/db/docs/execute', insertBodyOfLength(bytes + 1, `past-${bytes}`))}${httpRequestText('POST', '/db/docs/query', '{"sql":"SELECT count(*) AS n FROM documents"}')}`,
        100,
      )
      const afterwards = await postJson(served.base, '/db/docs/query', { sql: 'SELECT count(*) AS n FROM documents' })

      expect(atLimit.status, `${bytes} bytes`).toBe(200)
      expect(statusLinesIn(pastLimit.received()), `${bytes + 1} bytes`).toEqual([413, 200])
      expect(pastLimit.received(), `${bytes + 1} bytes`).toContain('"code":"PAYLOAD_TOO_LARGE"')
      expect(storedLabels(), `${bytes} bytes`).toEqual([...before, `at-${bytes}`])
      expect(afterwards.body).toEqual({ rows: [{ n: before.length + 1 }] })
      await served.server.close()
    }
  })

  it('serves a WebSocket message of exactly the limit and closes the connection with 1009 PAYLOAD_TOO_LARGE on one byte more', async () => {
    for (const limit of [undefined, 2 * DEFAULT_BODY_LIMIT, 256]) {
      const bytes = limit ?? DEFAULT_BODY_LIMIT
      const served = await serveDocuments(limit === undefined ? {} : { maxBodyBytes: limit })
      const before = storedLabels()
      const ws = await RawSocket.open(`${served.wsBase}/db/docs`)
      const doubled = await RawSocket.open(`${served.wsBase}/db/docs`)
      const farPast = await RawSocket.open(`${served.wsBase}/db/docs`)

      const atLimit = insertFrameOfLength(bytes, `at-${bytes}`)
      const accepted = ws.reply(`at-${bytes}`)
      ws.sendText(atLimit)
      expect((await accepted).type, `${bytes} bytes`).toBe('result')
      ws.sendText(insertFrameOfLength(bytes + 1, `past-${bytes}`))
      doubled.sendText(insertFrameOfLength(2 * bytes, `doubled-${bytes}`))
      farPast.sendText(insertFrameOfLength(2 * bytes + 1, `far-${bytes}`))
      const closed = await within(ws.closed, 'the oversized frame closing the connection')
      const closedDoubled = await within(doubled.closed, 'the doubled frame closing the connection')
      const closedFarPast = await within(farPast.closed, 'the frame past the ceiling closing the connection')
      const fresh = await RawSocket.open(`${served.wsBase}/db/docs`)
      const counted = await fresh.request({ id: 'count', type: 'query', sql: 'SELECT count(*) AS n FROM documents' })

      expect(Buffer.byteLength(atLimit)).toBe(bytes)
      expect(closed, `${bytes + 1} bytes`).toEqual({ code: 1009, reason: 'PAYLOAD_TOO_LARGE' })
      expect(closedDoubled, `${2 * bytes} bytes`).toEqual({ code: 1009, reason: 'PAYLOAD_TOO_LARGE' })
      expect(closedFarPast.code, `${2 * bytes + 1} bytes`).not.toBe(1000)
      expect(storedLabels(), `${bytes} bytes`).toEqual([...before, `at-${bytes}`])
      expect(counted.data).toEqual({ rows: [{ n: before.length + 1 }] })
      await served.server.close()
    }
  })

  it('answers a body over the limit exactly once while a slow authenticate hook decides, and keeps serving', async () => {
    const served = await serveDocuments({
      maxBodyBytes: 256,
      authenticate: async ctx => {
        const verdict = ctx.headers[VERDICT_HEADER]
        if (verdict === 'refuse-at-once') throw new RequestDeniedError(403, 'HOOK_DENIED', 'Refused before the body')
        await new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS))
        if (verdict === 'refuse') throw new RequestDeniedError(403, 'HOOK_DENIED', 'Refused after a pause')
        if (verdict === 'fail') throw new Error('The hook failed after a pause')
        return { user: 'clerk' }
      },
    })
    const oversized = insertBodyOfLength(1_024, 'oversized')

    for (const verdict of ['allow', 'refuse', 'fail']) {
      const connection = await sendOverRaw(
        served,
        httpRequestText('POST', '/db/docs/execute', oversized, { [VERDICT_HEADER]: verdict }),
        HOOK_DELAY_MS * 3,
      )
      expect(statusLinesIn(connection.received()), verdict).toEqual([413])
    }

    const refusedFirst = await connectRaw(served.port)
    const request = httpRequestText('POST', '/db/docs/execute', oversized, { [VERDICT_HEADER]: 'refuse-at-once' })
    refusedFirst.socket.write(request.slice(0, request.indexOf('\r\n\r\n') + 4))
    await waitUntil(() => statusLinesIn(refusedFirst.received()).length > 0, 'the refusal before the body')
    refusedFirst.socket.write(request.slice(request.indexOf('\r\n\r\n') + 4))
    await new Promise(resolve => setTimeout(resolve, HOOK_DELAY_MS))

    const allowed = await sendRaw(served.base, 'POST', '/db/docs/execute', insertBodyOfLength(200, 'allowed'), {
      [VERDICT_HEADER]: 'allow',
    })

    expect(statusLinesIn(refusedFirst.received())).toEqual([403])
    expect([allowed.status, errorCodeOf(allowed.body)]).toEqual([200, null])
    expect(storedLabels()).toEqual(['allowed'])
    expect(countSqlite(path, 'documents')).toBe(1)
  })
})
