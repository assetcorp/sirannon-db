import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonClient } from '../../../client/index.js'
import { HookDeniedError } from '../../../core/errors.js'
import type { Sirannon } from '../../../core/sirannon.js'
import { type OperationRegistry, operationRef, type ServerOptions } from '../../../core/types.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  readSqlite,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  sendRaw,
  serveRegistry,
  trackedRegistry,
  waitUntil,
} from '../lib/served-registry.js'

const STATEMENT_ROUTES = ['query', 'execute', 'transaction', 'batch', 'load'] as const
const SMUGGLED = "INSERT INTO members (name) VALUES ('mallory')"
const QUIET_PERIOD_MS = 150

const operations: OperationRegistry = {
  club: {
    reads: { everyMember: { statement: () => ({ sql: 'SELECT id, name FROM members ORDER BY id' }) } },
    writes: {
      addMember: {
        args: ['name'],
        statements: args => ({ sql: 'INSERT INTO members (name) VALUES (?)', params: [args.name] }),
      },
    },
  },
}

const allMembers = operationRef<Record<string, never>, { id: number; name: string }>('everyMember')

let dir: string
let path: string
let opened = 0

function members(): string[] {
  return readSqlite<{ name: string }>(path, 'SELECT name FROM members ORDER BY id').map(row => row.name)
}

function smugglingBody(): Record<string, unknown> {
  return { sql: SMUGGLED, params: [], paramsBatch: [[]], statements: [{ sql: SMUGGLED }] }
}

async function clubRegistry(options?: Parameters<typeof trackedRegistry>[0]): Promise<Sirannon> {
  const sirannon = trackedRegistry(options)
  path = join(dir, `club-${opened++}.db`)
  const db = await sirannon.open('club', path)
  await db.execute('CREATE TABLE members (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
  await db.execute('CREATE TABLE dues (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)')
  await db.execute("INSERT INTO members (name) VALUES ('ama')")
  return sirannon
}

async function serveClub(options?: ServerOptions): Promise<ServedRegistry> {
  return serveRegistry(await clubRegistry(), { operations, ...options })
}

async function capabilitiesOf(served: ServedRegistry): Promise<string[]> {
  const answer = await sendRaw(served.base, 'GET', '/capabilities')
  return (answer.body as { capabilities: string[] }).capabilities
}

beforeEach(() => {
  dir = scratchDirectory('sql-acceptance')
})

afterEach(releaseServedRegistries)

describe('a server that keeps SQL off', () => {
  it('refuses every statement route and message with SQL_NOT_ACCEPTED, by default and when told, and serves registered operations', async () => {
    for (const options of [{}, { acceptSql: false }]) {
      const served = await serveClub(options)
      const ws = await RawSocket.open(`${served.wsBase}/db/club`)

      for (const route of STATEMENT_ROUTES) {
        const answer = await postJson(served.base, `/db/club/${route}`, smugglingBody())
        const reply = await ws.request({ id: route, type: route, ...smugglingBody() })
        expect([answer.status, errorCodeOf(answer.body)], route).toEqual([403, 'SQL_NOT_ACCEPTED'])
        expect([reply.type, reply.error?.code], route).toEqual(['error', 'SQL_NOT_ACCEPTED'])
      }
      const named = await sendRaw(served.base, 'POST', '/db/club/query/everyMember')
      const namedOverWs = await ws.request({ id: 'named', type: 'query', name: 'everyMember' })
      const unknownRoute = await sendRaw(served.base, 'POST', '/db/club/nonsense', '{}')

      expect(named.body).toEqual({ rows: [{ id: 1, name: 'ama' }] })
      expect(namedOverWs.data).toEqual({ rows: [{ id: 1, name: 'ama' }] })
      expect([unknownRoute.status, errorCodeOf(unknownRoute.body)]).toEqual([404, 'NOT_FOUND'])
      expect(await capabilitiesOf(served)).toEqual(expect.arrayContaining(['query.named']))
      expect(await capabilitiesOf(served)).not.toContain('query.sql')
      expect(members()).toEqual(['ama'])
      await served.server.close()
    }
  })

  it('runs only the registered statement when a message or a body names an operation and also carries SQL', async () => {
    const served = await serveClub()
    const ws = await RawSocket.open(`${served.wsBase}/db/club`)

    const namedRead = await postJson(served.base, '/db/club/query/everyMember', smugglingBody())
    const namedWrite = await postJson(served.base, '/db/club/execute/addMember', {
      args: { name: 'kofi' },
      ...smugglingBody(),
    })
    const readOverWs = await ws.request({ id: 'read', type: 'query', name: 'everyMember', sql: SMUGGLED })
    const writeOverWs = await ws.request({
      id: 'write',
      type: 'execute',
      name: 'addMember',
      args: { name: 'esi' },
      sql: SMUGGLED,
    })
    const refusals = []
    for (const type of ['transaction', 'batch', 'load']) {
      refusals.push(await ws.request({ id: type, type, name: 'addMember', ...smugglingBody() }))
    }

    expect(namedRead.body).toEqual({ rows: [{ id: 1, name: 'ama' }] })
    expect(namedWrite.body).toEqual({ results: [{ changes: 1, lastInsertRowId: 2 }] })
    expect(readOverWs.data).toEqual({
      rows: [
        { id: 1, name: 'ama' },
        { id: 2, name: 'kofi' },
      ],
    })
    expect(writeOverWs.data).toEqual({ results: [{ changes: 1, lastInsertRowId: 3 }] })
    expect(refusals.map(reply => [reply.type, reply.error?.code])).toEqual([
      ['error', 'INVALID_MESSAGE'],
      ['error', 'INVALID_MESSAGE'],
      ['error', 'INVALID_MESSAGE'],
    ])
    expect(members()).toEqual(['ama', 'kofi', 'esi'])
  })

  it('refuses a table subscription and sends none of its changes, unless a subscribe hook admits that table', async () => {
    const closed = await serveClub()
    const refusedSocket = await RawSocket.open(`${closed.wsBase}/db/club`)
    const refused = await refusedSocket.request({ id: 'members', type: 'subscribe', table: 'members' })
    await postJson(closed.base, '/db/club/execute/addMember', { args: { name: 'kofi' } })
    await new Promise(resolve => setTimeout(resolve, QUIET_PERIOD_MS))

    expect([refused.type, refused.error?.code]).toEqual(['error', 'SQL_NOT_ACCEPTED'])
    expect(refusedSocket.messages.map(message => message.type)).toEqual(['error'])
    await closed.server.close()

    const admitted: string[] = []
    const gated = await clubRegistry({
      hooks: {
        onBeforeSubscribe: ({ table }) => {
          if (table !== 'members') throw new HookDeniedError('onBeforeSubscribe', `no stream for ${table}`)
          admitted.push(table)
        },
      },
    })
    const hooked = await serveRegistry(gated, { operations })
    const hookedSocket = await RawSocket.open(`${hooked.wsBase}/db/club`)
    const subscribed = await hookedSocket.request({ id: 'members', type: 'subscribe', table: 'members' })
    const denied = await hookedSocket.request({ id: 'dues', type: 'subscribe', table: 'dues' })
    const change = hookedSocket.reply('members')
    await postJson(hooked.base, '/db/club/execute/addMember', { args: { name: 'esi' } })

    expect(subscribed.type).toBe('subscribed')
    expect([denied.type, denied.error?.code]).toEqual(['error', 'HOOK_DENIED'])
    expect(await change).toMatchObject({
      type: 'change',
      event: { type: 'insert', table: 'members', row: { name: 'esi' } },
    })
    expect(admitted).toEqual(['members'])
  })

  it('stops the published client from sending SQL while it still calls registered operations', async () => {
    const served = await serveClub()

    for (const transport of ['http', 'websocket'] as const) {
      const client = new SirannonClient(served.base, { transport, autoReconnect: false })
      const club = client.database('club')

      await expect(club.query('SELECT * FROM members'), transport).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
      await expect(club.execute(SMUGGLED), transport).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
      await expect(club.batch(SMUGGLED, [[]]), transport).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
      expect(await club.query(allMembers, {}), transport).toEqual([{ id: 1, name: 'ama' }])
      client.close()
    }
    expect(members()).toEqual(['ama'])
  })
})

describe('a server that accepts SQL', () => {
  it('serves every statement route and message, announces query.sql, and serves table subscriptions', async () => {
    const served = await serveClub({ acceptSql: true })
    const ws = await RawSocket.open(`${served.wsBase}/db/club`)

    const read = await postJson(served.base, '/db/club/query', { sql: 'SELECT name FROM members' })
    const write = await ws.request({ id: 'write', type: 'execute', sql: "INSERT INTO members (name) VALUES ('kwame')" })
    const subscribed = await ws.request({ id: 'members', type: 'subscribe', table: 'members' })
    const batched = await postJson(served.base, '/db/club/batch', {
      sql: 'INSERT INTO members (name) VALUES (?)',
      paramsBatch: [['efua'], ['yaw']],
    })
    await waitUntil(() => ws.messages.filter(message => message.type === 'change').length >= 2, 'two change events')

    expect(read.body).toEqual({ rows: [{ name: 'ama' }] })
    expect(write.data).toEqual({ changes: 1, lastInsertRowId: 2 })
    expect(subscribed.type).toBe('subscribed')
    expect(batched.status).toBe(200)
    expect(
      ws.messages
        .filter(message => message.type === 'change')
        .map(message => {
          const event = message.event as { type: string; row: { name: string } }
          return [event.type, event.row.name]
        }),
    ).toEqual([
      ['insert', 'efua'],
      ['insert', 'yaw'],
    ])
    expect(await capabilitiesOf(served)).toEqual(expect.arrayContaining(['query.sql', 'query.named']))
    expect(members()).toEqual(['ama', 'kwame', 'efua', 'yaw'])
  })
})
