import { createServer as createHttpServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { LiveQueryState } from '../../core/live/types.js'
import type { OperationArguments, OperationRegistry } from '../../core/operation-registry.js'
import { operationRef } from '../../core/operation-registry.js'
import type { BulkLoadDurability, Params } from '../../core/types.js'
import { SirannonClient } from '../client.js'
import { RemoteDatabase } from '../database-proxy.js'
import { ServerCapabilities, type ServerCapabilityCheck, SQL_REFUSED_MESSAGE } from '../server-capabilities.js'
import type { RemoteSubscription, Transport } from '../types.js'
import { RemoteError } from '../types.js'
import { createClientServerHarness } from './server-harness.js'

interface Member {
  id: number
  name: string
}

const members = operationRef<Record<string, never>, Member>('members')
const membersCalled = operationRef<{ name: string }, Member>('membersCalled')
const addMember = operationRef<{ name: string }, never>('addMember')

const operations: OperationRegistry = {
  testdb: {
    reads: {
      members: { statement: () => ({ sql: 'SELECT id, name FROM users ORDER BY id' }) },
      membersCalled: {
        args: ['name'],
        statement: (args: OperationArguments) => ({
          sql: 'SELECT id, name FROM users WHERE name = ? ORDER BY id',
          params: [args.name],
        }),
      },
    },
    writes: {
      addMember: {
        args: ['name'],
        statements: (args: OperationArguments) => ({
          sql: 'INSERT INTO users (name) VALUES (?)',
          params: [args.name],
        }),
      },
    },
  },
}

class RecordingTransport implements Transport {
  readonly sent: string[] = []

  async query(sql: string): Promise<never> {
    this.sent.push(sql)
    throw new Error('The statement reached the transport')
  }
  async execute(sql: string): Promise<never> {
    this.sent.push(sql)
    throw new Error('The statement reached the transport')
  }
  async transaction(statements: { sql: string }[]): Promise<never> {
    this.sent.push(statements[0].sql)
    throw new Error('The statement reached the transport')
  }
  async batch(sql: string): Promise<never> {
    this.sent.push(sql)
    throw new Error('The statement reached the transport')
  }
  async load(sql: string, _paramsBatch: Params[], _durability?: BulkLoadDurability): Promise<never> {
    this.sent.push(sql)
    throw new Error('The statement reached the transport')
  }
  async queryNamed(): Promise<never> {
    throw new Error('not used')
  }
  async executeNamed(): Promise<never> {
    throw new Error('not used')
  }
  async liveSubscribe(): Promise<RemoteSubscription> {
    throw new Error('not used')
  }
  async subscribe(): Promise<RemoteSubscription> {
    throw new Error('not used')
  }
  close(): void {}
}

function refusesSql(): ServerCapabilityCheck {
  return {
    assertSqlAccepted: async () => {
      throw new RemoteError('SQL_NOT_ACCEPTED', SQL_REFUSED_MESSAGE)
    },
    registryDigest: async () => undefined,
  }
}

function acceptsSql(): ServerCapabilityCheck {
  return {
    assertSqlAccepted: async () => undefined,
    registryDigest: async () => undefined,
  }
}

async function settle(ms = 60): Promise<void> {
  await new Promise(resolve => setTimeout(resolve, ms))
}

async function waitForRows(read: () => LiveQueryState<Member>, count: number, timeout = 4000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeout) {
    const state = read()
    if (state.status === 'ready' && state.rows.length === count && !state.revalidating) return
    if (state.status === 'error') throw state.error
    await settle(10)
  }
  throw new Error(`The live query never held ${count} rows`)
}

describe('a client whose server refuses SQL', () => {
  it('never hands the statement to the transport', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('testdb', transport, refusesSql())

    await expect(db.query('SELECT 1')).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await expect(db.execute('DELETE FROM users')).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await expect(db.transaction([{ sql: 'DELETE FROM users' }])).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await expect(db.batch('DELETE FROM users', [[]])).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await expect(db.load('DELETE FROM users', [[]])).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await expect(db.loadAll('DELETE FROM users', [[]])).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })

    expect(transport.sent).toEqual([])
  })
})

describe('a read concern the transport cannot carry', () => {
  it('refuses the read rather than serving it at another level', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('testdb', transport, acceptsSql())

    await expect(db.query('SELECT 1', [], { readConcern: { level: 'linearizable' } })).rejects.toMatchObject({
      code: 'INVALID_ARGUMENT',
    })
    await expect(db.query(members, {}, { readConcern: { level: 'linearizable' } })).rejects.toMatchObject({
      code: 'INVALID_ARGUMENT',
    })

    expect(transport.sent).toEqual([])
  })

  it('serves the read when the transport carries it', async () => {
    const transport = new RecordingTransport()
    const carrying = Object.assign(transport, { carriesReadConcern: true as const })
    const db = new RemoteDatabase('testdb', carrying, acceptsSql())

    await expect(db.query('SELECT 1', [], { readConcern: { level: 'linearizable' } })).rejects.toThrow(
      /reached the transport/,
    )
    expect(transport.sent).toEqual(['SELECT 1'])
  })
})

describe('a server whose capabilities endpoint answers 404', () => {
  let notFoundServer: Server
  let notFoundUrl: string

  beforeEach(async () => {
    notFoundServer = createHttpServer((_req, res) => {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'Not found' } }))
    })
    await new Promise<void>(resolve => notFoundServer.listen(0, '127.0.0.1', resolve))
    const address = notFoundServer.address() as AddressInfo
    notFoundUrl = `http://127.0.0.1:${address.port}`
  })

  afterEach(async () => {
    await new Promise<void>(resolve => notFoundServer.close(() => resolve()))
  })

  it('refuses SQL rather than assuming the server accepts it', async () => {
    const capabilities = new ServerCapabilities(() => notFoundUrl, undefined, 2000)

    await expect(capabilities.assertSqlAccepted()).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    await expect(capabilities.assertSqlAccepted()).rejects.toThrow(/cannot confirm/i)
  })

  it('reports no registry digest so a live query still subscribes', async () => {
    const capabilities = new ServerCapabilities(() => notFoundUrl, undefined, 2000)
    expect(await capabilities.registryDigest()).toBeUndefined()
  })

  it('keeps the statement inside the client', async () => {
    const transport = new RecordingTransport()
    const db = new RemoteDatabase('testdb', transport, new ServerCapabilities(() => notFoundUrl, undefined, 2000))

    await expect(db.query('SELECT 1')).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    expect(transport.sent).toEqual([])
  })
})

describe.each(['websocket', 'http'] as const)('registered operations over the %s transport', transport => {
  const harness = createClientServerHarness()

  it('reads and writes by name, and refuses a statement', async () => {
    await harness.restart({ acceptSql: false, operations })
    const client = new SirannonClient(harness.baseUrl, { transport })
    const db = client.database('testdb')

    try {
      expect(await db.query(members, {})).toEqual([{ id: 1, name: 'Alice' }])

      const written = await db.execute(addMember, { name: 'Bilal' })
      expect(written[0].changes).toBe(1)

      expect(await db.query(membersCalled, { name: 'Bilal' })).toEqual([{ id: 2, name: 'Bilal' }])
      await expect(db.query('SELECT * FROM users')).rejects.toMatchObject({ code: 'SQL_NOT_ACCEPTED' })
    } finally {
      client.close()
    }
  })

  it('carries a per-call read concern to the server', async () => {
    await harness.restart({ acceptSql: false, operations })
    const client = new SirannonClient(harness.baseUrl, { transport })
    const db = client.database('testdb')

    try {
      expect(await db.query(members, {}, { readConcern: { level: 'local' } })).toEqual([{ id: 1, name: 'Alice' }])
      await expect(db.query(members, {}, { readConcern: { level: 'nonsense' } as never })).rejects.toMatchObject({
        code: transport === 'websocket' ? 'INVALID_MESSAGE' : 'INVALID_REQUEST',
      })
    } finally {
      client.close()
    }
  })
})

describe('a live query against a remote database', () => {
  const harness = createClientServerHarness()

  it('holds the rows the server sends and applies what follows', async () => {
    await harness.restart({ acceptSql: false, operations })
    const client = new SirannonClient(harness.baseUrl)
    const db = client.database('testdb')

    try {
      const live = await db.live(members, {})
      const seen: number[] = []
      live.subscribe(() => {
        const state = live.getState()
        if (state.status === 'ready') seen.push(state.rows.length)
      })

      expect(live.getState()).toMatchObject({ status: 'ready', rows: [{ id: 1, name: 'Alice' }] })

      await db.execute(addMember, { name: 'Bilal' })
      await waitForRows(() => live.getState(), 2)

      const state = live.getState()
      expect(state.status === 'ready' && state.rows[1]).toEqual({ id: 2, name: 'Bilal' })
      expect(seen).toContain(2)

      await live.close()
      await db.execute(addMember, { name: 'Chidi' })
      await settle(150)
      expect(live.getState().status).toBe('ready')
    } finally {
      client.close()
    }
  })

  it('shows its rows as refreshing while the connection is down, then reads again', async () => {
    await harness.restart({ acceptSql: false, operations })
    const port = Number(new URL(harness.baseUrl).port)
    const client = new SirannonClient(harness.baseUrl, { reconnectInterval: 50 })
    const db = client.database('testdb')

    try {
      const live = await db.live(members, {})
      expect(live.getState()).toMatchObject({ status: 'ready', revalidating: false })

      await harness.restart({ port, acceptSql: false, operations })
      await settle(60)
      expect(live.getState()).toMatchObject({ status: 'ready', revalidating: true })

      const db2 = new SirannonClient(harness.baseUrl).database('testdb')
      await db2.execute(addMember, { name: 'Dara' })

      await waitForRows(() => live.getState(), 2)
      const state = live.getState()
      expect(state.status === 'ready' && state.rows[1].name).toBe('Dara')
      await live.close()
    } finally {
      client.close()
    }
  })

  it('refuses a live query over the HTTP transport', async () => {
    await harness.restart({ acceptSql: false, operations })
    const client = new SirannonClient(harness.baseUrl, { transport: 'http' })
    try {
      await expect(client.database('testdb').live(members, {})).rejects.toMatchObject({ code: 'TRANSPORT_ERROR' })
    } finally {
      client.close()
    }
  })

  it('reports an unknown operation as an error rather than an empty result', async () => {
    await harness.restart({ acceptSql: false, operations })
    const client = new SirannonClient(harness.baseUrl)
    try {
      await expect(client.database('testdb').live('archivedMembers')).rejects.toMatchObject({
        code: 'UNKNOWN_QUERY',
      })
    } finally {
      client.close()
    }
  })
})
