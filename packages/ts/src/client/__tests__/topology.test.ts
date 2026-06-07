import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SirannonClient, type TopologyAwareClientOptions } from '../client.js'
import type { Transport } from '../types.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

function wait(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolveDeferred = () => {}
  const promise = new Promise<void>(resolve => {
    resolveDeferred = resolve
  })
  return { promise, resolve: resolveDeferred }
}

function createDelayedSubscriptionTransport(gate: Promise<void>, closedIndexes: number[], index: number): Transport {
  let closed = false
  return {
    query: async () => ({ rows: [] }),
    execute: async () => ({ changes: 0, lastInsertRowId: 0 }),
    transaction: async () => ({ results: [] }),
    subscribe: async () => {
      await gate
      if (closed) {
        throw new Error(`transport ${index} closed during subscription`)
      }
      return { unsubscribe: () => {} }
    },
    close: () => {
      closed = true
      closedIndexes.push(index)
    },
  }
}

interface RecordedSubscriptionOperation {
  action: 'subscribe' | 'unsubscribe'
  endpoint: string
  table: string
}

function createRecordedSubscriptionTransport(
  endpoint: string,
  operations: RecordedSubscriptionOperation[],
  closedEndpoints: string[],
  failSubscribe = false,
): Transport {
  let closed = false
  return {
    query: async () => ({ rows: [] }),
    execute: async () => ({ changes: 0, lastInsertRowId: 0 }),
    transaction: async () => ({ results: [] }),
    subscribe: async table => {
      if (closed) {
        throw new Error(`transport for ${endpoint} is closed`)
      }
      if (failSubscribe) {
        throw new Error(`subscriptions unavailable at ${endpoint}`)
      }
      let active = true
      operations.push({ action: 'subscribe', endpoint, table })
      return {
        unsubscribe: () => {
          if (!active) return
          active = false
          operations.push({ action: 'unsubscribe', endpoint, table })
        },
      }
    },
    close: () => {
      if (closed) return
      closed = true
      closedEndpoints.push(endpoint)
    },
  }
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-topo-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('testdb', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.execute("INSERT INTO users (name) VALUES ('Alice')")

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('backward compatibility', () => {
  it('single-URL constructor works unchanged', () => {
    const client = new SirannonClient(baseUrl)
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('single-URL constructor with options works unchanged', () => {
    const client = new SirannonClient(baseUrl, { transport: 'http' })
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('queries work with single-URL constructor', async () => {
    const client = new SirannonClient(baseUrl, { transport: 'http' })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    client.close()
  })

  it('throws after close with single-URL constructor', () => {
    const client = new SirannonClient(baseUrl)
    client.close()
    expect(() => client.database('testdb')).toThrow('Client is closed')
  })
})

describe('TopologyAwareClientOptions', () => {
  it('accepts primary and replicas config', () => {
    const opts: TopologyAwareClientOptions = {
      primary: baseUrl,
      replicas: [baseUrl],
      readPreference: 'primary',
      transport: 'http',
    }
    const client = new SirannonClient(opts)
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('rejects unsafe configured endpoint URLs', () => {
    expect(
      () =>
        new SirannonClient({
          primary: 'file:///tmp/test.db',
          transport: 'http',
        }),
    ).toThrow('must use http or https')

    expect(
      () =>
        new SirannonClient({
          primary: 'https://user:password@example.com',
          transport: 'http',
        }),
    ).toThrow('must not contain credentials')
  })

  it('rejects unsafe coordinator-discovered endpoint URLs', async () => {
    await server.close()
    server = createServer(sirannon, {
      port: 0,
      getClusterStatus: databaseId => ({
        databaseId,
        currentPrimary: { nodeId: 'node-a', endpoint: 'file:///tmp/test.db' },
        primaryTerm: 1n,
        readEndpoints: [],
        health: 'healthy',
      }),
    })
    await server.listen()
    baseUrl = `http://127.0.0.1:${server.listeningPort}`

    const client = new SirannonClient({
      endpoints: [baseUrl],
      discovery: 'coordinator',
      transport: 'http',
    })

    await expect(client._refreshClusterRouting('testdb')).rejects.toMatchObject({ code: 'INVALID_RESPONSE' })
    client.close()
  })

  it('queries via primary with readPreference primary', async () => {
    const client = new SirannonClient({
      primary: baseUrl,
      replicas: [],
      readPreference: 'primary',
      transport: 'http',
    })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    client.close()
  })

  describe('routing with distinct endpoints', () => {
    let replicaTempDir: string
    let replicaSirannon: Sirannon
    let replicaServer: SirannonServer
    let replicaUrl: string

    beforeEach(async () => {
      replicaTempDir = mkdtempSync(join(tmpdir(), 'sirannon-topo-replica-'))
      replicaSirannon = new Sirannon({ driver })
      const replicaDb = await replicaSirannon.open('testdb', join(replicaTempDir, 'test.db'))
      await replicaDb.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
      await replicaDb.execute("INSERT INTO users (name) VALUES ('ReplicaUser')")

      replicaServer = createServer(replicaSirannon, { port: 0 })
      await replicaServer.listen()
      replicaUrl = `http://127.0.0.1:${replicaServer.listeningPort}`
    })

    afterEach(async () => {
      await replicaServer.close()
      await replicaSirannon.shutdown()
      rmSync(replicaTempDir, { recursive: true, force: true })
    })

    it('executes writes via primary', async () => {
      const client = new SirannonClient({
        primary: baseUrl,
        replicas: [replicaUrl],
        readPreference: 'replica',
        transport: 'http',
      })
      const db = client.database('testdb')
      await db.execute("INSERT INTO users (name) VALUES ('Bob')")

      const primaryClient = new SirannonClient(baseUrl, { transport: 'http' })
      const primaryRows = await primaryClient
        .database('testdb')
        .query<{ name: string }>("SELECT name FROM users WHERE name = 'Bob'")
      expect(primaryRows).toHaveLength(1)

      const replicaClient = new SirannonClient(replicaUrl, { transport: 'http' })
      const replicaRows = await replicaClient
        .database('testdb')
        .query<{ name: string }>("SELECT name FROM users WHERE name = 'Bob'")
      expect(replicaRows).toHaveLength(0)

      client.close()
      primaryClient.close()
      replicaClient.close()
    })

    it('routes reads to replica when readPreference is replica', async () => {
      const client = new SirannonClient({
        primary: baseUrl,
        replicas: [replicaUrl],
        readPreference: 'replica',
        transport: 'http',
      })
      const db = client.database('testdb')
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('ReplicaUser')
      client.close()
    })

    it('routes coordinator replica reads away from a readable current primary', async () => {
      await server.close()
      server = createServer(sirannon, {
        port: 0,
        getClusterStatus: databaseId => ({
          databaseId,
          currentPrimary: { nodeId: 'node-a', endpoint: baseUrl },
          primaryTerm: 1n,
          readEndpoints: [
            { nodeId: 'node-a', endpoint: baseUrl, readConcerns: ['local', 'majority', 'linearizable'] },
            { nodeId: 'node-b', endpoint: replicaUrl, readConcerns: ['local', 'majority'] },
          ],
          health: 'healthy',
        }),
      })
      await server.listen()
      baseUrl = `http://127.0.0.1:${server.listeningPort}`

      const client = new SirannonClient({
        endpoints: [baseUrl],
        discovery: 'coordinator',
        readPreference: 'replica',
        transport: 'http',
      })
      const db = client.database('testdb')
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('ReplicaUser')
      client.close()
    })

    it('routes reads with readPreference nearest', async () => {
      const client = new SirannonClient({
        primary: baseUrl,
        replicas: [replicaUrl],
        readPreference: 'nearest',
        transport: 'http',
      })
      const db = client.database('testdb')
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
      expect(['Alice', 'ReplicaUser']).toContain(rows[0].name)
      client.close()
    })
  })

  it('falls back to primary when all replicas are unreachable', async () => {
    const client = new SirannonClient({
      primary: baseUrl,
      replicas: ['http://127.0.0.1:1'],
      readPreference: 'replica',
      transport: 'http',
    })
    const db = client.database('testdb')
    try {
      await db.query<{ name: string }>('SELECT name FROM users')
    } catch {
      const rows = await db.query<{ name: string }>('SELECT name FROM users')
      expect(rows).toHaveLength(1)
    }
    client.close()
  })

  it('defaults to primary readPreference when omitted', async () => {
    const client = new SirannonClient({
      primary: baseUrl,
      transport: 'http',
    })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    client.close()
  })

  it('returns cached database instances', () => {
    const client = new SirannonClient({
      primary: baseUrl,
      transport: 'http',
    })
    const db1 = client.database('testdb')
    const db2 = client.database('testdb')
    expect(db1).toBe(db2)
    client.close()
  })

  it('keeps coordinator subscriptions on one stable read transport', async () => {
    const client = new SirannonClient({
      endpoints: [baseUrl],
      discovery: 'coordinator',
      readPreference: 'replica',
      transport: 'websocket',
    })
    const gate = deferred()
    const closedIndexes: number[] = []
    const endpoints = [
      'http://127.0.0.1:7101',
      'http://127.0.0.1:7102',
      'http://127.0.0.1:7103',
      'http://127.0.0.1:7104',
      'http://127.0.0.1:7105',
    ]

    const getReadEndpoint = vi.spyOn(client, '_getReadEndpoint').mockImplementation(async () => {
      return endpoints.shift() ?? baseUrl
    })
    const createTransport = vi.spyOn(client, '_createTransportForEndpoint').mockImplementation(() => {
      const index = createTransport.mock.calls.length
      return createDelayedSubscriptionTransport(gate.promise, closedIndexes, index)
    })

    const db = client.database('testdb')
    const subscriptions = Promise.all([
      db.on('customers').subscribe(() => {}),
      db.on('entitlements').subscribe(() => {}),
      db.on('usage_events').subscribe(() => {}),
      db.on('billing_events').subscribe(() => {}),
      db.on('audit_log').subscribe(() => {}),
    ])

    await wait(0)
    gate.resolve()
    const handles = await subscriptions

    expect(getReadEndpoint).toHaveBeenCalledTimes(1)
    expect(createTransport).toHaveBeenCalledTimes(1)
    expect(closedIndexes).toEqual([])

    for (const handle of handles) {
      handle.unsubscribe()
    }
    client.close()
  })

  it('migrates active coordinator subscriptions when routing metadata changes', async () => {
    await server.close()
    let primaryEndpoint = baseUrl
    let readableEndpoint = 'http://127.0.0.1:7201'
    server = createServer(sirannon, {
      port: 0,
      getClusterStatus: databaseId => ({
        databaseId,
        currentPrimary: { nodeId: 'node-a', endpoint: primaryEndpoint },
        primaryTerm: 1n,
        readEndpoints: [{ nodeId: 'node-b', endpoint: readableEndpoint, readConcerns: ['majority'] }],
        health: 'healthy',
      }),
    })
    await server.listen()
    primaryEndpoint = `http://127.0.0.1:${server.listeningPort}`
    baseUrl = primaryEndpoint

    const client = new SirannonClient({
      endpoints: [baseUrl],
      discovery: 'coordinator',
      readPreference: 'replica',
      transport: 'websocket',
    })
    const operations: RecordedSubscriptionOperation[] = []
    const closedEndpoints: string[] = []
    const createTransport = vi
      .spyOn(client, '_createTransportForEndpoint')
      .mockImplementation(endpoint =>
        createRecordedSubscriptionTransport(endpoint, operations, closedEndpoints, endpoint.endsWith(':7203')),
      )

    const db = client.database('testdb')
    const customers = await db.on('customers').subscribe(() => {})
    const usage = await db.on('usage_events').subscribe(() => {})

    expect(createTransport).toHaveBeenCalledTimes(1)
    expect(
      operations
        .filter(operation => operation.action === 'subscribe')
        .map(operation => [operation.endpoint, operation.table]),
    ).toEqual([
      ['http://127.0.0.1:7201', 'customers'],
      ['http://127.0.0.1:7201', 'usage_events'],
    ])

    customers.unsubscribe()
    readableEndpoint = 'http://127.0.0.1:7202'
    await client._refreshClusterRouting('testdb')

    expect(
      operations
        .filter(operation => operation.action === 'subscribe')
        .map(operation => [operation.endpoint, operation.table]),
    ).toEqual([
      ['http://127.0.0.1:7201', 'customers'],
      ['http://127.0.0.1:7201', 'usage_events'],
      ['http://127.0.0.1:7202', 'usage_events'],
    ])
    expect(operations).toContainEqual({
      action: 'unsubscribe',
      endpoint: 'http://127.0.0.1:7201',
      table: 'customers',
    })
    expect(operations).toContainEqual({
      action: 'unsubscribe',
      endpoint: 'http://127.0.0.1:7201',
      table: 'usage_events',
    })
    expect(closedEndpoints).toContain('http://127.0.0.1:7201')

    readableEndpoint = 'http://127.0.0.1:7203'
    await expect(client._refreshClusterRouting('testdb')).rejects.toMatchObject({
      code: 'ROUTING_ERROR',
      message: expect.stringContaining('Could not re-establish active subscriptions'),
    })
    expect(closedEndpoints).toContain('http://127.0.0.1:7203')
    expect(closedEndpoints).not.toContain('http://127.0.0.1:7202')

    usage.unsubscribe()
    client.close()
  })
})
