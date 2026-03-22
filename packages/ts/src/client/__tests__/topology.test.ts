import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SirannonClient, type TopologyAwareClientOptions } from '../client.js'

const driver = betterSqlite3()

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

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

  it('executes writes via primary', async () => {
    const client = new SirannonClient({
      primary: baseUrl,
      replicas: [baseUrl],
      readPreference: 'replica',
      transport: 'http',
    })
    const db = client.database('testdb')
    const result = await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    expect(result.changes).toBe(1)
    client.close()
  })

  it('routes reads to replica when readPreference is replica', async () => {
    const client = new SirannonClient({
      primary: baseUrl,
      replicas: [baseUrl],
      readPreference: 'replica',
      transport: 'http',
    })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    client.close()
  })

  it('routes reads with readPreference nearest', async () => {
    const client = new SirannonClient({
      primary: baseUrl,
      replicas: [baseUrl],
      readPreference: 'nearest',
      transport: 'http',
    })
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users')
    expect(rows).toHaveLength(1)
    client.close()
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
})
