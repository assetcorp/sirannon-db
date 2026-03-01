import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import type { SirannonServer } from '../../server/server.js'
import { createServer } from '../../server/server.js'
import { SirannonClient } from '../client.js'
import { RemoteDatabase } from '../database-proxy.js'
import { HttpTransport } from '../transport/http.js'
import { RemoteError } from '../types.js'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-client-'))
  sirannon = new Sirannon()

  const db = sirannon.open('testdb', join(tempDir, 'test.db'))
  db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('SirannonClient', () => {
  it('creates with default options', () => {
    const client = new SirannonClient(baseUrl)
    expect(client).toBeInstanceOf(SirannonClient)
    client.close()
  })

  it('returns the same RemoteDatabase for the same id', () => {
    const client = new SirannonClient(baseUrl)
    const db1 = client.database('testdb')
    const db2 = client.database('testdb')
    expect(db1).toBe(db2)
    client.close()
  })

  it('returns different RemoteDatabase instances for different ids', () => {
    const client = new SirannonClient(baseUrl)
    const db1 = client.database('db1')
    const db2 = client.database('db2')
    expect(db1).not.toBe(db2)
    client.close()
  })

  it('throws after close', () => {
    const client = new SirannonClient(baseUrl)
    client.close()
    expect(() => client.database('testdb')).toThrow('Client is closed')
  })

  it('creates a new database instance after the previous one was closed', () => {
    const client = new SirannonClient(baseUrl)
    const db1 = client.database('testdb')
    db1.close()
    const db2 = client.database('testdb')
    expect(db2).not.toBe(db1)
    client.close()
  })
})

describe('RemoteDatabase via HTTP', () => {
  let client: SirannonClient

  beforeEach(() => {
    client = new SirannonClient(baseUrl, { transport: 'http' })
  })

  afterEach(() => {
    client.close()
  })

  it('queries and returns rows', async () => {
    const db = client.database('testdb')
    const rows = await db.query<{ id: number; name: string; age: number }>('SELECT * FROM users ORDER BY id')
    expect(rows).toHaveLength(2)
    expect(rows[0].name).toBe('Alice')
    expect(rows[1].name).toBe('Bob')
  })

  it('queries with positional parameters', async () => {
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users WHERE age > ?', [26])
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
  })

  it('queries with named parameters', async () => {
    const db = client.database('testdb')
    const rows = await db.query<{ name: string }>('SELECT name FROM users WHERE name = :name', { name: 'Bob' })
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Bob')
  })

  it('returns empty array for no matching rows', async () => {
    const db = client.database('testdb')
    const rows = await db.query('SELECT * FROM users WHERE age > 100')
    expect(rows).toEqual([])
  })

  it('executes a mutation and returns result', async () => {
    const db = client.database('testdb')
    const result = await db.execute("INSERT INTO users (name, age) VALUES ('Charlie', 35)")
    expect(result.changes).toBe(1)
    expect(result.lastInsertRowId).toBeDefined()
  })

  it('executes an update', async () => {
    const db = client.database('testdb')
    const result = await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    expect(result.changes).toBe(1)

    const rows = await db.query<{ age: number }>("SELECT age FROM users WHERE name = 'Alice'")
    expect(rows[0].age).toBe(31)
  })

  it('executes a delete', async () => {
    const db = client.database('testdb')
    const result = await db.execute("DELETE FROM users WHERE name = 'Bob'")
    expect(result.changes).toBe(1)

    const rows = await db.query('SELECT * FROM users')
    expect(rows).toHaveLength(1)
  })

  it('runs a transaction', async () => {
    const db = client.database('testdb')
    const results = await db.transaction([
      { sql: "INSERT INTO users (name, age) VALUES ('Charlie', 35)" },
      { sql: "INSERT INTO users (name, age) VALUES ('Diana', 28)" },
    ])
    expect(results).toHaveLength(2)
    expect(results[0].changes).toBe(1)
    expect(results[1].changes).toBe(1)

    const rows = await db.query('SELECT * FROM users')
    expect(rows).toHaveLength(4)
  })

  it('throws RemoteError on query error', async () => {
    const db = client.database('testdb')
    try {
      await db.query('SLECT * FORM users')
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(RemoteError)
      expect((err as RemoteError).code).toBe('QUERY_ERROR')
    }
  })

  it('throws RemoteError for nonexistent database', async () => {
    const db = client.database('nonexistent')
    try {
      await db.query('SELECT 1')
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(RemoteError)
      expect((err as RemoteError).code).toBe('DATABASE_NOT_FOUND')
    }
  })

  it('throws RemoteError when subscribing via HTTP transport', async () => {
    const db = client.database('testdb')
    try {
      await db.on('users').subscribe(() => {})
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(RemoteError)
      expect((err as RemoteError).code).toBe('TRANSPORT_ERROR')
    }
  })

  it('throws after transport is closed', async () => {
    const db = client.database('testdb')
    db.close()
    try {
      await db.query('SELECT 1')
      expect.fail('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(RemoteError)
      expect((err as RemoteError).code).toBe('TRANSPORT_ERROR')
    }
  })

  it('sends custom headers', async () => {
    const authClient = new SirannonClient(baseUrl, {
      transport: 'http',
      headers: { 'x-custom': 'test-value' },
    })
    const db = authClient.database('testdb')
    // The server doesn't validate custom headers, so we verify
    // the request succeeds (headers are forwarded).
    const rows = await db.query('SELECT 1 as result')
    expect(rows).toHaveLength(1)
    authClient.close()
  })
})

describe('RemoteDatabase', () => {
  it('exposes the database id', () => {
    const client = new SirannonClient(baseUrl)
    const db = client.database('testdb')
    expect(db.id).toBe('testdb')
    client.close()
  })

  it('is an instance of RemoteDatabase', () => {
    const client = new SirannonClient(baseUrl)
    const db = client.database('testdb')
    expect(db).toBeInstanceOf(RemoteDatabase)
    client.close()
  })
})

describe('HttpTransport', () => {
  it('rejects after close', async () => {
    const transport = new HttpTransport(`${baseUrl}/db/testdb`)
    transport.close()
    await expect(transport.query('SELECT 1')).rejects.toThrow('Transport is closed')
  })

  it('rejects subscribe', async () => {
    const transport = new HttpTransport(`${baseUrl}/db/testdb`)
    await expect(transport.subscribe('users', undefined, () => {})).rejects.toThrow(
      'Subscriptions require WebSocket transport',
    )
    transport.close()
  })
})
