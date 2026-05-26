import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-'))
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('test', join(tempDir, 'test.db'))
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
  await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
  await db.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)")

  server = createServer(sirannon, { port: 0 })
  await server.listen()
})

afterEach(async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('CORS', () => {
  it('sets CORS headers when cors: true', async () => {
    const corsServer = createServer(sirannon, { port: 0, cors: true })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      const preflight = await fetch(`${corsUrl}/db/test/query`, {
        method: 'OPTIONS',
      })
      expect(preflight.status).toBe(204)
      expect(preflight.headers.get('access-control-allow-origin')).toBe('*')
      expect(preflight.headers.get('access-control-allow-methods')).toContain('POST')

      const res = await fetch(`${corsUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.headers.get('access-control-allow-origin')).toBe('*')
    } finally {
      await corsServer.close()
    }
  })

  it('uses custom CORS origin', async () => {
    const corsServer = createServer(sirannon, {
      port: 0,
      cors: { origin: 'https://app.example.com' },
    })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      const res = await fetch(`${corsUrl}/db/test/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.headers.get('access-control-allow-origin')).toBe('https://app.example.com')
    } finally {
      await corsServer.close()
    }
  })

  it('echoes matching origin from array configuration', async () => {
    const corsServer = createServer(sirannon, {
      port: 0,
      cors: { origin: ['https://app.example.com', 'https://admin.example.com'] },
    })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      const res = await fetch(`${corsUrl}/db/test/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Origin: 'https://admin.example.com',
        },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.headers.get('access-control-allow-origin')).toBe('https://admin.example.com')
      expect(res.headers.get('vary')).toBe('Origin')
    } finally {
      await corsServer.close()
    }
  })

  it('omits CORS header when origin is not in allowed list', async () => {
    const corsServer = createServer(sirannon, {
      port: 0,
      cors: { origin: ['https://app.example.com', 'https://admin.example.com'] },
    })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      const res = await fetch(`${corsUrl}/db/test/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Origin: 'https://evil.example.com',
        },
        body: JSON.stringify({ sql: 'SELECT 1 as val' }),
      })
      expect(res.headers.get('access-control-allow-origin')).toBeNull()
    } finally {
      await corsServer.close()
    }
  })

  it('defaults CORS origin to wildcard when omitted in config object', async () => {
    const corsServer = createServer(sirannon, {
      port: 0,
      cors: {},
    })
    await corsServer.listen()
    const corsUrl = `http://127.0.0.1:${corsServer.listeningPort}`

    try {
      const preflight = await fetch(`${corsUrl}/db/test/query`, {
        method: 'OPTIONS',
      })
      expect(preflight.status).toBe(204)
      expect(preflight.headers.get('access-control-allow-origin')).toBe('*')
    } finally {
      await corsServer.close()
    }
  })
})
