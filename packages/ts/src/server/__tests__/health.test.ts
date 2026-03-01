import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { createServer, type SirannonServer } from '../server.js'

let tempDir: string
let sirannon: Sirannon
let server: SirannonServer
let baseUrl: string

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-health-'))
  sirannon = new Sirannon()
  server = createServer(sirannon, { port: 0 })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
})

afterEach(async () => {
  await server.close()
  sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('GET /health', () => {
  it('returns 200 with status ok', async () => {
    const res = await fetch(`${baseUrl}/health`)
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toBe('application/json')
    const body = await res.json()
    expect(body).toEqual({ status: 'ok' })
  })

  it('returns 200 even with no databases', async () => {
    const res = await fetch(`${baseUrl}/health`)
    expect(res.status).toBe(200)
  })
})

describe('GET /health/ready', () => {
  it('returns ok status with no databases', async () => {
    const res = await fetch(`${baseUrl}/health/ready`)
    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body.status).toBe('ok')
    expect(body.databases).toEqual([])
  })

  it('returns database details for open databases', async () => {
    const dbPath = join(tempDir, 'ready.db')
    sirannon.open('mydb', dbPath)

    const res = await fetch(`${baseUrl}/health/ready`)
    expect(res.status).toBe(200)
    const body = await res.json()
    expect(body.status).toBe('ok')
    expect(body.databases).toHaveLength(1)
    expect(body.databases[0].id).toBe('mydb')
    expect(body.databases[0].readOnly).toBe(false)
    expect(body.databases[0].closed).toBe(false)
  })

  it('shows multiple databases', async () => {
    sirannon.open('db1', join(tempDir, 'db1.db'))
    sirannon.open('db2', join(tempDir, 'db2.db'))

    const res = await fetch(`${baseUrl}/health/ready`)
    const body = await res.json()
    expect(body.databases).toHaveLength(2)
    const ids = body.databases.map((d: { id: string }) => d.id)
    expect(ids).toContain('db1')
    expect(ids).toContain('db2')
  })

  it('returns ok when closed databases are auto-removed from registry', async () => {
    const db = sirannon.open('closing', join(tempDir, 'closing.db'))
    db.close()

    const db2 = sirannon.open('remaining', join(tempDir, 'remaining.db'))

    const res = await fetch(`${baseUrl}/health/ready`)
    const body = await res.json()
    expect(body.status).toBe('ok')
    db2.close()
  })

  it('returns ok with read-only database', async () => {
    // Create a database first so it exists on disk
    const dbPath = join(tempDir, 'ro.db')
    const setup = sirannon.open('setup', dbPath)
    setup.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    sirannon.close('setup')

    sirannon.open('readonly', dbPath, { readOnly: true })

    const res = await fetch(`${baseUrl}/health/ready`)
    const body = await res.json()
    expect(body.status).toBe('ok')
    const rodb = body.databases.find((d: { id: string }) => d.id === 'readonly')
    expect(rodb).toBeDefined()
    expect(rodb.readOnly).toBe(true)
  })
})

describe('server lifecycle', () => {
  it('reports listeningPort after listen', () => {
    expect(server.listeningPort).toBeGreaterThan(0)
  })

  it('reports -1 for listeningPort after close', async () => {
    const tmpServer = createServer(sirannon, { port: 0 })
    await tmpServer.listen()
    expect(tmpServer.listeningPort).toBeGreaterThan(0)
    await tmpServer.close()
    expect(tmpServer.listeningPort).toBe(-1)
  })
})
