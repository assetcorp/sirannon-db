import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type MemoryDestination, memoryDestination } from '../../../core/__tests__/backup/memory-destination.js'
import type { BackupChain } from '../../../core/backup/chain.js'
import type { BackupCycleStatus } from '../../../core/backup/cycle-status.js'
import type { BackupVerifyResult } from '../../../core/backup/verify.js'
import { RequestDeniedError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ServerOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { createServer, type SirannonServer } from '../../server.js'

let tempDir: string
let sirannon: Sirannon
let destination: MemoryDestination
let server: SirannonServer | null = null
let baseUrl: string

const driver = betterSqlite3()

async function listen(options?: ServerOptions): Promise<void> {
  server = createServer(sirannon, { port: 0, ...options })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-backup-'))
  destination = memoryDestination()
  sirannon = new Sirannon({ driver })
  const db = await sirannon.open('orders', join(tempDir, 'orders.db'), {
    backups: {
      destination,
      intervalMs: 0,
      stagingDir: join(tempDir, 'orders-staging'),
      onError: () => {},
    },
  })
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)')
  await db.execute('INSERT INTO orders (total) VALUES (10)')
  await sirannon.open('ledger', join(tempDir, 'ledger.db'))
})

afterEach(async () => {
  await server?.close()
  server = null
  await sirannon.shutdown().catch(() => {})
  rmSync(tempDir, { recursive: true, force: true })
})

async function statusUntilIdle(): Promise<BackupCycleStatus> {
  for (let attempt = 0; attempt < 200; attempt++) {
    const response = await fetch(`${baseUrl}/db/orders/backup`)
    const status = (await response.json()) as BackupCycleStatus
    if (!status.running && status.lastRun) return status
    await new Promise(resolve => setTimeout(resolve, 10))
  }
  throw new Error('the backup never finished')
}

describe('the backup routes an operator reaches over the server', () => {
  it('accepts a triggered backup at once and leaves the request open for nobody', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })

    expect(response.status).toBe(202)
    expect(await response.json()).toEqual({ started: true })
  })

  it('reports the run a triggered backup produced through the progress route', async () => {
    await listen()

    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    const status = await statusUntilIdle()

    expect(status.running).toBe(false)
    expect(status.lastRun?.databaseId).toBe('orders')
    expect(status.chainId).toBe(status.lastRun?.chainId)
  })

  it('lists the full copy and the change pieces the destination stores', async () => {
    await listen()
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await statusUntilIdle()

    const response = await fetch(`${baseUrl}/db/orders/backup/chain`)
    const body = (await response.json()) as { chains: BackupChain[] }

    expect(response.status).toBe(200)
    expect(body.chains).toHaveLength(1)
    expect(body.chains[0]?.base?.kind).toBe('full')
  })

  it('reads a stored backup back and reports what it found', async () => {
    await listen()
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await statusUntilIdle()
    const listed = (await (await fetch(`${baseUrl}/db/orders/backup/chain`)).json()) as { chains: BackupChain[] }
    const name = listed.chains[0]?.base?.name

    const response = await fetch(`${baseUrl}/db/orders/backup/verify`, {
      method: 'POST',
      body: JSON.stringify({ name }),
    })
    const body = (await response.json()) as BackupVerifyResult

    expect(response.status).toBe(200)
    expect(body.name).toBe(name)
    expect(body.kind).toBe('full')
    expect(body.bytesRead).toBeGreaterThan(0)
  })

  it('refuses a verify of a name no chain record states', async () => {
    await listen()
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await statusUntilIdle()

    const response = await fetch(`${baseUrl}/db/orders/backup/verify`, {
      method: 'POST',
      body: JSON.stringify({ name: 'nothing-stored-here.db' }),
    })

    expect(response.status).toBe(409)
    expect((await response.json()).error.code).toBe('BACKUP_CHAIN_BROKEN')
  })

  it('refuses a verify that names nothing', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/orders/backup/verify`, {
      method: 'POST',
      body: JSON.stringify({}),
    })

    expect(response.status).toBe(400)
    expect((await response.json()).error.code).toBe('INVALID_REQUEST')
  })

  it('answers which backups no restore still needs', async () => {
    await listen()
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await statusUntilIdle()

    const response = await fetch(`${baseUrl}/db/orders/backup/safe-to-delete`, {
      method: 'POST',
      body: JSON.stringify({}),
    })

    expect(response.status).toBe(200)
    expect((await response.json()).records).toEqual([])
  })

  it('refuses a safe-to-delete asking to keep a moment that is not a number', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/orders/backup/safe-to-delete`, {
      method: 'POST',
      body: JSON.stringify({ restorableFrom: 'yesterday' }),
    })

    expect(response.status).toBe(400)
    expect((await response.json()).error.code).toBe('INVALID_REQUEST')
  })

  it('tells a caller plainly that a database opened without backups keeps no chain', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/ledger/backup/chain`)

    expect(response.status).toBe(501)
    expect((await response.json()).error.code).toBe('BACKUP_UNSUPPORTED')
  })

  it('reports a turn refused before it began, in place of reporting that one started', async () => {
    await listen()
    await sirannon.close('orders')

    const response = await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })

    expect(response.status).toBe(404)
    expect((await response.json()).error.code).toBe('DATABASE_NOT_FOUND')
  })

  it('refuses a body that is valid JSON but is no object', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/orders/backup/verify`, { method: 'POST', body: '0' })

    expect(response.status).toBe(400)
    expect((await response.json()).error.code).toBe('INVALID_REQUEST')
  })

  it('answers an unknown database with the code every other route uses', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/missing/backup/chain`)

    expect(response.status).toBe(404)
    expect((await response.json()).error.code).toBe('DATABASE_NOT_FOUND')
  })

  it('lets the authenticate hook refuse the backup routes alone, from the path it is given', async () => {
    await listen({
      authenticate: ctx => {
        if (ctx.path.startsWith('/db/orders/backup')) {
          throw new RequestDeniedError(403, 'HOOK_DENIED', 'Only an operator may reach the backups')
        }
        return { user: 'app' }
      },
    })

    const backup = await fetch(`${baseUrl}/db/orders/backup/chain`)
    const cluster = await fetch(`${baseUrl}/db/orders/cluster`)

    expect(backup.status).toBe(403)
    expect((await backup.json()).error.code).toBe('HOOK_DENIED')
    expect(cluster.status).toBe(404)
  })

  it('runs the authenticate hook before every backup route', async () => {
    const seen: string[] = []
    await listen({
      authenticate: ctx => {
        seen.push(ctx.path)
        throw new RequestDeniedError(401, 'IDENTITY_REQUIRED', 'Name yourself')
      },
    })

    const paths = ['/db/orders/backup', '/db/orders/backup/chain', '/db/orders/backup/restore']
    for (const path of paths) {
      expect((await fetch(`${baseUrl}${path}`)).status).toBe(401)
    }
    for (const path of ['/db/orders/backup', '/db/orders/backup/verify', '/db/orders/backup/safe-to-delete']) {
      expect((await fetch(`${baseUrl}${path}`, { method: 'POST', body: '{}' })).status).toBe(401)
    }

    expect(seen).toHaveLength(6)
  })
})
