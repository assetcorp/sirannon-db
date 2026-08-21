import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { type MemoryDestination, memoryDestination } from '../../../core/__tests__/backup/memory-destination.js'
import type { BackupCycleStatus } from '../../../core/backup/cycle-status.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { ServerOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { BackupRestoreStatus } from '../../backup-restore-runs.js'
import { createServer, type SirannonServer } from '../../server.js'

let tempDir: string
let sirannon: Sirannon
let destination: MemoryDestination
let server: SirannonServer | null = null
let baseUrl: string

const driver = betterSqlite3()

async function listen(options?: ServerOptions): Promise<void> {
  server = createServer(sirannon, { port: 0, authenticate: () => ({ operator: true }), ...options })
  await server.listen()
  baseUrl = `http://127.0.0.1:${server.listeningPort}`
}

function pause(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function backupSettled(): Promise<void> {
  for (let attempt = 0; attempt < 200; attempt++) {
    const status = (await (await fetch(`${baseUrl}/db/orders/backup`)).json()) as BackupCycleStatus
    if (!status.running && status.lastRun) return
    await pause(10)
  }
  throw new Error('the backup never finished')
}

async function restoreSettled(): Promise<BackupRestoreStatus> {
  for (let attempt = 0; attempt < 400; attempt++) {
    const status = (await (await fetch(`${baseUrl}/db/orders/backup/restore`)).json()) as BackupRestoreStatus
    if (status.state === 'done' || status.state === 'failed') return status
    await pause(10)
  }
  throw new Error('the restore never finished')
}

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-http-restore-'))
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
  await sirannon.open('ledger', join(tempDir, 'ledger.db'))
})

afterEach(async () => {
  await server?.close()
  server = null
  await sirannon.shutdown().catch(() => {})
  rmSync(tempDir, { recursive: true, force: true })
})

describe('the restore route the operator opens', () => {
  it('refuses to start a server that would restore for a caller it cannot name', () => {
    expect(() => createServer(sirannon, { port: 0, acceptBackupRestore: true })).toThrow(/authenticate hook/)
  })

  it('stays shut on a server nobody opened it on', async () => {
    await listen()

    const response = await fetch(`${baseUrl}/db/orders/backup/restore`, { method: 'POST', body: '{}' })

    expect(response.status).toBe(403)
    expect((await response.json()).error.code).toBe('BACKUP_RESTORE_NOT_ACCEPTED')
  })

  it('reports an idle restore before anyone has asked for one', async () => {
    await listen({ acceptBackupRestore: true })

    const status = (await (await fetch(`${baseUrl}/db/orders/backup/restore`)).json()) as BackupRestoreStatus

    expect(status).toEqual({ state: 'idle' })
  })

  it('puts the database back to the moment the caller named, and opens it again', async () => {
    await listen({ acceptBackupRestore: true })
    await sirannon.get('orders')?.execute('INSERT INTO orders (total) VALUES (10)')
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await backupSettled()

    await pause(20)
    const moment = Date.now()
    await pause(20)

    await sirannon.get('orders')?.execute('INSERT INTO orders (total) VALUES (20)')
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await backupSettled()
    expect(await sirannon.get('orders')?.query('SELECT total FROM orders')).toHaveLength(2)

    const accepted = await fetch(`${baseUrl}/db/orders/backup/restore`, {
      method: 'POST',
      body: JSON.stringify({ moment }),
    })
    expect(accepted.status).toBe(202)
    expect(await accepted.json()).toEqual({ started: true })

    const status = await restoreSettled()
    expect(status.state).toBe('done')
    expect(status.report?.restoresTo).toBeLessThanOrEqual(moment)
    expect(sirannon.has('orders')).toBe(true)
    expect(await sirannon.get('orders')?.query('SELECT total FROM orders')).toEqual([{ total: 10 }])
  })

  it('refuses a second restore while the first still has the database', async () => {
    await listen({ acceptBackupRestore: true })
    await sirannon.get('orders')?.execute('INSERT INTO orders (total) VALUES (10)')
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await backupSettled()

    await fetch(`${baseUrl}/db/orders/backup/restore`, { method: 'POST', body: '{}' })
    const second = await fetch(`${baseUrl}/db/orders/backup/restore`, { method: 'POST', body: '{}' })

    expect(second.status).toBe(409)
    expect((await second.json()).error.code).toBe('BACKUP_RESTORE_IN_PROGRESS')
    await restoreSettled()
  })

  it('refuses a moment that is not a moment', async () => {
    await listen({ acceptBackupRestore: true })

    const response = await fetch(`${baseUrl}/db/orders/backup/restore`, {
      method: 'POST',
      body: JSON.stringify({ moment: 'yesterday' }),
    })

    expect(response.status).toBe(400)
    expect((await response.json()).error.code).toBe('INVALID_REQUEST')
  })

  it('tells a caller plainly that a database opened without backups has nothing to restore from', async () => {
    await listen({ acceptBackupRestore: true })

    const response = await fetch(`${baseUrl}/db/ledger/backup/restore`, { method: 'POST', body: '{}' })

    expect(response.status).toBe(501)
    expect((await response.json()).error.code).toBe('BACKUP_UNSUPPORTED')
  })

  it('reports a restore that found no backup reaching back to the moment asked for', async () => {
    await listen({ acceptBackupRestore: true })
    await fetch(`${baseUrl}/db/orders/backup`, { method: 'POST' })
    await backupSettled()

    await fetch(`${baseUrl}/db/orders/backup/restore`, { method: 'POST', body: JSON.stringify({ moment: 1000 }) })
    const status = await restoreSettled()

    expect(status.state).toBe('failed')
    expect(status.error?.code).toBe('BACKUP_CHAIN_BROKEN')
    expect(sirannon.has('orders')).toBe(true)
  })
})
