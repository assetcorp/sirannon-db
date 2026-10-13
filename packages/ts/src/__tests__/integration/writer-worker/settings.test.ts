import { existsSync } from 'node:fs'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { defineDriver } from '../../../core/driver/define.js'
import type { SQLiteDriver } from '../../../core/driver/types.js'
import type { WriterWorkerOptions } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { columnRaw, createWorkspace, type Workspace } from './harness.js'

const OUT_OF_RANGE: [label: string, value: unknown][] = [
  ['maxPendingWrites of 0', { maxPendingWrites: 0 }],
  ['negative maxPendingWrites', { maxPendingWrites: -1 }],
  ['fractional maxPendingWrites', { maxPendingWrites: 1.5 }],
  ['NaN maxPendingWrites', { maxPendingWrites: Number.NaN }],
  ['infinite maxPendingWrites', { maxPendingWrites: Number.POSITIVE_INFINITY }],
  ['maxPendingWrites as text', { maxPendingWrites: '8' }],
  ['null maxPendingWrites', { maxPendingWrites: null }],
  ['negative writeTimeoutMs', { writeTimeoutMs: -1 }],
  ['fractional writeTimeoutMs', { writeTimeoutMs: 0.5 }],
  ['NaN writeTimeoutMs', { writeTimeoutMs: Number.NaN }],
  ['infinite writeTimeoutMs', { writeTimeoutMs: Number.POSITIVE_INFINITY }],
  ['writeTimeoutMs as text', { writeTimeoutMs: '1000' }],
  ['negative maxRestarts', { maxRestarts: -1 }],
  ['fractional maxRestarts', { maxRestarts: 2.5 }],
  ['infinite maxRestarts', { maxRestarts: Number.POSITIVE_INFINITY }],
  ['the text false', 'false'],
  ['the number 1', 1],
  ['a list', []],
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('settings')
})

afterEach(async () => {
  await workspace.dispose()
})

function driverWithoutWorkerEntry(): SQLiteDriver {
  const base = betterSqlite3()
  return defineDriver({ capabilities: base.capabilities, open: base.open })
}

describe('writerWorker settings', () => {
  it('refuses each malformed writerWorker value with INVALID_WRITER_WORKER, from an open call or the registry default, and creates no file', async () => {
    const sirannon = workspace.registry()

    for (const [label, value] of OUT_OF_RANGE) {
      const path = workspace.path('refused.db')
      const writerWorker = value as WriterWorkerOptions
      await expect(sirannon.open('refused', path, { writerWorker }), label).rejects.toMatchObject({
        code: 'INVALID_WRITER_WORKER',
      })
      await expect(workspace.registry({ writerWorker }).open('refused', path), label).rejects.toMatchObject({
        code: 'INVALID_WRITER_WORKER',
      })
      expect(sirannon.has('refused'), label).toBe(false)
      expect(existsSync(path), label).toBe(false)
    }

    const lowest = await sirannon.open('lowest', workspace.path('lowest.db'), {
      writerWorker: { maxPendingWrites: 1, writeTimeoutMs: 0, maxRestarts: 0 },
    })
    await lowest.execute('CREATE TABLE kept (n INTEGER)')
    await lowest.execute('INSERT INTO kept (n) VALUES (1)')
    expect(columnRaw(workspace.path('lowest.db'), 'SELECT n FROM kept')).toEqual([1n])
  })

  it('refuses to open a writable database with the worker on a driver that has no worker entry, and opens it read-only or with the worker off', async () => {
    const path = workspace.path('no-worker.db')
    const seed = new BetterSqlite3(path)
    seed.exec("CREATE TABLE kept (name TEXT); INSERT INTO kept (name) VALUES ('seeded')")
    seed.close()
    const fromOpen = workspace.registry({ driver: driverWithoutWorkerEntry() })
    const fromDefault = workspace.registry({ driver: driverWithoutWorkerEntry(), writerWorker: { maxRestarts: 1 } })

    await expect(fromOpen.open('writable', path, { writerWorker: true })).rejects.toMatchObject({
      code: 'WRITER_WORKER_UNSUPPORTED',
    })
    await expect(fromDefault.open('writable', path)).rejects.toMatchObject({ code: 'WRITER_WORKER_UNSUPPORTED' })
    await expect(
      fromOpen.open('missing', workspace.path('never-created.db'), { writerWorker: true }),
    ).rejects.toMatchObject({ code: 'WRITER_WORKER_UNSUPPORTED' })
    expect(existsSync(workspace.path('never-created.db'))).toBe(false)
    expect(fromOpen.has('writable') || fromDefault.has('writable')).toBe(false)

    const readOnly = await fromDefault.open('read-only', path, { readOnly: true })
    expect(await readOnly.query('SELECT name FROM kept')).toEqual([{ name: 'seeded' }])
    await readOnly.close()
    const workerOff = await fromDefault.open('worker-off', path, { writerWorker: false })
    await workerOff.execute("INSERT INTO kept (name) VALUES ('written')")
    expect(columnRaw(path, 'SELECT name FROM kept ORDER BY rowid')).toEqual(['seeded', 'written'])
  })
})
