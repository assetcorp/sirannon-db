import { threadId } from 'node:worker_threads'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { WriteOverloadError } from '../../../core/errors.js'
import type { DatabaseOptions, SirannonOptions } from '../../../core/types.js'
import { heldSql, releaseHeldWrite, sleepingDriver } from '../../helpers/fault-drivers/sleeping-driver.js'
import { EXECUTING_THREAD_ID, threadRecordingDriver } from '../../helpers/fault-drivers/thread-recording-driver.js'
import { columnRaw, createWorkspace, nextTurn, readRaw, type Workspace } from './harness.js'

const PLACEMENTS_SCHEMA = 'CREATE TABLE placements (id INTEGER PRIMARY KEY, via TEXT NOT NULL, thread INTEGER NOT NULL)'
const RECORD_THREAD = `INSERT INTO placements (via, thread) VALUES (?, ${EXECUTING_THREAD_ID})`
const SELECT_THREAD = `SELECT ${EXECUTING_THREAD_ID} AS thread`
const BURST = 20
const CALLING_THREAD = BigInt(threadId)

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('placement')
})

afterEach(async () => {
  await workspace.dispose()
})

async function openRecorder(
  name: string,
  registryOptions: Partial<SirannonOptions>,
  options?: DatabaseOptions,
): Promise<{ db: Database; path: string }> {
  const path = workspace.path(`${name}.db`)
  const db = await workspace.registry({ driver: threadRecordingDriver(), ...registryOptions }).open(name, path, options)
  await db.execute(PLACEMENTS_SCHEMA)
  return { db, path }
}

function writerThreadOf(path: string, via: string): bigint {
  const threads = columnRaw(path, 'SELECT DISTINCT thread FROM placements WHERE via = ?', [via])
  expect(threads, via).toHaveLength(1)
  return threads[0] as bigint
}

describe('where the writer worker executes statements', () => {
  it('executes every kind of write on one worker thread, and every read outside a transaction on the calling thread', async () => {
    const { db, path } = await openRecorder('every-write', {}, { writerWorker: true })

    await db.execute(RECORD_THREAD, ['execute'])
    await Promise.all(Array.from({ length: BURST }, () => db.execute(RECORD_THREAD, ['burst'])))
    await db.executeBatch(RECORD_THREAD, [['batch'], ['batch']])
    await db.executeTransaction([{ sql: RECORD_THREAD, params: ['list'] }])
    const readInsideCallback = await db.transaction(async tx => {
      await tx.execute(RECORD_THREAD, ['callback'])
      return tx.query<{ thread: bigint | number }>(SELECT_THREAD)
    })
    await db.bulkLoad(RECORD_THREAD, [['load'], ['load']])

    const rows = readRaw<{ via: string; thread: bigint }>(path, 'SELECT via, thread FROM placements ORDER BY id')
    expect(rows.map(row => row.via)).toEqual([
      'execute',
      ...Array(BURST).fill('burst'),
      'batch',
      'batch',
      'list',
      'callback',
      'load',
      'load',
    ])
    const writerThreads = new Set(rows.map(row => row.thread))
    expect(writerThreads.size).toBe(1)
    expect(writerThreads.has(CALLING_THREAD)).toBe(false)
    const [workerThread] = writerThreads
    expect(readInsideCallback.map(row => BigInt(row.thread))).toEqual([workerThread])
    for (let read = 0; read < 6; read++) {
      expect((await db.query<{ thread: number }>(SELECT_THREAD)).map(row => BigInt(row.thread))).toEqual([
        CALLING_THREAD,
      ])
    }
    expect(await db.queryOne<{ thread: number }>(SELECT_THREAD)).toEqual({ thread: threadId })
  })

  it('follows the registry default, lets each open call override it, and writes on the calling thread when it is off', async () => {
    const inherits = await openRecorder('inherits', { writerWorker: true })
    const optsOut = await openRecorder('opts-out', { writerWorker: true }, { writerWorker: false })
    const emptyOptions = await openRecorder('empty-options', {}, { writerWorker: {} })
    const unset = await openRecorder('unset', {})
    const inheritsLimits = await openRecorder('inherits-limits', { writerWorker: { maxPendingWrites: 1 } })

    for (const { db } of [inherits, optsOut, emptyOptions, unset]) await db.execute(RECORD_THREAD, ['write'])
    const [admitted, refused] = await Promise.allSettled([
      inheritsLimits.db.execute(RECORD_THREAD, ['write']),
      inheritsLimits.db.execute(RECORD_THREAD, ['write']),
    ])

    expect(writerThreadOf(inherits.path, 'write')).not.toBe(CALLING_THREAD)
    expect(writerThreadOf(emptyOptions.path, 'write')).not.toBe(CALLING_THREAD)
    expect(writerThreadOf(optsOut.path, 'write')).toBe(CALLING_THREAD)
    expect(writerThreadOf(unset.path, 'write')).toBe(CALLING_THREAD)
    expect(admitted).toMatchObject({ status: 'fulfilled', value: { changes: 1 } })
    expect(refused.status === 'rejected' && refused.reason).toBeInstanceOf(WriteOverloadError)
    expect(refused).toMatchObject({ reason: { code: 'WRITE_OVERLOADED', limit: 1 } })
    expect(writerThreadOf(inheritsLimits.path, 'write')).not.toBe(CALLING_THREAD)
    expect(columnRaw(inheritsLimits.path, 'SELECT count(*) FROM placements')).toEqual([1n])
  })

  it('keeps the calling thread free while the worker holds a write, and serves reads of the last commit meanwhile', async () => {
    const path = workspace.path('held.db')
    const release = workspace.releaseFile('held')
    const db = await workspace
      .registry({ driver: sleepingDriver() })
      .open('held', path, { writerWorker: { writeTimeoutMs: 0 } })
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.execute("INSERT INTO notes (body) VALUES ('committed')")

    let settled = false
    const held = db.execute(heldSql("INSERT INTO notes (body) VALUES ('held')", release)).finally(() => {
      settled = true
    })
    await nextTurn()
    const timerFired = await new Promise<boolean>(resolve => setTimeout(() => resolve(true), 0))
    const readsWhileHeld = await Promise.all([
      db.query<{ body: string }>('SELECT body FROM notes ORDER BY id'),
      db.queryOne<{ n: number }>('SELECT count(*) AS n FROM notes'),
    ])

    expect(timerFired).toBe(true)
    expect(settled).toBe(false)
    expect(readsWhileHeld).toEqual([[{ body: 'committed' }], { n: 1 }])
    expect(columnRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual(['committed'])

    releaseHeldWrite(release)
    await expect(held).resolves.toMatchObject({ changes: 1 })
    expect(columnRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual(['committed', 'held'])
  })
})
