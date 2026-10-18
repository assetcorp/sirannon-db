import { performance } from 'node:perf_hooks'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { WriteOverloadError } from '../../../core/errors.js'
import type { WriterWorkerOptions } from '../../../core/types.js'
import { heldSql, releaseHeldWrite, sleepingDriver } from '../../helpers/fault-drivers/sleeping-driver.js'
import {
  codeOf,
  columnRaw,
  createWorkspace,
  eventually,
  HELD_WRITE_DEADLINE_MS,
  nextTurn,
  type Workspace,
} from './harness.js'

const PENDING_LIMIT = 3
const SPEC_RETRY_AFTER_MS = 1_000
const INSIDE_THE_GRACE_WINDOW_MS = HELD_WRITE_DEADLINE_MS * 1.5
const TIMER_TOLERANCE_MS = 50
const INSERT_MARKER = 'INSERT INTO markers (n) VALUES (?)'

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('queue')
})

afterEach(async () => {
  await workspace.dispose()
})

async function openMarkers(name: string, writerWorker: WriterWorkerOptions): Promise<{ db: Database; path: string }> {
  const path = workspace.path(`${name}.db`)
  const db = await workspace.registry({ driver: sleepingDriver() }).open(name, path, { writerWorker })
  await db.execute('CREATE TABLE markers (n INTEGER NOT NULL)')
  return { db, path }
}

function storedMarkers(path: string): unknown[] {
  return columnRaw(path, 'SELECT n FROM markers ORDER BY n')
}

async function expectServing(db: Database, marker: number): Promise<void> {
  await eventually(async () => {
    try {
      await db.execute(INSERT_MARKER, [marker])
      return true
    } catch {
      return false
    }
  }, 'the writer worker to accept a write')
}

describe('the writer queue bound', () => {
  it('admits writes up to maxPendingWrites in flight and refuses every write past it with a retryable WRITE_OVERLOADED, executing none of them', async () => {
    const { db, path } = await openMarkers('bound', { maxPendingWrites: PENDING_LIMIT, writeTimeoutMs: 0 })

    const atTheLimit = await Promise.allSettled(
      Array.from({ length: PENDING_LIMIT }, (_, index) => db.execute(INSERT_MARKER, [index + 1])),
    )
    const pastTheLimit = await Promise.allSettled(
      Array.from({ length: PENDING_LIMIT + 1 }, (_, index) => db.execute(INSERT_MARKER, [index + 11])),
    )
    expect(atTheLimit.map(codeOf)).toEqual(['ok', 'ok', 'ok'])
    expect(pastTheLimit.map(codeOf)).toEqual(['ok', 'ok', 'ok', 'WRITE_OVERLOADED'])

    const release = workspace.releaseFile('bound')
    const inFlight = [
      db.execute(heldSql('INSERT INTO markers (n) VALUES (21)', release)),
      db.execute(INSERT_MARKER, [22]),
      db.execute(INSERT_MARKER, [23]),
    ]
    await nextTurn()
    let callbackRan = false
    const refused = await Promise.allSettled([
      db.execute(INSERT_MARKER, [31]),
      db.executeBatch(INSERT_MARKER, [[32], [33]]),
      db.executeTransaction([{ sql: INSERT_MARKER, params: [34] }]),
      db.transaction(async tx => {
        callbackRan = true
        await tx.execute(INSERT_MARKER, [35])
      }),
      db.bulkLoad(INSERT_MARKER, [[36]]),
    ])
    const readWhileFull = await db.query<{ n: number }>('SELECT count(*) AS n FROM markers')

    expect(refused.map(codeOf)).toEqual(Array(5).fill('WRITE_OVERLOADED'))
    for (const outcome of refused) {
      expect(outcome.status === 'rejected' && outcome.reason).toBeInstanceOf(WriteOverloadError)
      expect(outcome).toMatchObject({ reason: { limit: PENDING_LIMIT, retryAfterMs: SPEC_RETRY_AFTER_MS } })
    }
    expect(callbackRan).toBe(false)
    expect(readWhileFull).toEqual([{ n: 6 }])

    releaseHeldWrite(release)
    expect((await Promise.allSettled(inFlight)).map(codeOf)).toEqual(['ok', 'ok', 'ok'])
    await expect(db.execute(INSERT_MARKER, [41])).resolves.toMatchObject({ changes: 1 })
    expect(storedMarkers(path)).toEqual([1n, 2n, 3n, 11n, 12n, 13n, 21n, 22n, 23n, 41n])
  })
})

describe('the writer worker deadline', () => {
  it('delivers the result of a write that finishes after its deadline but inside the grace window', async () => {
    const { db, path } = await openMarkers('late', { writeTimeoutMs: HELD_WRITE_DEADLINE_MS })
    const release = workspace.releaseFile('late')
    setTimeout(() => releaseHeldWrite(release), INSIDE_THE_GRACE_WINDOW_MS)

    const started = performance.now()
    await expect(db.execute(heldSql('INSERT INTO markers (n) VALUES (1)', release))).resolves.toMatchObject({
      changes: 1,
    })

    expect(performance.now() - started).toBeGreaterThan(HELD_WRITE_DEADLINE_MS)
    expect(storedMarkers(path)).toEqual([1n])
  }, 30_000)

  it('rejects a write still running at twice its deadline with WRITER_WORKER_TIMEOUT, then commits it once the worker finishes and keeps accepting writes', async () => {
    const { db, path } = await openMarkers('stalled', { writeTimeoutMs: HELD_WRITE_DEADLINE_MS })
    const release = workspace.releaseFile('stalled')

    const started = performance.now()
    const stalled = await db.execute(heldSql('INSERT INTO markers (n) VALUES (1)', release)).catch(error => error)
    const waited = performance.now() - started

    expect(stalled).toMatchObject({ code: 'WRITER_WORKER_TIMEOUT' })
    expect(waited).toBeGreaterThan(HELD_WRITE_DEADLINE_MS * 2 - TIMER_TOLERANCE_MS)
    expect(storedMarkers(path)).toEqual([])

    releaseHeldWrite(release)
    await eventually(() => storedMarkers(path).length === 1, 'the held write to commit')
    await expect(db.execute(INSERT_MARKER, [2])).resolves.toMatchObject({ changes: 1 })
    expect(storedMarkers(path)).toEqual([1n, 2n])
  }, 30_000)

  it('never applies a queued write whose deadline passes before the worker reaches it, and rejects it with the retryable WRITE_OVERLOADED when the worker frees up inside the grace window', async () => {
    const outcomes: string[] = []
    for (const freesInsideGrace of [true, false]) {
      const name = freesInsideGrace ? 'shed' : 'abandoned'
      const { db, path } = await openMarkers(name, { writeTimeoutMs: HELD_WRITE_DEADLINE_MS })
      const release = workspace.releaseFile(name)

      const held = db.execute(heldSql('INSERT INTO markers (n) VALUES (0)', release)).catch(error => error)
      await nextTurn()
      const queued = db.execute(INSERT_MARKER, [1]).catch(error => error)
      expect(await held).toMatchObject({ code: 'WRITER_WORKER_TIMEOUT' })
      if (freesInsideGrace) setTimeout(() => releaseHeldWrite(release), INSIDE_THE_GRACE_WINDOW_MS)
      const queuedError = await queued
      if (!freesInsideGrace) releaseHeldWrite(release)
      outcomes.push(String(queuedError.code))

      await expectServing(db, 2)
      expect(storedMarkers(path), name).toEqual([0n, 2n])
    }
    expect(outcomes).toEqual(['WRITE_OVERLOADED', 'WRITER_WORKER_TIMEOUT'])
  }, 60_000)
})
