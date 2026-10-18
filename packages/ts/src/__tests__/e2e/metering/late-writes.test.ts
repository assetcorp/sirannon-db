import { existsSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import {
  heldAfterSqlite,
  heldStatementReached,
  meteringFaultDriver,
  releaseHeldStatement,
} from '../../helpers/fault-drivers/metering-fault-driver.js'
import {
  errorCodeOf,
  postJson,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  waitUntil,
} from '../lib/served-registry.js'
import { billedAs, Meter, meteredRegistry, rowInserts, rowsIn, SIRANNON_SQLITE_BINDING } from './metered-registry.js'

const SEEDED_NOTES = 50
const WRITER_DEADLINE_MS = 2_000
const NOTES_SCHEMA = [
  'CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL, tag TEXT NOT NULL)',
  'CREATE UNIQUE INDEX notes_by_tag ON notes (tag)',
]
const INSERT_NOTE = 'INSERT INTO notes (body, tag) VALUES (?, ?)'

let dir: string
let releaseFile: string
let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry
let path: string

async function openNotes(): Promise<void> {
  path = join(dir, 'notes.db')
  const db = await sirannon.open('notes', path, { writerWorker: { writeTimeoutMs: WRITER_DEADLINE_MS } })
  for (const statement of NOTES_SCHEMA) await db.execute(statement)
  await db.bulkLoad(
    INSERT_NOTE,
    Array.from({ length: SEEDED_NOTES }, (_, index) => ['seeded', `seeded-${index}`]),
  )
  served = await serveRegistry(sirannon, { acceptSql: true })
  meter.clear()
}

async function timesOutWhileHeld(route: string, body: unknown): Promise<{ status: number; code: string | null }> {
  const answer = postJson(served.base, `/db/notes/${route}`, body)
  await waitUntil(() => heldStatementReached(releaseFile), 'the held statement reaching SQLite')
  const settled = await answer
  return { status: settled.status, code: errorCodeOf(settled.body) }
}

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('billing work that SQLite finishes after the writer deadline', () => {
  beforeEach(async () => {
    dir = scratchDirectory('metered-late-writes')
    releaseFile = join(dir, 'release')
    meter = new Meter()
    sirannon = meteredRegistry(meter, {
      driver: meteringFaultDriver({ sqliteBindingPath: SIRANNON_SQLITE_BINDING ?? '' }),
    })
    await openNotes()
  })

  afterEach(async () => {
    if (existsSync(dir)) releaseHeldStatement(releaseFile)
    await releaseServedRegistries()
  })

  it('bills a timed-out batch for every row that SQLite wrote before the rollback, in one report', async () => {
    const heldInsert = heldAfterSqlite(INSERT_NOTE, releaseFile)
    const paramsBatch = [
      ['late', 'late-1'],
      ['late', 'late-2'],
      ['late', 'late-3'],
    ]

    const timedOut = await timesOutWhileHeld('batch', { sql: heldInsert, paramsBatch })
    const billedWhileHeld = meter.reports.length
    releaseHeldStatement(releaseFile)
    await waitUntil(() => meter.of('notes').length >= 1, 'the late batch reaching onQueryComplete')
    const after = await postJson(served.base, '/db/notes/query', { sql: 'SELECT count(*) AS n FROM notes' })

    expect(timedOut).toEqual({ status: 500, code: 'WRITER_WORKER_TIMEOUT' })
    expect(billedWhileHeld).toBe(0)
    expect(after.status).toBe(200)
    expect(await rowsIn(path, 'notes')).toBe(SEEDED_NOTES)
    expect(meter.billed('notes')[0]).toEqual(
      billedAs(heldInsert, true, await rowInserts(path, 'notes', paramsBatch.length)),
    )
  })

  it('bills each statement of a timed-out group commit that committed after the deadline', async () => {
    const heldInsert = heldAfterSqlite(INSERT_NOTE, releaseFile)
    const statements = [
      { sql: INSERT_NOTE, params: ['late', 'late-first'] },
      { sql: heldInsert, params: ['late', 'late-second'] },
    ]

    const timedOut = await timesOutWhileHeld('transaction', { statements })
    const billedWhileHeld = meter.reports.length
    releaseHeldStatement(releaseFile)
    await waitUntil(() => meter.of('notes').length >= 2, 'the late group reaching onQueryComplete')

    const oneInsert = await rowInserts(path, 'notes')
    expect(timedOut).toEqual({ status: 500, code: 'WRITER_WORKER_TIMEOUT' })
    expect(billedWhileHeld).toBe(0)
    expect(await rowsIn(path, 'notes')).toBe(SEEDED_NOTES + 2)
    expect(meter.billed('notes')).toEqual([
      billedAs(INSERT_NOTE, true, oneInsert),
      billedAs(heldInsert, true, oneInsert),
    ])
  })

  it('reports a timed-out write without counts when the registry closes before SQLite finishes it', async () => {
    const heldInsert = heldAfterSqlite(INSERT_NOTE, releaseFile)

    const timedOut = await timesOutWhileHeld('execute', { sql: heldInsert, params: ['late', 'late-closed'] })
    const billedWhileHeld = meter.reports.length
    await releaseServedRegistries()

    expect(timedOut).toEqual({ status: 500, code: 'WRITER_WORKER_TIMEOUT' })
    expect(billedWhileHeld).toBe(0)
    const reports = meter.of('notes')
    expect(reports).toHaveLength(1)
    expect(reports[0]).toMatchObject({ sql: heldInsert, error: true })
    expect(reports[0]).not.toHaveProperty('rowsRead')
    expect(reports[0]).not.toHaveProperty('rowsWritten')
  })
})
