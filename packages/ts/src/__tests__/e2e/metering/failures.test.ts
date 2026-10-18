import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Sirannon } from '../../../core/sirannon.js'
import type { DatabaseOptions } from '../../../core/types.js'
import {
  heldAfterSqlite,
  heldStatementReached,
  meteringFaultDriver,
  releaseHeldStatement,
} from '../../helpers/fault-drivers/metering-fault-driver.js'
import { SIRANNON_SQLITE_REQUIRED } from '../../helpers/sirannon-sqlite.js'
import {
  errorCodeOf,
  postJson,
  RawSocket,
  releaseServedRegistries,
  type ServedRegistry,
  scratchDirectory,
  serveRegistry,
  waitUntil,
} from '../lib/served-registry.js'
import {
  billedAs,
  indexesOn,
  Meter,
  meteredRegistry,
  rowsIn,
  SIRANNON_SQLITE_BINDING,
  scansWholeTable,
  TOPOLOGIES,
} from './metered-registry.js'
import { readInSeparateProcess } from './oracle.js'

const SEEDED_EVENTS = 400
const MALFORMED_EVENT = 137
const EVENTS_SCHEMA = [
  'CREATE TABLE events (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, tag TEXT NOT NULL)',
  'CREATE UNIQUE INDEX events_by_tag ON events (tag)',
  'CREATE TABLE archive (id INTEGER PRIMARY KEY, payload TEXT NOT NULL CHECK (json_valid(payload)))',
  'CREATE INDEX archive_by_payload ON archive (payload)',
]
const READ_EVERY_PAYLOAD = "SELECT id, json_extract(payload, '$.n') AS n FROM events"
const ARCHIVE_EVERY_EVENT = 'INSERT INTO archive (id, payload) SELECT id, payload FROM events'
const RETAG_FROM = 'UPDATE events SET tag = ? WHERE id >= ?'
const INSERT_EVENT = 'INSERT INTO events (payload, tag) VALUES (?, ?)'
const WRITER_DEADLINE_MS = 500

let dir: string
let meter: Meter
let sirannon: Sirannon
let served: ServedRegistry
let path: string

async function openEvents(options: DatabaseOptions): Promise<void> {
  path = join(dir, 'ledger.db')
  const db = await sirannon.open('ledger', path, options)
  for (const statement of EVENTS_SCHEMA) await db.execute(statement)
  await db.bulkLoad(
    INSERT_EVENT,
    Array.from({ length: SEEDED_EVENTS }, (_, index) => [
      index + 1 === MALFORMED_EVENT ? '{"n": ' : JSON.stringify({ n: index }),
      `tag-${index}`,
    ]),
  )
  served = await serveRegistry(sirannon, { acceptSql: true })
  meter.clear()
}

async function firstMalformedEvent(): Promise<{ id: number; before: number }> {
  const [first] = await readInSeparateProcess<{ id: bigint }>(
    path,
    'SELECT min(id) AS id FROM events WHERE json_valid(payload) = 0',
  )
  const [earlier] = await readInSeparateProcess<{ n: bigint }>(path, 'SELECT count(*) AS n FROM events WHERE id < ?', [
    first?.id,
  ])
  return { id: Number(first?.id), before: Number(earlier?.n) }
}

describe("Sirannon's SQLite build", () => {
  it.skipIf(!SIRANNON_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(SIRANNON_SQLITE_BINDING).not.toBeNull()
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering statements that fail partway', () => {
  afterEach(releaseServedRegistries)

  describe.each(TOPOLOGIES)('with $label', topology => {
    beforeEach(async () => {
      dir = scratchDirectory('metered-failures')
      meter = new Meter()
      sirannon = meteredRegistry(meter)
      await openEvents(topology.options)
    })

    it('bills a scan that a runtime error stops for every row it read up to the bad row, over HTTP and WebSocket', async () => {
      const malformed = await firstMalformedEvent()
      expect(malformed.id).toBe(MALFORMED_EVENT)
      expect(await scansWholeTable(path, READ_EVERY_PAYLOAD, [], 'events')).toBe(true)

      const overHttp = await postJson(served.base, '/db/ledger/query', { sql: READ_EVERY_PAYLOAD })
      const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
      const overWs = await ws.request({ id: 'scan', type: 'query', sql: READ_EVERY_PAYLOAD })

      expect([overHttp.status, errorCodeOf(overHttp.body)]).toEqual([400, 'QUERY_ERROR'])
      expect(overWs.error?.code).toBe('QUERY_ERROR')
      const stoppedScan = { rowsRead: malformed.before + 1, rowsWritten: 0 }
      expect(meter.billed('ledger')).toEqual([
        billedAs(READ_EVERY_PAYLOAD, true, stoppedScan),
        billedAs(READ_EVERY_PAYLOAD, true, stoppedScan),
      ])
    })

    it('bills an INSERT ... SELECT that a CHECK constraint stops for each row it read, each rowid it probed, and each row and index entry it wrote before the stop', async () => {
      const malformed = await firstMalformedEvent()
      expect(await readInSeparateProcess(path, `EXPLAIN QUERY PLAN ${ARCHIVE_EVERY_EVENT}`)).toEqual([
        expect.objectContaining({ detail: 'SCAN events' }),
      ])
      const archiveEntriesPerRow = 1 + (await indexesOn(path, 'archive'))
      const sourceRowsScanned = malformed.before + 1
      const explicitRowidLookups = malformed.before

      const answer = await postJson(served.base, '/db/ledger/execute', { sql: ARCHIVE_EVERY_EVENT })

      expect([answer.status, errorCodeOf(answer.body)]).toEqual([400, 'QUERY_ERROR'])
      expect(await rowsIn(path, 'archive')).toBe(0)
      expect(meter.billed('ledger')).toEqual([
        billedAs(ARCHIVE_EVERY_EVENT, true, {
          rowsRead: sourceRowsScanned + explicitRowidLookups,
          rowsWritten: malformed.before * archiveEntriesPerRow,
        }),
      ])
    })

    it('bills an UPDATE that a UNIQUE constraint stops on its second row for the range seek, one step, and the first row it rewrote', async () => {
      const start = SEEDED_EVENTS - 10
      const indexesHoldingTag = 1

      const answer = await postJson(served.base, '/db/ledger/execute', { sql: RETAG_FROM, params: ['same', start] })
      const ws = await RawSocket.open(`${served.wsBase}/db/ledger`)
      const overWs = await ws.request({ id: 'retag', type: 'execute', sql: RETAG_FROM, params: ['same', start] })

      expect([answer.status, errorCodeOf(answer.body)]).toEqual([400, 'QUERY_ERROR'])
      expect(overWs.error?.code).toBe('QUERY_ERROR')
      const [stored] = await readInSeparateProcess<{ n: bigint }>(
        path,
        "SELECT count(*) AS n FROM events WHERE tag = 'same'",
      )
      expect(stored?.n).toBe(0n)
      const twoRowsReadOneRewritten = { rowsRead: 2, rowsWritten: 1 + indexesHoldingTag }
      expect(meter.billed('ledger')).toEqual([
        billedAs(RETAG_FROM, true, twoRowsReadOneRewritten),
        billedAs(RETAG_FROM, true, twoRowsReadOneRewritten),
      ])
    })
  })
})

describe.skipIf(SIRANNON_SQLITE_BINDING === null)('metering statements that hit a configured deadline or limit', () => {
  let releaseFile: string

  beforeEach(() => {
    dir = scratchDirectory('metered-deadlines')
    releaseFile = join(dir, 'release')
    meter = new Meter()
    sirannon = meteredRegistry(meter, {
      driver: meteringFaultDriver({ sqliteBindingPath: SIRANNON_SQLITE_BINDING ?? '' }),
    })
  })

  afterEach(async () => {
    releaseHeldStatement(releaseFile)
    await releaseServedRegistries()
  })

  it('bills a write that outlives the writer deadline once SQLite finishes it, in one report, and bills the reads beside it and every later write exactly', async () => {
    await openEvents({ writerWorker: { writeTimeoutMs: WRITER_DEADLINE_MS }, readPoolSize: 1 })
    const heldInsert = heldAfterSqlite(INSERT_EVENT, releaseFile)
    const countEvents = 'SELECT count(*) AS n FROM events'

    const timedOutWrite = postJson(served.base, '/db/ledger/execute', { sql: heldInsert, params: ['{}', 'held'] })
    await waitUntil(() => heldStatementReached(releaseFile), 'the held write reaching SQLite')
    const rowsWhileHeld = await rowsIn(path, 'events')
    const readWhileHeld = await postJson(served.base, '/db/ledger/query', { sql: countEvents })
    const timedOut = await timedOutWrite
    const billedBeforeRelease = meter.billed('ledger')
    releaseHeldStatement(releaseFile)
    const inserted = await postJson(served.base, '/db/ledger/execute', { sql: INSERT_EVENT, params: ['{}', 'later'] })
    await waitUntil(() => meter.of('ledger').length >= 3, 'the late write reaching onQueryComplete')

    expect(rowsWhileHeld).toBe(SEEDED_EVENTS + 1)
    expect([readWhileHeld.status, timedOut.status, inserted.status]).toEqual([200, 500, 200])
    expect(errorCodeOf(timedOut.body)).toBe('WRITER_WORKER_TIMEOUT')
    expect(await rowsIn(path, 'events')).toBe(SEEDED_EVENTS + 2)
    const insertCounts = { rowsRead: 0, rowsWritten: 1 + (await indexesOn(path, 'events')) }
    expect(billedBeforeRelease).toEqual([billedAs(countEvents, false, { rowsRead: rowsWhileHeld, rowsWritten: 0 })])
    expect(meter.billed('ledger')).toEqual([
      billedAs(countEvents, false, { rowsRead: rowsWhileHeld, rowsWritten: 0 }),
      billedAs(heldInsert, true, insertCounts),
      billedAs(INSERT_EVENT, false, insertCounts),
    ])
  })

  it('reports nothing for a write that maxPendingWrites refuses before it starts, and bills the write that was in flight', async () => {
    await openEvents({ writerWorker: { maxPendingWrites: 1 }, readPoolSize: 2 })
    const heldInsert = heldAfterSqlite(INSERT_EVENT, releaseFile)

    const inFlight = postJson(served.base, '/db/ledger/execute', { sql: heldInsert, params: ['{}', 'in-flight'] })
    await waitUntil(() => heldStatementReached(releaseFile), 'the held write reaching SQLite')
    const refused = await Promise.all(
      Array.from({ length: 5 }, (_, index) =>
        postJson(served.base, '/db/ledger/execute', { sql: INSERT_EVENT, params: ['{}', `refused-${index}`] }),
      ),
    )
    const billedWhileHeld = meter.reports.length
    releaseHeldStatement(releaseFile)
    const settled = await inFlight

    for (const answer of refused) expect([answer.status, errorCodeOf(answer.body)]).toEqual([503, 'WRITE_OVERLOADED'])
    expect(settled.status).toBe(200)
    expect(billedWhileHeld).toBe(0)
    expect(await rowsIn(path, 'events')).toBe(SEEDED_EVENTS + 1)
    expect(meter.billed('ledger')).toEqual([
      billedAs(heldInsert, false, { rowsRead: 0, rowsWritten: 1 + (await indexesOn(path, 'events')) }),
    ])
  })
})
