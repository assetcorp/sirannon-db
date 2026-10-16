import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { QueryMetrics } from '../../../core/types.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { builtSirannonSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'

const LEDGER_ROWS = 100
const INCOMING_ROWS = 100
const GROUPS = 10
const LEDGER_SCHEMA = 'CREATE TABLE ledger (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL)'
const INCOMING_SCHEMA = 'CREATE TABLE incoming (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL)'
const INSERT_LEDGER = 'INSERT INTO ledger (id, grp) VALUES (?, ?)'
const INSERT_INCOMING = 'INSERT INTO incoming (id, grp) VALUES (?, ?)'
const COPY_INCOMING_BELOW_GROUP = 'INSERT INTO ledger (id, grp) SELECT id, grp FROM incoming WHERE grp < ?'
const DELETE_GROUP = 'DELETE FROM ledger WHERE grp = ?'
const DELETE_ABOVE_ID = 'DELETE FROM ledger WHERE id > ?'
const DELETE_BY_ID = 'DELETE FROM ledger WHERE id = ?'
const MOVE_GROUP = 'UPDATE ledger SET grp = grp + 100 WHERE grp = ?'
const DELETE_ALL = 'DELETE FROM ledger'

const binding = builtSirannonSqliteBindingPath()

const TOPOLOGIES = [
  { label: 'writes on the calling thread', writerWorker: false },
  { label: 'writes on the writer worker', writerWorker: true },
]

interface Expected {
  sql: string
  params: unknown[]
  changes: number
  plainRead: number
  watchedRead: number
}

let dir: string
let sirannon: Sirannon
let reported: QueryMetrics[]

function reportFor(databaseId: string, sql: string): QueryMetrics {
  const matching = reported.filter(metrics => metrics.databaseId === databaseId && metrics.sql === sql)
  expect(matching, `${databaseId}: ${sql}`).toHaveLength(1)
  return matching[0]
}

function rowsWhere(rows: Map<number, number>, keep: (id: number, grp: number) => boolean): number[] {
  return [...rows].filter(([id, grp]) => keep(id, grp)).map(([id]) => id)
}

function expectedCounts(): Expected[] {
  const ledger = new Map<number, number>()
  for (let id = 1; id <= LEDGER_ROWS; id++) ledger.set(id, id % GROUPS)
  const incoming = new Map<number, number>()
  for (let id = LEDGER_ROWS + 1; id <= LEDGER_ROWS + INCOMING_ROWS; id++) incoming.set(id, id % GROUPS)
  const steps: Expected[] = []

  const copied = rowsWhere(incoming, (_, grp) => grp < 5)
  for (const id of copied) ledger.set(id, incoming.get(id) ?? -1)
  steps.push({
    sql: COPY_INCOMING_BELOW_GROUP,
    params: [5],
    changes: copied.length,
    plainRead: incoming.size + copied.length,
    watchedRead: incoming.size + copied.length * 2,
  })

  const scannedForGroup = ledger.size
  const inGroup = rowsWhere(ledger, (_, grp) => grp === 3)
  for (const id of inGroup) ledger.delete(id)
  steps.push({
    sql: DELETE_GROUP,
    params: [3],
    changes: inGroup.length,
    plainRead: scannedForGroup,
    watchedRead: scannedForGroup + inGroup.length,
  })

  const aboveId = rowsWhere(ledger, id => id > 150)
  for (const id of aboveId) ledger.delete(id)
  steps.push({
    sql: DELETE_ABOVE_ID,
    params: [150],
    changes: aboveId.length,
    plainRead: aboveId.length,
    watchedRead: aboveId.length * 2,
  })

  ledger.delete(7)
  steps.push({ sql: DELETE_BY_ID, params: [7], changes: 1, plainRead: 1, watchedRead: 1 })

  const scannedForMove = ledger.size
  const moved = rowsWhere(ledger, (_, grp) => grp === 2)
  for (const id of moved) ledger.set(id, (ledger.get(id) ?? -1) + 100)
  steps.push({
    sql: MOVE_GROUP,
    params: [2],
    changes: moved.length,
    plainRead: scannedForMove,
    watchedRead: scannedForMove + moved.length * 2,
  })

  steps.push({ sql: DELETE_ALL, params: [], changes: ledger.size, plainRead: 0, watchedRead: ledger.size * 2 })
  return steps
}

async function openLedger(name: string, writerWorker: boolean, watched: boolean): Promise<Database> {
  const db = await sirannon.open(name, join(dir, `${name}.db`), { writerWorker })
  await db.execute(LEDGER_SCHEMA)
  await db.execute(INCOMING_SCHEMA)
  await db.bulkLoad(
    INSERT_LEDGER,
    Array.from({ length: LEDGER_ROWS }, (_, index) => [index + 1, (index + 1) % GROUPS]),
  )
  await db.bulkLoad(
    INSERT_INCOMING,
    Array.from({ length: INCOMING_ROWS }, (_, index) => [LEDGER_ROWS + index + 1, (LEDGER_ROWS + index + 1) % GROUPS]),
  )
  if (watched) await db.watch('ledger')
  return db
}

function readRaw<T>(path: string, sql: string): T[] {
  const raw = new BetterSqlite3(path, { readonly: true })
  try {
    return raw.prepare(sql).all() as T[]
  } finally {
    raw.close()
  }
}

describe.skipIf(binding === null)('rows read on a watched table', () => {
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'sirannon-watched-counts-'))
    reported = []
    sirannon = new Sirannon({
      driver: betterSqlite3({ rowCounts: true, sqliteBindingPath: binding ?? undefined }),
      metrics: { onQueryComplete: metrics => reported.push(metrics) },
    })
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(dir, { recursive: true, force: true })
  })

  describe.each(TOPOLOGIES)('with $label', topology => {
    it('counts the second pass that SQLite makes over the rows of a multi-row DELETE or INSERT ... SELECT', async () => {
      const plain = await openLedger('plain', topology.writerWorker, false)
      const watched = await openLedger('watched', topology.writerWorker, true)
      const steps = expectedCounts()

      for (const step of steps) {
        const plainResult = await plain.execute(step.sql, step.params)
        const watchedResult = await watched.execute(step.sql, step.params)
        expect(plainResult.changes, step.sql).toBe(step.changes)
        expect(watchedResult.changes, step.sql).toBe(step.changes)
      }

      for (const step of steps) {
        const plainReport = reportFor('plain', step.sql)
        const watchedReport = reportFor('watched', step.sql)
        expect(plainReport.rowsRead, step.sql).toBe(step.plainRead)
        expect(watchedReport.rowsRead, step.sql).toBe(step.watchedRead)
        expect(plainReport.rowsWritten, step.sql).toBe(step.changes)
        expect(watchedReport.rowsWritten, step.sql).toBe(step.changes)
      }

      const logged = readRaw<{ operation: string; n: number }>(
        join(dir, 'watched.db'),
        "SELECT operation, count(*) AS n FROM _sirannon_changes WHERE table_name = 'ledger' GROUP BY operation ORDER BY operation",
      )
      const deleted = steps.filter(step => step.sql.startsWith('DELETE')).reduce((sum, step) => sum + step.changes, 0)
      expect(logged).toEqual([
        { operation: 'DELETE', n: deleted },
        { operation: 'INSERT', n: steps[0].changes },
        { operation: 'UPDATE', n: steps[4].changes },
      ])
    })

    it('keeps counting and capturing after a migration drops a column of a watched table', async () => {
      const name = 'migrated'
      const path = join(dir, `${name}.db`)
      const db = await sirannon.open(name, path, { writerWorker: topology.writerWorker })
      await db.execute('CREATE TABLE ledger (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, note TEXT)')
      await db.execute("INSERT INTO ledger (id, grp, note) VALUES (1, 1, 'first')")
      await db.watch('ledger')

      const result = await db.migrate([
        {
          version: 1,
          name: 'drop_ledger_note',
          up: async tx => {
            await tx.execute('ALTER TABLE ledger DROP COLUMN note')
            await tx.executeBatch(INSERT_LEDGER, [
              [2, 2],
              [3, 3],
            ])
            expect(await tx.query('SELECT id, grp FROM ledger ORDER BY id')).toEqual([
              { id: 1, grp: 1 },
              { id: 2, grp: 2 },
              { id: 3, grp: 3 },
            ])
          },
        },
      ])
      expect(result.applied.map(migration => migration.version)).toEqual([1])

      await db.execute(INSERT_LEDGER, [4, 4])
      await db.execute(DELETE_GROUP, [1])
      expect(reportFor(name, INSERT_LEDGER)).toMatchObject({ changes: 1, rowsRead: 1, rowsWritten: 1 })
      expect(reportFor(name, DELETE_GROUP)).toMatchObject({ changes: 1, rowsRead: 5, rowsWritten: 1 })

      const columns = readRaw<{ name: string }>(path, "SELECT name FROM pragma_table_info('ledger') ORDER BY cid")
      expect(columns.map(column => column.name)).toEqual(['id', 'grp'])
      const logged = readRaw<{ operation: string; new_data: string | null; old_data: string | null }>(
        path,
        "SELECT operation, new_data, old_data FROM _sirannon_changes WHERE table_name = 'ledger' ORDER BY seq",
      )
      expect(logged.map(change => change.operation)).toEqual(['INSERT', 'INSERT', 'INSERT', 'DELETE'])
      expect(JSON.parse(logged[2].new_data ?? 'null')).toEqual({ id: 4, grp: 4 })
      expect(JSON.parse(logged[3].old_data ?? 'null')).toEqual({ id: 1, grp: 1 })
    })
  })
})
