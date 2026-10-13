import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { ChangeEvent } from '../../../core/types.js'
import { decodeHlc, encodeHlc } from '../../e2e/lib/device-sync-oracle.js'
import {
  type ChangeLogRow,
  changeLog,
  createWorkspace,
  FAST_POLL_MS,
  metaValue,
  NOTES_DDL,
  tableExists,
  type Workspace,
  waitFor,
  writeRaw,
} from './harness.js'

const HEX_32 = /^[0-9a-f]{32}$/
const DAY_MS = 86_400_000

interface WritePath {
  label: string
  rows: number
  transactions: number
  write(db: Database): Promise<unknown>
}

const WRITE_PATHS: WritePath[] = [
  { label: 'execute', rows: 1, transactions: 1, write: db => db.execute("INSERT INTO notes (body) VALUES ('one')") },
  {
    label: 'concurrent execute calls',
    rows: 8,
    transactions: 8,
    write: db =>
      Promise.all(Array.from({ length: 8 }, (_, at) => db.execute('INSERT INTO notes (body) VALUES (?)', [`n${at}`]))),
  },
  {
    label: 'executeTransaction',
    rows: 2,
    transactions: 1,
    write: db =>
      db.executeTransaction([
        { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['a'] },
        { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['b'] },
      ]),
  },
  {
    label: 'executeBatch',
    rows: 3,
    transactions: 1,
    write: db => db.executeBatch('INSERT INTO notes (body) VALUES (?)', [['a'], ['b'], ['c']]),
  },
  {
    label: 'a transaction callback',
    rows: 2,
    transactions: 1,
    write: db =>
      db.transaction(async tx => {
        await tx.execute('INSERT INTO notes (body) VALUES (?)', ['a'])
        await tx.execute('UPDATE notes SET body = ? WHERE body = ?', ['b', 'a'])
      }),
  },
  {
    label: 'bulkLoad',
    rows: 5,
    transactions: 1,
    write: db =>
      db.bulkLoad(
        'INSERT INTO notes (body) VALUES (?)',
        Array.from({ length: 5 }, (_, at) => [`n${at}`]),
      ),
  },
]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('stamping')
})

afterEach(async () => {
  await workspace.dispose()
})

function expectStampedBy(rows: ChangeLogRow[], nodeId: string, from: number, to: number): void {
  expect(rows.length).toBeGreaterThan(0)
  for (const row of rows) {
    const clock = decodeHlc(row.hlc)
    expect([row.node_id, HEX_32.test(row.tx_id), clock?.nodeId]).toEqual([nodeId, true, nodeId])
    expect(clock?.wallMs).toBeGreaterThanOrEqual(from)
    expect(clock?.wallMs).toBeLessThanOrEqual(to)
  }
}

describe('stamping local writes', () => {
  it('stamps every write path with the node id, one transaction id per transaction, and a rising clock', async () => {
    for (const writerWorker of [false, true]) {
      for (const path of WRITE_PATHS) {
        const label = `${path.label} with writerWorker ${writerWorker}`
        const name = `${path.label.replace(/\W+/g, '-')}-${writerWorker}`
        const registry = workspace.registry()
        const db = await registry.open(name, workspace.path(name), { writerWorker })
        await db.execute(NOTES_DDL)
        await db.watch('notes')
        const before = Date.now()
        await path.write(db)
        const after = Date.now()
        await registry.shutdown()

        const file = workspace.path(name)
        const rows = changeLog(file)
        const nodeId = metaValue(file, 'node_id') ?? ''
        expect(nodeId, label).toMatch(HEX_32)
        expect(rows, label).toHaveLength(path.rows)
        expectStampedBy(rows, nodeId, before, after)
        expect(new Set(rows.map(row => row.tx_id)).size, label).toBe(path.transactions)
        const clocks = rows.map(row => row.hlc)
        expect(clocks, label).toEqual([...clocks].sort())
        for (const txId of new Set(rows.map(row => row.tx_id))) {
          expect(new Set(rows.filter(row => row.tx_id === txId).map(row => row.hlc)).size, label).toBe(1)
        }
        expect(metaValue(file, 'hlc_clock'), label).toBe(clocks[clocks.length - 1])
      }
    }
  })

  it('leaves no change and no clock movement behind a transaction that rolls back', async () => {
    const db = await workspace.openNotes('rolled-back')
    await db.execute("INSERT INTO notes (body) VALUES ('kept')")
    const file = workspace.path('rolled-back')
    const clockBefore = metaValue(file, 'hlc_clock')

    await expect(
      db.transaction(async tx => {
        await tx.execute("INSERT INTO notes (body) VALUES ('discarded')")
        throw new Error('the caller abandons the transaction')
      }),
    ).rejects.toThrow('the caller abandons the transaction')
    await expect(
      db.executeTransaction([
        { sql: "INSERT INTO notes (id, body) VALUES (50, 'first')" },
        { sql: "INSERT INTO notes (id, body) VALUES (50, 'duplicate key')" },
      ]),
    ).rejects.toThrow()

    expect(changeLog(file)).toHaveLength(1)
    expect(metaValue(file, 'hlc_clock')).toBe(clockBefore)
  })

  it('keeps the node id across reopens and never moves the clock backwards, whatever the file holds', async () => {
    const registry = workspace.registry()
    const file = workspace.path('reopened')
    const reopen = async () => {
      const db = await registry.open('reopened', file)
      await db.watch('notes')
      return db
    }
    const first = await registry.open('reopened', file)
    await first.execute(NOTES_DDL)
    await first.watch('notes')
    await first.execute("INSERT INTO notes (body) VALUES ('first')")
    await registry.close('reopened')
    const nodeId = metaValue(file, 'node_id') ?? ''

    const aheadInMeta = encodeHlc(Date.now() + DAY_MS, 7, nodeId)
    writeRaw(file, "UPDATE _sirannon_meta SET value = ? WHERE key = 'hlc_clock'", [aheadInMeta])
    await (await reopen()).execute("INSERT INTO notes (body) VALUES ('after a clock far ahead in the meta table')")
    await registry.close('reopened')

    const aheadInLog = encodeHlc(Date.now() + 2 * DAY_MS, 3, 'a'.repeat(32))
    writeRaw(
      file,
      "INSERT INTO _sirannon_changes (table_name, operation, row_id, new_data, node_id, tx_id, hlc) VALUES ('notes', 'INSERT', '999', '{}', ?, ?, ?)",
      ['a'.repeat(32), 'b'.repeat(32), aheadInLog],
    )
    writeRaw(file, "UPDATE _sirannon_meta SET value = 'not a clock' WHERE key = 'hlc_clock'")
    await (await reopen()).execute("INSERT INTO notes (body) VALUES ('after a corrupt clock')")
    await registry.close('reopened')

    const own = changeLog(file).filter(row => row.node_id === nodeId)
    expect(metaValue(file, 'node_id')).toBe(nodeId)
    expect(own).toHaveLength(3)
    expect(own[1].hlc > aheadInMeta).toBe(true)
    expect(own[2].hlc > aheadInLog).toBe(true)
    expect(decodeHlc(own[2].hlc)?.nodeId).toBe(nodeId)
    expect(metaValue(file, 'hlc_clock')).toBe(own[2].hlc)
  })

  it('creates no stamping state for a database whose tables nobody watches', async () => {
    const db = await workspace.open('unwatched')
    await db.execute(NOTES_DDL)
    for (const path of WRITE_PATHS) await path.write(db)
    const file = workspace.path('unwatched')

    expect(tableExists(file, '_sirannon_changes')).toBe(false)
    expect([metaValue(file, 'node_id'), metaValue(file, 'hlc_clock')]).toEqual([null, null])
  })

  it('reports on each change event the origin, clock, and transaction that the change log holds', async () => {
    const db = await workspace.openNotes('events', { cdcPollInterval: FAST_POLL_MS })
    const events: ChangeEvent[] = []
    db.on('notes').subscribe(event => events.push(event))

    await db.execute("INSERT INTO notes (id, body) VALUES (1, 'one')")
    await db.executeTransaction([
      { sql: "UPDATE notes SET body = 'two' WHERE id = 1" },
      { sql: 'DELETE FROM notes WHERE id = 1' },
    ])
    await waitFor(() => events.length >= 3, 'three change events')

    const logged = changeLog(workspace.path('events'))
    expect(events.map(event => [event.seq, event.origin, event.hlc, event.txId])).toEqual(
      logged.map(row => [row.seq, row.node_id, row.hlc, row.tx_id]),
    )
  })
})
