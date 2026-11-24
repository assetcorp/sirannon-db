import { execFileSync } from 'node:child_process'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  collect,
  createWorkspace,
  PLACEMENTS,
  pause,
  QUIET_PERIOD_MS,
  type Workspace,
  waitForCount,
} from './harness.js'

const CONNECTION_IDLE_TIMEOUT_MS = 40

const WRITE_FROM_ANOTHER_PROCESS = `
const Database = require('better-sqlite3')
const db = new Database(process.argv[1], { fileMustExist: true })
db.pragma('busy_timeout = 5000')
db.prepare("INSERT INTO orders (id, source) VALUES (?, 'another process')").run(Number(process.argv[2]))
db.close()
`

const READ_IDS_FROM_ANOTHER_PROCESS = `
const Database = require('better-sqlite3')
const db = new Database(process.argv[1], { readonly: true, fileMustExist: true })
process.stdout.write(JSON.stringify(db.prepare('SELECT id FROM orders ORDER BY id').pluck().all()))
db.close()
`

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('external-writes')
})

afterEach(async () => {
  await workspace.dispose()
})

function insertFromAnotherProcess(path: string, id: number): void {
  execFileSync(process.execPath, ['-e', WRITE_FROM_ANOTHER_PROCESS, path, String(id)], { cwd: process.cwd() })
}

function idsFromAnotherProcess(path: string): number[] {
  const output = execFileSync(process.execPath, ['-e', READ_IDS_FROM_ANOTHER_PROCESS, path], { cwd: process.cwd() })
  return JSON.parse(output.toString()) as number[]
}

describe.each(PLACEMENTS)('writes from another process with $label', placement => {
  it('reach a subscriber of an idle database, in commit order, alongside the writes made through Sirannon', async () => {
    const db = await workspace.open('shared', { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, source TEXT NOT NULL)')
    await db.watch('orders')
    const seen = collect<{ id: number; source: string }>(db.on('orders'))
    const path = workspace.path('shared')

    await db.execute("INSERT INTO orders VALUES (1, 'sirannon')")
    await waitForCount(seen, 1)
    for (const id of [2, 3]) {
      await pause(QUIET_PERIOD_MS)
      insertFromAnotherProcess(path, id)
      await waitForCount(seen, id)
    }
    await db.execute("INSERT INTO orders VALUES (4, 'sirannon')")
    insertFromAnotherProcess(path, 5)
    await waitForCount(seen, 5)

    expect(seen.events.map(event => [Number(event.row.id), event.row.source])).toEqual([
      [1, 'sirannon'],
      [2, 'another process'],
      [3, 'another process'],
      [4, 'sirannon'],
      [5, 'another process'],
    ])
    expect(seen.errors).toEqual([])
  })

  it('reach a subscriber, and every read and write succeeds, after the database has closed its idle connections', async () => {
    const db = await workspace.open(
      'idle',
      { writerWorker: placement.writerWorker, connectionIdleTimeout: CONNECTION_IDLE_TIMEOUT_MS },
      placement.driver(),
    )
    await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, source TEXT NOT NULL)')
    await db.watch('orders')
    const seen = collect<{ id: number; source: string }>(db.on('orders'))
    const path = workspace.path('idle')
    const ids = () => idsFromAnotherProcess(path)

    for (let round = 0; round < 3; round++) {
      const first = round * 10 + 1
      await pause(CONNECTION_IDLE_TIMEOUT_MS * 3)
      insertFromAnotherProcess(path, first)
      await waitForCount(seen, round * 4 + 1)
      await pause(CONNECTION_IDLE_TIMEOUT_MS * 3)
      const [read, firstWrite, secondWrite] = await Promise.all([
        db.query<{ id: number }>('SELECT id FROM orders ORDER BY id'),
        db.execute("INSERT INTO orders VALUES (?, 'sirannon')", [first + 1]),
        db.execute("INSERT INTO orders VALUES (?, 'sirannon')", [first + 2]),
        db.transaction(async tx => {
          await tx.execute("INSERT INTO orders VALUES (?, 'sirannon')", [first + 3])
        }),
      ])
      expect([firstWrite.changes, secondWrite.changes]).toEqual([1, 1])
      expect(ids()).toEqual(expect.arrayContaining(read.map(row => Number(row.id))))
      await waitForCount(seen, round * 4 + 4)
    }

    const committed = ids()
    expect(committed).toEqual([1, 2, 3, 4, 11, 12, 13, 14, 21, 22, 23, 24])
    expect(seen.events.map(event => Number(event.row.id)).sort((a, b) => a - b)).toEqual(committed)
    expect((await db.query<{ id: number }>('SELECT id FROM orders ORDER BY id')).map(row => Number(row.id))).toEqual(
      committed,
    )
    expect(seen.errors).toEqual([])
  })
})
