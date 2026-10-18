import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { Sirannon } from '../../../core/sirannon.js'
import type { ChangeEvent } from '../../../core/types.js'
import { EXIT_TABLE, exitingDriver } from '../../helpers/fault-drivers/exiting-driver.js'
import {
  CRASH_DEADLINE_MS,
  codeOf,
  columnRaw,
  createWorkspace,
  eventually,
  integrityRaw,
  nextTurn,
  readRaw,
  type Workspace,
} from './harness.js'

const CRASH = `INSERT INTO ${EXIT_TABLE} (n) VALUES (1)`
const INSERT_ENTRY = 'INSERT INTO entries (label) VALUES (?)'
const SQLITE_SYNCHRONOUS_FULL = 2
const HEX_IDENTITY = /^[0-9a-f]{32}$/

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('crashes')
})

afterEach(async () => {
  await workspace.dispose()
})

async function openEntries(
  name: string,
  maxRestarts?: number,
): Promise<{ sirannon: Sirannon; db: Database; path: string }> {
  const path = workspace.path(`${name}.db`)
  const sirannon = workspace.registry({ driver: exitingDriver() })
  const db = await sirannon.open(name, path, {
    synchronous: 'full',
    cdcPollInterval: 10,
    writerWorker: { writeTimeoutMs: CRASH_DEADLINE_MS, ...(maxRestarts === undefined ? {} : { maxRestarts }) },
  })
  await db.execute('CREATE TABLE entries (id INTEGER PRIMARY KEY, label TEXT NOT NULL)')
  await db.execute(INSERT_ENTRY, ['before the crash'])
  return { sirannon, db, path }
}

function storedLabels(path: string): unknown[] {
  return columnRaw(path, 'SELECT label FROM entries ORDER BY id')
}

describe('a writer worker that dies', () => {
  it('rejects every write in flight with WRITER_WORKER_EXIT, keeps none of them, and serves queued and later writes on a restarted worker', async () => {
    const { db, path } = await openEntries('mid-transaction')

    const inFlight = Promise.allSettled([
      db.execute(INSERT_ENTRY, ['grouped first']),
      db.execute(INSERT_ENTRY, ['grouped second']),
      db.execute(CRASH),
      db.executeTransaction([
        { sql: INSERT_ENTRY, params: ['list first'] },
        { sql: INSERT_ENTRY, params: ['list second'] },
      ]),
    ])
    await nextTurn()
    const queued = db.execute(INSERT_ENTRY, ['queued behind the crash'])

    expect((await inFlight).map(codeOf)).toEqual(Array(4).fill('WRITER_WORKER_EXIT'))
    await expect(queued).resolves.toMatchObject({ changes: 1 })
    await expect(db.executeBatch(INSERT_ENTRY, [['batch'], ['batch']])).resolves.toHaveLength(2)
    expect(await db.transaction(tx => tx.query('PRAGMA synchronous'))).toEqual([
      { synchronous: SQLITE_SYNCHRONOUS_FULL },
    ])
    expect(integrityRaw(path)).toEqual(['ok'])
    expect(storedLabels(path)).toEqual(['before the crash', 'queued behind the crash', 'batch', 'batch'])
  }, 30_000)

  it('fails every later statement of a transaction callback that carries on after the worker dies inside it, commits none of its writes, and serves the next transaction on the restarted worker', async () => {
    const { db, path } = await openEntries('callback')
    const afterCrash: string[] = []

    const outcome = await Promise.allSettled([
      db.transaction(async tx => {
        await tx.execute(INSERT_ENTRY, ['callback before the crash'])
        await tx.execute(CRASH).catch(() => {})
        for (const attempt of [
          () => tx.execute(INSERT_ENTRY, ['callback after the crash']),
          () => tx.executeBatch(INSERT_ENTRY, [['batch after the crash']]),
          () => tx.query('SELECT label FROM entries'),
        ]) {
          afterCrash.push(codeOf((await Promise.allSettled([attempt()]))[0]))
        }
      }),
    ])

    expect(outcome.map(codeOf)).toEqual(['WRITER_WORKER_EXIT'])
    expect(afterCrash).toEqual(Array(3).fill('WRITER_WORKER_EXIT'))
    expect(storedLabels(path)).toEqual(['before the crash'])
    await db.transaction(tx => tx.execute(INSERT_ENTRY, ['next transaction']))
    expect(storedLabels(path)).toEqual(['before the crash', 'next transaction'])
    expect(integrityRaw(path)).toEqual(['ok'])
  }, 30_000)

  it('restarts after each crash, counts only crashes in a row, and fails every write with WRITER_WORKER_FATAL once they pass maxRestarts', async () => {
    for (const maxRestarts of [0, 2]) {
      const name = `budget-${maxRestarts}`
      const { sirannon, db, path } = await openEntries(name, maxRestarts)

      for (let crash = 0; crash < maxRestarts; crash++) {
        await expect(db.execute(CRASH), name).rejects.toMatchObject({ code: 'WRITER_WORKER_EXIT' })
      }
      await expect(db.execute(INSERT_ENTRY, ['after crashes up to the budget'])).resolves.toMatchObject({ changes: 1 })
      for (let crash = 0; crash <= maxRestarts; crash++) {
        await expect(db.execute(CRASH), name).rejects.toMatchObject({ code: 'WRITER_WORKER_EXIT' })
      }

      const refused = await Promise.allSettled([
        db.execute(INSERT_ENTRY, ['refused']),
        db.executeBatch(INSERT_ENTRY, [['refused']]),
        db.executeTransaction([{ sql: INSERT_ENTRY, params: ['refused'] }]),
        db.transaction(tx => tx.execute(INSERT_ENTRY, ['refused'])),
        db.bulkLoad(INSERT_ENTRY, [['refused']]),
      ])
      expect(refused.map(codeOf), name).toEqual(Array(5).fill('WRITER_WORKER_FATAL'))
      const expected = ['before the crash', 'after crashes up to the budget']
      expect(storedLabels(path), name).toEqual(expected)
      expect(
        (await db.query<{ label: string }>('SELECT label FROM entries ORDER BY id')).map(row => row.label),
      ).toEqual(expected)

      await db.close()
      const reopened = await sirannon.open(name, path, { writerWorker: { maxRestarts } })
      await expect(reopened.execute(INSERT_ENTRY, ['after reopening'])).resolves.toMatchObject({ changes: 1 })
      expect(storedLabels(path), name).toEqual([...expected, 'after reopening'])
    }
  }, 60_000)

  it('stamps every write that the worker commits with one node identity and a transaction id of its own, across a crash and a reopen', async () => {
    const { sirannon, db, path } = await openEntries('stamps')
    await db.watch('entries')
    const events: ChangeEvent[] = []
    db.on('entries').subscribe(event => events.push(event))

    await Promise.all(Array.from({ length: 6 }, (_, index) => db.execute(INSERT_ENTRY, [`burst ${index}`])))
    await expect(db.execute(CRASH)).rejects.toMatchObject({ code: 'WRITER_WORKER_EXIT' })
    await db.execute(INSERT_ENTRY, ['after the crash'])
    await eventually(() => events.length === 7, 'seven change events')
    await db.close()
    const reopened = await sirannon.open('stamps', path, { writerWorker: true })
    await reopened.watch('entries')
    await reopened.execute(INSERT_ENTRY, ['after reopening'])

    const stamps = readRaw<{ node_id: string; tx_id: string; hlc: string }>(
      path,
      "SELECT node_id, tx_id, hlc FROM _sirannon_changes WHERE table_name = 'entries' AND operation = 'INSERT' ORDER BY seq",
    )
    expect(stamps).toHaveLength(8)
    const identities = new Set(stamps.map(stamp => stamp.node_id))
    expect(identities.size).toBe(1)
    const [identity] = identities
    expect(identity).toMatch(HEX_IDENTITY)
    expect(new Set(stamps.map(stamp => stamp.tx_id)).size).toBe(8)
    for (const stamp of stamps) {
      expect(stamp.tx_id).toMatch(HEX_IDENTITY)
      expect(stamp.hlc).not.toBe('')
    }
    expect(events.map(event => event.origin)).toEqual(Array(7).fill(identity))
    expect(events.map(event => (event.row as { label: string }).label)).toEqual([
      ...Array.from({ length: 6 }, (_, index) => `burst ${index}`),
      'after the crash',
    ])
  }, 30_000)
})
