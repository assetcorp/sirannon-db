import { extname } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery } from '../../../core/live/types.js'
import { classBasedDriver } from '../../helpers/fault-drivers/extension-contract-drivers.js'
import {
  restartingWriterDriver,
  WRITER_THREAD_EXIT_TABLE,
} from '../../helpers/fault-drivers/restarting-writer-driver.js'
import {
  createWorkspace,
  DRIVER_PLACEMENTS,
  PROBE,
  PROBE_LIBRARY,
  PROBE_VALUE,
  probeAnswers,
  probeFailure,
  readRaw,
  type Workspace,
} from './harness.js'

const library = PROBE_LIBRARY ?? ''
const READERS = 3
const WRITES_DURING_LOAD = 300

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('loading')
})

afterEach(async () => {
  await workspace.dispose()
})

function rowsOf<T>(live: LiveQuery<T>): readonly T[] | string {
  const state = live.getState()
  return state.status === 'ready' ? state.rows : state.status
}

async function createNotes(db: Database, bodies: string[]): Promise<void> {
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
  for (const body of bodies) await db.execute('INSERT INTO notes (body) VALUES (?)', [body])
}

async function expectLiveQueriesAnswer(db: Database): Promise<void> {
  const earlier = await db.live<{ body: string }>('SELECT body FROM notes ORDER BY id')
  await db.loadExtension(library)
  const marked = await db.live<{ body: string; mark: string }>(
    `SELECT body, ${PROBE}() AS mark FROM notes ORDER BY id`,
    undefined,
    { maxTransactionChanges: 1 },
  )
  expect(rowsOf(marked)).toEqual([{ body: 'first', mark: PROBE_VALUE }])

  await db.executeTransaction(
    ['second', 'third', 'fourth'].map(body => ({ sql: 'INSERT INTO notes (body) VALUES (?)', params: [body] })),
  )

  const bodies = ['first', 'second', 'third', 'fourth']
  await vi.waitFor(() => expect(rowsOf(marked)).toEqual(bodies.map(body => ({ body, mark: PROBE_VALUE }))))
  await vi.waitFor(() => expect(rowsOf(earlier)).toEqual(bodies.map(body => ({ body }))))
  await marked.close()
  await earlier.close()
}

describe.skipIf(PROBE_LIBRARY === null).each(DRIVER_PLACEMENTS)(
  'loading a compiled extension with $label',
  ({ driver, writerWorker }) => {
    it('answers through the extension on the writer, on every reader, inside transactions, and in a column default', async () => {
      const path = workspace.path('shop.db')
      const db = await workspace.registry(driver()).open('shop', path, { writerWorker, readPoolSize: READERS })
      await createNotes(db, [])
      expect(await probeFailure(db)).toMatch(/no such function/)

      await db.loadExtension(library)

      for (let read = 0; read < READERS * 2; read++) expect(await probeAnswers(db)).toBe(PROBE_VALUE)
      await db.execute(`CREATE TABLE stamped (id INTEGER PRIMARY KEY, mark TEXT DEFAULT (${PROBE}()))`)
      await db.execute('INSERT INTO stamped (id) VALUES (1)')
      await db.execute(`INSERT INTO notes (body) VALUES (${PROBE}())`)
      await db.executeBatch(`INSERT INTO notes (body) VALUES (${PROBE}() || ?)`, [['-batch-a'], ['-batch-b']])
      await db.executeTransaction([
        { sql: `INSERT INTO notes (body) VALUES (${PROBE}() || '-list')` },
        { sql: 'INSERT INTO stamped (id) VALUES (2)' },
      ])
      const insideCallback = await db.transaction(async tx => {
        await tx.execute(`INSERT INTO notes (body) VALUES (${PROBE}() || '-callback')`)
        return tx.query<{ value: string }>(`SELECT ${PROBE}() AS value`)
      })

      expect(insideCallback).toEqual([{ value: PROBE_VALUE }])
      expect(readRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual(
        ['', '-batch-a', '-batch-b', '-list', '-callback'].map(suffix => ({ body: `${PROBE_VALUE}${suffix}` })),
      )
      expect(readRaw(path, 'SELECT id, mark FROM stamped ORDER BY id')).toEqual([
        { id: 1, mark: PROBE_VALUE },
        { id: 2, mark: PROBE_VALUE },
      ])
    })

    it('answers through the extension in a live query opened before the load and in each re-read after it', async () => {
      const db = await workspace.registry(driver()).open('live', workspace.path('live.db'), { writerWorker })
      await createNotes(db, ['first'])

      await expectLiveQueriesAnswer(db)
    })

    it('accepts the same extension again under another spelling and from many callers at once', async () => {
      const path = workspace.path('repeat.db')
      const db = await workspace.registry(driver()).open('repeat', path, { writerWorker, readPoolSize: READERS })
      await createNotes(db, ['first'])

      await db.loadExtension(library)
      await db.loadExtension(library.slice(0, -extname(library).length))
      const settled = await Promise.allSettled([
        ...Array.from({ length: 8 }, () => db.loadExtension(library)),
        ...Array.from({ length: 8 }, () => probeAnswers(db)),
        db.execute(`INSERT INTO notes (body) VALUES (${PROBE}())`),
      ])

      expect(settled.filter(outcome => outcome.status === 'rejected')).toEqual([])
      expect(settled.slice(8, 16).map(outcome => outcome.status === 'fulfilled' && outcome.value)).toEqual(
        Array.from({ length: 8 }, () => PROBE_VALUE),
      )
      expect(readRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual([{ body: 'first' }, { body: PROBE_VALUE }])
      const live = await db.live<{ mark: string }>(`SELECT ${PROBE}() AS mark FROM notes ORDER BY id`)
      expect(rowsOf(live)).toEqual([{ mark: PROBE_VALUE }, { mark: PROBE_VALUE }])
      await live.close()
    })

    it('commits every write that callers send while the extension loads', async () => {
      const path = workspace.path('busy.db')
      const db = await workspace.registry(driver()).open('busy', path, { writerWorker, readPoolSize: READERS })
      await createNotes(db, [])

      const writes: Promise<unknown>[] = []
      let loading: Promise<void> = Promise.resolve()
      for (let write = 0; write < WRITES_DURING_LOAD; write++) {
        if (write === WRITES_DURING_LOAD / 2) loading = db.loadExtension(library)
        writes.push(db.execute('INSERT INTO notes (body) VALUES (?)', [`plain-${write}`]))
      }
      await loading
      writes.push(db.execute(`INSERT INTO notes (body) VALUES (${PROBE}())`))
      await Promise.all(writes)

      const stored = readRaw<{ body: string }>(path, 'SELECT body FROM notes ORDER BY id').map(row => row.body)
      expect(stored).toEqual([
        ...Array.from({ length: WRITES_DURING_LOAD }, (_, write) => `plain-${write}`),
        PROBE_VALUE,
      ])
      for (let read = 0; read < READERS * 2; read++) expect(await probeAnswers(db)).toBe(PROBE_VALUE)
    })

    it('needs the extension loaded again after the database reopens, and reads what it wrote with it either way', async () => {
      const path = workspace.path('reopen.db')
      const sirannon = workspace.registry(driver())
      const first = await sirannon.open('reopen', path, { writerWorker })
      await createNotes(first, [])
      await first.loadExtension(library)
      await first.execute(`INSERT INTO notes (body) VALUES (${PROBE}())`)
      await sirannon.close('reopen')

      const reopened = await sirannon.open('reopen', path, { writerWorker })
      expect(await probeFailure(reopened)).toMatch(/no such function/)
      expect(await reopened.query('SELECT body FROM notes')).toEqual([{ body: PROBE_VALUE }])

      await reopened.loadExtension(library)
      await reopened.execute(`INSERT INTO notes (body) VALUES (${PROBE}() || '-again')`)
      expect(readRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual([
        { body: PROBE_VALUE },
        { body: `${PROBE_VALUE}-again` },
      ])
    })
  },
)

describe.skipIf(PROBE_LIBRARY === null)('loading a compiled extension on a writer worker that exits', () => {
  it('loads the extension onto each writer thread that replaces one that exits', async () => {
    const path = workspace.path('restart.db')
    const db = await workspace.registry(restartingWriterDriver()).open('restart', path, { writerWorker: true })
    await createNotes(db, [])
    await db.loadExtension(library)

    for (let restart = 1; restart <= 3; restart++) {
      await expect(db.execute(`CREATE TABLE ${WRITER_THREAD_EXIT_TABLE} (id INTEGER)`)).rejects.toMatchObject({
        code: 'WRITER_WORKER_EXIT',
      })
      await db.execute(`INSERT INTO notes (body) VALUES (${PROBE}() || ?)`, [`-${restart}`])
    }

    expect(readRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual(
      [1, 2, 3].map(restart => ({ body: `${PROBE_VALUE}-${restart}` })),
    )
    expect(readRaw(path, `SELECT name FROM sqlite_master WHERE name = '${WRITER_THREAD_EXIT_TABLE}'`)).toEqual([])
    expect(await probeAnswers(db)).toBe(PROBE_VALUE)
  })
})

describe.skipIf(PROBE_LIBRARY === null)('loading a compiled extension through a driver built from classes', () => {
  it('loads the extension and keeps reads, writes, and live queries working through class instances', async () => {
    const path = workspace.path('classes.db')
    const db = await workspace.registry(classBasedDriver()).open('classes', path)
    await createNotes(db, ['first'])
    expect(await db.query('SELECT body FROM notes')).toEqual([{ body: 'first' }])

    await expectLiveQueriesAnswer(db)
    await db.execute(`INSERT INTO notes (body) VALUES (${PROBE}())`)

    expect(await probeAnswers(db)).toBe(PROBE_VALUE)
    expect(readRaw(path, 'SELECT body FROM notes ORDER BY id').at(-1)).toEqual({ body: PROBE_VALUE })
  })
})
