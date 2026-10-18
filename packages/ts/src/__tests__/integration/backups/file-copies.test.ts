import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import BetterSqlite3 from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  COPY_FAILURE_MESSAGE,
  failingCopyDriver,
  PAGE_SIZE_FAILURE_MESSAGE,
} from '../../helpers/fault-drivers/failing-copy-driver.js'
import {
  CREATE_ORDERS,
  codeOf,
  createWorkspace,
  failureOf,
  filesIn,
  INSERT_ORDER,
  integrityOf,
  ORDER_PADDING,
  orderLabels,
  PLACEMENTS,
  readRaw,
  seedOrders,
  type Workspace,
  waitFor,
  watchUnhandledRejections,
} from './harness.js'

const LARGE_ORDER_COUNT = 20_000
const STALL_PAST_THE_DEADLINE_MS = 31_500
const HEX_RUN_ID = /^[0-9a-f]{16}$/

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('file-copies')
})

afterEach(async () => {
  await workspace.dispose()
})

function everyRow(path: string): Record<string, unknown[]> {
  return {
    orders: readRaw(path, 'SELECT * FROM orders ORDER BY id'),
    parcels: readRaw(path, 'SELECT * FROM parcels ORDER BY id'),
  }
}

describe.each(PLACEMENTS)('copying a database to a file with $label', ({ driver, writerWorker }) => {
  it('writes a file that opens as the same database and reports the file, the pages, and the time it took', async () => {
    const sourcePath = workspace.path('catalogue.db')
    const db = await workspace.registry(driver()).open('catalogue', sourcePath, { writerWorker })
    await seedOrders(db, 'order', 3_000, ORDER_PADDING)
    await db.execute('CREATE TABLE parcels (id INTEGER PRIMARY KEY, weight REAL, label BLOB, big INTEGER)')
    await db.execute('INSERT INTO parcels (weight, label, big) VALUES (?, ?, ?)', [
      2.75,
      new Uint8Array([0, 255, 7]),
      9_007_199_254_740_993n,
    ])
    await db.execute('INSERT INTO parcels (weight, label, big) VALUES (NULL, NULL, -1)')
    const requested = join(workspace.dir, 'copies', '.', 'catalogue-copy.db')

    const first = await db.backup(requested)
    const second = await db.backup(workspace.path('second-copy.db'))

    const copy = resolve(requested)
    expect(first.destPath).toBe(copy)
    expect(first).toMatchObject({ databaseId: 'catalogue', sourcePath, restarts: 0 })
    expect(first.runId).toMatch(HEX_RUN_ID)
    expect(second.runId).not.toBe(first.runId)
    expect(first.byteLength).toBe(statSync(copy).size)
    expect(first.pageSize).toBe(readRaw<{ page_size: number }>(copy, 'PRAGMA page_size')[0]?.page_size)
    expect(first.pageCount).toBe(readRaw<{ page_count: number }>(copy, 'PRAGMA page_count')[0]?.page_count)
    expect(first.pageCount * first.pageSize).toBe(first.byteLength)
    expect(first.durationMs).toBe(first.finishedAt - first.startedAt)
    expect(first.startedAt).toBeLessThanOrEqual(first.finishedAt)
    expect(integrityOf(copy)).toBe('ok')
    expect(everyRow(copy)).toEqual(everyRow(sourcePath))
    expect(readRaw(copy, 'SELECT typeof(big) AS kind, CAST(big AS TEXT) AS big FROM parcels WHERE id = 1')).toEqual([
      { kind: 'integer', big: '9007199254740993' },
    ])
  })

  it('keeps taking writes for the whole copy with zero restarts, and copies one consistent moment', async () => {
    const db = await workspace.registry(driver()).open('busy', workspace.path('busy.db'), { writerWorker })
    await seedOrders(db, 'seeded', LARGE_ORDER_COUNT, ORDER_PADDING)
    let copying = true
    let writesDuringCopy = 0
    const writer = (async () => {
      while (copying) {
        await new Promise(settle => setImmediate(settle))
        if (!copying) break
        await db.execute(INSERT_ORDER, [`during-${writesDuringCopy}`, ''])
        writesDuringCopy++
      }
    })()

    const report = await db.backup(workspace.path('busy-copy.db'))
    copying = false
    await writer

    const copied = orderLabels(report.destPath)
    expect(report.restarts).toBe(0)
    expect(writesDuringCopy).toBeGreaterThan(1)
    expect(integrityOf(report.destPath)).toBe('ok')
    expect(copied.slice(0, LARGE_ORDER_COUNT)).toEqual(
      Array.from({ length: LARGE_ORDER_COUNT }, (_, at) => `seeded-${at}`),
    )
    expect(copied.slice(LARGE_ORDER_COUNT)).toEqual(
      Array.from({ length: copied.length - LARGE_ORDER_COUNT }, (_, at) => `during-${at}`),
    )
    expect(await db.queryOne('SELECT count(*) AS n FROM orders')).toEqual({ n: LARGE_ORDER_COUNT + writesDuringCopy })
  })

  it('refuses a path that exists, the source file, a traversal segment, and control characters, and changes no file', async () => {
    const sourcePath = workspace.path('guarded.db')
    const db = await workspace.registry(driver()).open('guarded', sourcePath, { writerWorker })
    await seedOrders(db, 'kept', 10)
    const occupied = workspace.path('occupied.db')
    writeFileSync(occupied, 'an earlier copy that an operator still needs')
    const sourceBefore = orderLabels(sourcePath)

    const refused: [string, string][] = [
      ['an existing file', occupied],
      ['the source file', sourcePath],
      ['a traversal segment', `${workspace.dir}/nested/../escaped.db`],
      ['a backslash traversal segment', `${workspace.dir}\\nested\\..\\escaped.db`],
      ['a line feed', `${workspace.path('line')}\nfeed.db`],
      ['a null byte', `${workspace.path('null')}\u0000byte.db`],
      ['an escape character', `${workspace.path('escape')}\u001bcode.db`],
      ['a directory under a regular file', join(occupied, 'nested', 'copy.db')],
    ]
    for (const [label, path] of refused) {
      const error = await failureOf(db.backup(path))
      expect(codeOf(error), label).toBe('BACKUP_ERROR')
    }

    expect(readFileSync(occupied, 'utf8')).toBe('an earlier copy that an operator still needs')
    expect(existsSync(workspace.path('escaped.db'))).toBe(false)
    expect(orderLabels(sourcePath)).toEqual(sourceBefore)
    expect(integrityOf(sourcePath)).toBe('ok')
  })

  it('creates every missing parent directory and copies to paths with spaces, quotes, and other scripts', async () => {
    const sourcePath = workspace.path('named.db')
    const db = await workspace.registry(driver()).open('named', sourcePath, { writerWorker })
    const labels = await seedOrders(db, 'named', 25)
    const paths = [
      join(workspace.dir, 'a', 'b', 'c', 'deep.db'),
      join(workspace.dir, 'dir with spaces', 'copy file.db'),
      join(workspace.dir, "it's quoted", 'copy "double".db'),
      join(workspace.dir, 'commande-été', '注文.db'),
    ]

    for (const path of paths) {
      const report = await db.backup(path)
      expect(report.destPath, path).toBe(path)
      expect(integrityOf(path), path).toBe('ok')
      expect(orderLabels(path), path).toEqual(labels)
    }
  })
})

describe('copying a database to a file when the copy cannot finish', () => {
  it('fails with BACKUP_RESTARTED once another connection keeps writing to the file, and removes the partial copy', async () => {
    const sourcePath = workspace.path('contended.db')
    const db = await workspace.open('contended', { writerWorker: false })
    await seedOrders(db, 'seeded', LARGE_ORDER_COUNT, ORDER_PADDING)
    const intruder = new BetterSqlite3(sourcePath)
    intruder.pragma('busy_timeout = 5000')
    const insert = intruder.prepare(INSERT_ORDER)
    let copying = true
    let intrusions = 0
    const intruding = (async () => {
      while (copying) {
        await new Promise(settle => setImmediate(settle))
        if (!copying) break
        insert.run(`intruder-${intrusions}`, '')
        intrusions++
      }
    })()

    const copyPath = workspace.path('contended-copy.db')
    const error = await failureOf(db.backup(copyPath))
    copying = false
    await intruding
    intruder.close()

    expect(codeOf(error)).toBe('BACKUP_RESTARTED')
    expect(error.message).toContain('Close any other connection that writes to this file')
    expect(intrusions).toBeGreaterThan(3)
    expect(existsSync(copyPath)).toBe(false)
    expect(await db.queryOne('SELECT count(*) AS n FROM orders')).toEqual({ n: LARGE_ORDER_COUNT + intrusions })
  })

  it.each([
    false,
    true,
  ])('removes the partial copy when SQLite fails part-way, and keeps serving writes (writer worker: %s)', async writerWorker => {
    const watch = watchUnhandledRejections()
    try {
      const sourcePath = workspace.path('failing.db')
      const db = await workspace
        .registry(failingCopyDriver({ failAtStep: 2 }))
        .open('failing', sourcePath, { writerWorker })
      await seedOrders(db, 'seeded', LARGE_ORDER_COUNT, ORDER_PADDING)
      const copyPath = workspace.path('failing-copy.db')

      const error = await failureOf(db.backup(copyPath))
      await db.execute(INSERT_ORDER, ['after-the-failure', ''])

      expect(codeOf(error)).toBe('BACKUP_ERROR')
      expect(error.message).toContain(COPY_FAILURE_MESSAGE)
      expect(existsSync(copyPath)).toBe(false)
      expect(filesIn(workspace.dir).filter(name => name.startsWith('failing-copy'))).toEqual([])
      expect(orderLabels(sourcePath).at(-1)).toBe('after-the-failure')
      expect(watch.reasons).toEqual([])
    } finally {
      watch.stop()
    }
  })

  it('fails before it copies anything when it cannot read the page size, and leaves no file', async () => {
    const db = await workspace
      .registry(failingCopyDriver({ refusePageSize: true }))
      .open('pageless', workspace.path('pageless.db'))
    await seedOrders(db, 'seeded', 10)
    const copyPath = workspace.path('pageless-copy.db')

    const error = await failureOf(db.backup(copyPath))

    expect(codeOf(error)).toBe('BACKUP_ERROR')
    expect(error.message).toContain(PAGE_SIZE_FAILURE_MESSAGE)
    expect(existsSync(copyPath)).toBe(false)
  })

  it(
    'reports a copy that stops moving as BACKUP_STALLED at once, and removes its partial file only once the copy stops',
    async () => {
      const watch = watchUnhandledRejections()
      try {
        const db = await workspace
          .registry(failingCopyDriver({ stallFirstStepMs: STALL_PAST_THE_DEADLINE_MS }))
          .open('stalled', workspace.path('stalled.db'), { writerWorker: true })
        await seedOrders(db, 'seeded', LARGE_ORDER_COUNT, ORDER_PADDING)
        const copyPath = workspace.path('stalled-copy.db')

        const startedAt = Date.now()
        const error = await failureOf(db.backup(copyPath))
        const waited = Date.now() - startedAt

        expect(codeOf(error)).toBe('BACKUP_STALLED')
        expect(error.message).toContain('moved no pages for 30000ms')
        expect(waited).toBeLessThan(STALL_PAST_THE_DEADLINE_MS)
        expect(existsSync(copyPath)).toBe(true)
        await waitFor(() => !existsSync(copyPath), 'the stalled copy to stop', STALL_PAST_THE_DEADLINE_MS)
        await db.execute(INSERT_ORDER, ['after-the-stall', ''])
        expect(watch.reasons).toEqual([])
      } finally {
        watch.stop()
      }
    },
    STALL_PAST_THE_DEADLINE_MS * 2,
  )

  it('refuses to copy through a read-only database or a closed one, and writes no file', async () => {
    const path = workspace.path('locked.db')
    const raw = new BetterSqlite3(path)
    raw.exec(CREATE_ORDERS)
    raw.close()
    const sirannon = workspace.registry()
    const readOnly = await sirannon.open('locked', path, { readOnly: true })
    const closing = await sirannon.open('closing', workspace.path('closing.db'))
    await closing.close()
    mkdirSync(workspace.path('copies'))

    const readOnlyError = await failureOf(readOnly.backup(workspace.path('copies/read-only.db')))
    const closedError = await failureOf(closing.backup(workspace.path('copies/closed.db')))

    expect(codeOf(readOnlyError)).toBe('CONNECTION_POOL_ERROR')
    expect(readOnlyError.message).toContain('read-only')
    expect(codeOf(closedError)).toBe('DATABASE_CLOSED')
    expect(filesIn(workspace.path('copies'))).toEqual([])
  })
})
