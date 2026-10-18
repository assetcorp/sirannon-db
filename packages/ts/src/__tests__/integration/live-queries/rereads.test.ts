import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import type { LiveQuery, LiveQueryOptions } from '../../../core/live/types.js'
import type { ChangeEvent } from '../../../core/types.js'
import { MARKED_READ_COMMENT, markedReadDriver } from '../../helpers/fault-drivers/marked-read-driver.js'
import {
  canonicalRows,
  createWorkspace,
  logLiveStatements,
  pause,
  QUIET_PERIOD_MS,
  readyRows,
  replicate,
  settlesToQuery,
  type Workspace,
  waitFor,
} from './harness.js'

const ITEMS = 'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, bucket TEXT NOT NULL)'
const INSERT = 'INSERT INTO items (id, name, bucket) VALUES (?, ?, ?)'
const IN_BUCKET = 'SELECT id, name FROM items WHERE bucket = ? ORDER BY id'
const MARKED_IN_BUCKET = `SELECT id, name FROM items WHERE bucket = ? ORDER BY ${MARKED_READ_COMMENT} id`

let workspace: Workspace
const opened: LiveQuery<unknown>[] = []

beforeEach(() => {
  workspace = createWorkspace('rereads')
})

afterEach(async () => {
  for (const query of opened.splice(0)) await query.close().catch(() => {})
  await workspace.dispose()
})

function rows(count: number, firstId: number, bucket: string): unknown[][] {
  return Array.from({ length: count }, (_, index) => [firstId + index, `item-${firstId + index}`, bucket])
}

async function seed(db: Database, count: number, firstId = 1, bucket = 'a'): Promise<void> {
  await db.execute(ITEMS)
  if (count > 0) await db.executeBatch(INSERT, rows(count, firstId, bucket))
}

async function live<T = Record<string, unknown>>(
  db: Database,
  sql: string,
  params: unknown[],
  options?: LiveQueryOptions,
): Promise<LiveQuery<T>> {
  const query = await db.live<T>(sql, params, { rereadJitterMs: 0, ...options })
  opened.push(query as LiveQuery<unknown>)
  return query
}

describe('a live query', () => {
  it('reads once, then applies each later transaction to the rows it holds', async () => {
    const log = logLiveStatements()
    const db = await workspace.open('incremental', undefined, { metrics: log.metrics })
    await seed(db, 30)

    const query = await live(db, IN_BUCKET, ['a'])
    expect(log.returned()).toEqual([30])

    for (let index = 0; index < 10; index++) {
      await db.execute(INSERT, [100 + index, `late-${index}`, 'a'])
      await settlesToQuery(query, db, IN_BUCKET, ['a'])
    }
    for (let index = 0; index < 5; index++) await db.execute(INSERT, [200 + index, `other-${index}`, 'b'])
    await waitFor(() => log.returned().length === 16, 'a check of each transaction')
    await pause(QUIET_PERIOD_MS)

    expect(log.returned()).toEqual([30, ...Array(10).fill(1), ...Array(5).fill(0)])
    expect(readyRows(query)).toHaveLength(40)
  })

  it('reads again when a transaction changes more rows than it holds, and keeps the previous rows readable meanwhile', async () => {
    const log = logLiveStatements()
    const db = await workspace.open('outnumbered', undefined, { metrics: log.metrics })
    await seed(db, 2)
    const query = await live(db, IN_BUCKET, ['a'], { rereadJitterMs: 60 })
    const replica = replicate(query)
    const seenWhileReading: unknown[] = []
    query.subscribe(update => {
      const state = query.getState()
      if (update.kind === 'revalidating' && state.status === 'ready') seenWhileReading.push(...state.rows)
    })

    await db.executeBatch(INSERT, rows(8, 10, 'a'))
    await settlesToQuery(query, db, IN_BUCKET, ['a'])
    await db.execute(INSERT, [99, 'single', 'a'])
    await settlesToQuery(query, db, IN_BUCKET, ['a'])

    expect(log.returned().filter(count => count === 2 || count >= 10)).toEqual([2, 10])
    expect(replica.kinds).toEqual(['revalidating', 'rows', 'ops'])
    expect(canonicalRows(seenWhileReading)).toEqual(canonicalRows(await db.query(`${IN_BUCKET} LIMIT 2`, ['a'])))
    expect(canonicalRows(replica.rows)).toEqual(canonicalRows(readyRows(query)))
  })

  it('reads again in place of buffering a transaction larger than maxTransactionChanges', async () => {
    const log = logLiveStatements()
    const db = await workspace.open('oversized', undefined, { metrics: log.metrics })
    await seed(db, 40)
    const query = await live(db, IN_BUCKET, ['a'], { maxTransactionChanges: 3 })

    await db.executeBatch(INSERT, rows(10, 100, 'a'))
    await settlesToQuery(query, db, IN_BUCKET, ['a'])
    await db.executeBatch(INSERT, rows(3, 300, 'a'))
    await settlesToQuery(query, db, IN_BUCKET, ['a'])

    expect(log.returned()).toEqual([40, 50, 3])
  })

  it('leaves out of its first read a write that commits during that read, and applies that write once afterwards', async () => {
    const faults = markedReadDriver()
    const db = await workspace.open('mid-read', undefined, { driver: faults.driver })
    await seed(db, 1)
    faults.afterNextMarkedRead(async () => {
      await db.execute(INSERT, [2, 'during-read', 'a'])
    })

    const query = await live<{ id: number }>(db, MARKED_IN_BUCKET, ['a'])
    const replica = replicate(query)

    expect(readyRows(query).map(row => row.id)).toEqual([1])
    await settlesToQuery(query, db, IN_BUCKET, ['a'])
    expect(readyRows(query).map(row => row.id)).toEqual([1, 2])
    expect(replica.kinds).toEqual(['ops'])
  })

  it('reads again after its change feed overflows while it opens, and keeps applying changes afterwards', async () => {
    const faults = markedReadDriver()
    const db = await workspace.open('overflow', undefined, { driver: faults.driver })
    await seed(db, 5)
    await db.watch('items')
    const delivered: ChangeEvent[] = []
    db.on('items').subscribe(event => {
      delivered.push(event)
    })
    let releaseReplay = (): void => {}
    const replayHeld = new Promise<void>(resolve => {
      releaseReplay = resolve
    })
    faults.afterNextMarkedRead(async () => {
      await db.execute(INSERT, [6, 'after-the-read', 'a'])
      await waitFor(() => delivered.length === 1, 'the poller to pass the write')
      faults.holdChangeLogRangeReads(replayHeld)
    })

    const opening = live(db, MARKED_IN_BUCKET, ['a'])
    await waitFor(() => faults.heldChangeLogRangeReads() > 0, 'the query to replay the change log')
    await db.executeBatch(INSERT, rows(10_001, 1_000, 'a'))
    await waitFor(() => delivered.length === 10_002, 'the burst to reach subscribers')
    releaseReplay()
    const query = await opening

    await settlesToQuery(query, db, IN_BUCKET, ['a'])
    await db.execute(INSERT, [50_000, 'after-the-overflow', 'a'])
    await db.execute('DELETE FROM items WHERE id = 1')
    await settlesToQuery(query, db, IN_BUCKET, ['a'])
  })

  it('reports a failed second read as an error, and reads again on the next transaction', async () => {
    const faults = markedReadDriver()
    const db = await workspace.open('failed-read', undefined, { driver: faults.driver })
    await seed(db, 3)
    const query = await live(db, MARKED_IN_BUCKET, ['a'], { maxTransactionChanges: 1 })
    const replica = replicate(query)

    faults.failNextMarkedRead('disk I/O error')
    await db.executeBatch(INSERT, rows(2, 10, 'a'))
    await waitFor(() => query.getState().status === 'error', 'the failed read to reach the query')
    const failed = query.getState()
    expect(failed.status === 'error' && failed.error.message).toBe('disk I/O error')
    await pause(QUIET_PERIOD_MS)
    expect(query.getState().status).toBe('error')

    await db.execute(INSERT, [20, 'after-the-failure', 'a'])
    await settlesToQuery(query, db, IN_BUCKET, ['a'])
    expect(replica.kinds).toEqual(['revalidating', 'error', 'rows'])
  })

  it('reports an error that SQLite raises for a changed row, and recovers once the row is repaired', async () => {
    const db = await workspace.open('malformed')
    await db.execute('CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT NOT NULL)')
    await db.execute(`INSERT INTO docs VALUES (1, '{"ok":1}'), (2, '{"ok":1}')`)
    const sql = "SELECT id FROM docs WHERE json_extract(body, '$.ok') = 1 ORDER BY id"
    const query = await live(db, sql, [])

    await db.executeTransaction([
      { sql: 'INSERT INTO docs VALUES (3, ?)', params: ['{"ok":1}'] },
      { sql: 'INSERT INTO docs VALUES (4, ?)', params: ['{not json'] },
    ])
    const oracleFailure = await db.query(sql).then(
      () => 'no error',
      (error: Error) => error.message,
    )
    await waitFor(() => query.getState().status === 'error', 'the malformed row to fail the query')
    const failed = query.getState()
    expect(failed.status === 'error' && failed.error.message).toBe(oracleFailure)

    await db.execute('INSERT INTO docs VALUES (5, ?)', ['{"ok":1}'])
    await pause(QUIET_PERIOD_MS)
    expect(query.getState().status).toBe('error')

    await db.execute(`UPDATE docs SET body = '{"ok":1}' WHERE id = 4`)
    await settlesToQuery(query, db, sql)
    expect(readyRows(query)).toHaveLength(5)
  })

  it('closes every connection it opens, whether its first read succeeds or fails', async () => {
    const faults = markedReadDriver()
    const sirannon = workspace.registry({ driver: faults.driver })
    const db = await sirannon.open('connections', workspace.path('connections'), { cdcPollInterval: 5 })
    await seed(db, 3)
    const first = await live(db, MARKED_IN_BUCKET, ['a'])
    await first.close()
    const settled = faults.openConnections()

    faults.failNextMarkedRead('disk I/O error')
    await expect(db.live(MARKED_IN_BUCKET, ['a'])).rejects.toThrow('disk I/O error')
    await expect(db.live('SELECT nope FROM items ORDER BY id')).rejects.toThrow('no such column: nope')
    const kept = await live(db, MARKED_IN_BUCKET, ['a'], { maxTransactionChanges: 1 })
    for (let round = 0; round < 3; round++) {
      await db.executeBatch(INSERT, rows(2, 100 + round * 10, 'a'))
      await settlesToQuery(kept, db, IN_BUCKET, ['a'])
    }
    await kept.close()

    expect(faults.openConnections()).toBe(settled)
    await sirannon.shutdown()
    expect(faults.openConnections()).toBe(0)
  })
})
