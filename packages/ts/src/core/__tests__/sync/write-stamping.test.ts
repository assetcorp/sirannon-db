import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import type { SQLiteConnection } from '../../driver/types.js'
import { CHANGES_TABLE, META_TABLE } from '../../internal-tables.js'
import { isWellFormedHlc } from '../../sync/hlc-store.js'
import { NODE_ID_META_KEY } from '../../sync/stamper.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string
let db: Database | undefined
let inspect: SQLiteConnection | undefined

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-stamping-'))
})

afterEach(async () => {
  await inspect?.close()
  inspect = undefined
  await db?.close()
  db = undefined
  rmSync(tempDir, { recursive: true, force: true })
})

interface StampedRow {
  seq: number | bigint
  node_id: string
  tx_id: string
  hlc: string
}

async function openWatched(name: string): Promise<Database> {
  const database = await Database.create('stamping', join(tempDir, name), testDriver)
  await database.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
  await database.watch('notes')
  return database
}

async function readChangeRows(path: string): Promise<StampedRow[]> {
  inspect = await testDriver.open(path)
  const stmt = await inspect.prepare(`SELECT seq, node_id, tx_id, hlc FROM ${CHANGES_TABLE} ORDER BY seq`)
  const rows = (await stmt.all()) as unknown as StampedRow[]
  await inspect.close()
  inspect = undefined
  return rows
}

async function readMeta(path: string, key: string): Promise<string | null> {
  inspect = await testDriver.open(path)
  const stmt = await inspect.prepare(`SELECT value FROM ${META_TABLE} WHERE key = ?`)
  const row = (await stmt.get(key)) as { value?: unknown } | undefined
  await inspect.close()
  inspect = undefined
  return typeof row?.value === 'string' ? row.value : null
}

function expectStamped(rows: StampedRow[], nodeId: string): void {
  expect(rows.length).toBeGreaterThan(0)
  for (const row of rows) {
    expect(row.node_id).toBe(nodeId)
    expect(row.tx_id).toMatch(/^[0-9a-f]{32}$/)
    expect(isWellFormedHlc(row.hlc)).toBe(true)
  }
}

describe('write-time stamping', () => {
  it('stamps single execute writes inside the write transaction', async () => {
    const path = join(tempDir, 'single.db')
    db = await openWatched('single.db')
    await db.execute("INSERT INTO notes (body) VALUES ('one')")
    await db.close()
    db = undefined

    const nodeId = await readMeta(path, NODE_ID_META_KEY)
    expect(nodeId).toMatch(/^[0-9a-f]{32}$/)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(1)
    expectStamped(rows, nodeId as string)
    expect(await readMeta(path, 'hlc_clock')).toBe(rows[0].hlc)
  })

  it('stamps concurrent group-committed writes with one tx_id per write', async () => {
    const path = join(tempDir, 'group.db')
    db = await openWatched('group.db')
    await Promise.all(
      Array.from({ length: 8 }, (_, i) => db?.execute('INSERT INTO notes (body) VALUES (?)', [`n${i}`])),
    )
    await db.close()
    db = undefined

    const nodeId = await readMeta(path, NODE_ID_META_KEY)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(8)
    expectStamped(rows, nodeId as string)
    expect(new Set(rows.map(r => r.tx_id)).size).toBe(8)
    const hlcs = rows.map(r => r.hlc)
    expect([...hlcs].sort()).toEqual(hlcs)
  })

  it('stamps executeTransaction writes with a shared tx_id per transaction', async () => {
    const path = join(tempDir, 'etx.db')
    db = await openWatched('etx.db')
    await db.executeTransaction([
      { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['a'] },
      { sql: 'INSERT INTO notes (body) VALUES (?)', params: ['b'] },
    ])
    await db.close()
    db = undefined

    const nodeId = await readMeta(path, NODE_ID_META_KEY)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(2)
    expectStamped(rows, nodeId as string)
    expect(rows[0].tx_id).toBe(rows[1].tx_id)
    expect(rows[0].hlc).toBe(rows[1].hlc)
  })

  it('stamps executeBatch writes', async () => {
    const path = join(tempDir, 'batch.db')
    db = await openWatched('batch.db')
    await db.executeBatch('INSERT INTO notes (body) VALUES (?)', [['a'], ['b'], ['c']])
    await db.close()
    db = undefined

    const nodeId = await readMeta(path, NODE_ID_META_KEY)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(3)
    expectStamped(rows, nodeId as string)
    expect(new Set(rows.map(r => r.tx_id)).size).toBe(1)
  })

  it('stamps callback transaction writes', async () => {
    const path = join(tempDir, 'tx.db')
    db = await openWatched('tx.db')
    await db.transaction(async tx => {
      await tx.execute('INSERT INTO notes (body) VALUES (?)', ['a'])
      await tx.execute('INSERT INTO notes (body) VALUES (?)', ['b'])
    })
    await db.close()
    db = undefined

    const nodeId = await readMeta(path, NODE_ID_META_KEY)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(2)
    expectStamped(rows, nodeId as string)
    expect(rows[0].tx_id).toBe(rows[1].tx_id)
  })

  it('stamps bulkLoad rows', async () => {
    const path = join(tempDir, 'load.db')
    db = await openWatched('load.db')
    await db.bulkLoad(
      'INSERT INTO notes (body) VALUES (?)',
      Array.from({ length: 5 }, (_, i) => [`n${i}`]),
    )
    await db.close()
    db = undefined

    const nodeId = await readMeta(path, NODE_ID_META_KEY)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(5)
    expectStamped(rows, nodeId as string)
  })

  it('keeps the node identity and advances the clock across reopen', async () => {
    const path = join(tempDir, 'reopen.db')
    db = await openWatched('reopen.db')
    await db.execute("INSERT INTO notes (body) VALUES ('first')")
    await db.close()
    db = undefined
    const firstNode = await readMeta(path, NODE_ID_META_KEY)
    const firstClock = await readMeta(path, 'hlc_clock')

    db = await Database.create('stamping', path, testDriver)
    await db.watch('notes')
    await db.execute("INSERT INTO notes (body) VALUES ('second')")
    await db.close()
    db = undefined

    expect(await readMeta(path, NODE_ID_META_KEY)).toBe(firstNode)
    const secondClock = await readMeta(path, 'hlc_clock')
    expect(secondClock).not.toBeNull()
    expect((secondClock as string) > (firstClock as string)).toBe(true)
    const rows = await readChangeRows(path)
    expect(rows).toHaveLength(2)
    expectStamped(rows, firstNode as string)
  })

  it('leaves unwatched databases without stamping state', async () => {
    const path = join(tempDir, 'plain.db')
    db = await Database.create('stamping', path, testDriver)
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.execute("INSERT INTO notes (body) VALUES ('one')")
    await db.close()
    db = undefined

    inspect = await testDriver.open(path)
    const stmt = await inspect.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
    expect(await stmt.get(CHANGES_TABLE)).toBeUndefined()
    await inspect.close()
    inspect = undefined
  })

  it('delivers hlc and origin on subscription events', async () => {
    db = await openWatched('events.db')
    const events: { hlc?: string; origin?: string }[] = []
    db.on('notes').subscribe(event => events.push({ hlc: event.hlc, origin: event.origin }))

    await db.execute("INSERT INTO notes (body) VALUES ('one')")
    await new Promise(resolve => setTimeout(resolve, 150))

    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events[0].origin).toMatch(/^[0-9a-f]{32}$/)
    expect(isWellFormedHlc(events[0].hlc ?? '')).toBe(true)
  })
})
