import { randomBytes } from 'node:crypto'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Database } from '../../../core/database.js'
import { columnRaw, countRaw, createWorkspace, DRIVER_PLACEMENTS, readRaw, type Workspace } from './harness.js'

const LARGEST_SAFE = 2n ** 53n - 1n
const LARGEST_INT64 = 2n ** 63n - 1n
const SMALLEST_INT64 = -(2n ** 63n)
const INTEGERS = [
  0n,
  1n,
  -1n,
  LARGEST_SAFE - 1n,
  LARGEST_SAFE,
  -LARGEST_SAFE,
  LARGEST_SAFE + 1n,
  -LARGEST_SAFE - 1n,
  LARGEST_SAFE + 2n,
  LARGEST_INT64 - 1n,
  LARGEST_INT64,
  SMALLEST_INT64,
]
const BLOB_SIZES = [0, 1, 256, 1 << 20]

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('values')
})

afterEach(async () => {
  await workspace.dispose()
})

function expectedJavaScriptValue(stored: bigint): number | bigint {
  return stored >= -LARGEST_SAFE && stored <= LARGEST_SAFE ? Number(stored) : stored
}

function everyByteValue(size: number): Buffer {
  if (size <= 256) return Buffer.from(Array.from({ length: size }, (_, index) => 255 - index))
  return randomBytes(size)
}

describe.each(DRIVER_PLACEMENTS)('values with $label', ({ driver, writerWorker }) => {
  async function openValues(): Promise<{ db: Database; path: string }> {
    const path = workspace.path('values.db')
    const db = await workspace.registry({ driver: driver() }).open('values', path, { writerWorker })
    await db.execute('CREATE TABLE amounts (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)')
    await db.execute('CREATE TABLE blobs (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)')
    await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
    await db.execute('CREATE TABLE counters (id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT NOT NULL)')
    return { db, path }
  }

  it('returns each integer as a number inside the safe range and as an exact bigint beyond it, from every read path', async () => {
    const { db, path } = await openValues()
    await db.executeBatch(
      'INSERT INTO amounts (amount) VALUES (?)',
      INTEGERS.map(value => [value]),
    )
    await db.execute('INSERT INTO amounts (amount) VALUES (?)', [Number(LARGEST_SAFE)])

    const stored = columnRaw(path, 'SELECT CAST(amount AS TEXT) FROM amounts ORDER BY id').map(text =>
      BigInt(String(text)),
    )
    expect(stored).toEqual([...INTEGERS, LARGEST_SAFE])
    const expected = stored.map(expectedJavaScriptValue)
    const select = 'SELECT amount FROM amounts ORDER BY id'

    const fromReaders = (await db.query<{ amount: unknown }>(select)).map(row => row.amount)
    const fromWriter = await db.transaction(async tx =>
      (await tx.query<{ amount: unknown }>(select)).map(row => row.amount),
    )
    const one = await db.queryOne<{ amount: unknown }>('SELECT amount FROM amounts WHERE amount = ?', [LARGEST_INT64])

    expect(fromReaders).toEqual(expected)
    expect(fromWriter).toEqual(expected)
    expect(fromReaders.map(value => typeof value)).toEqual(expected.map(value => typeof value))
    expect(one).toEqual({ amount: LARGEST_INT64 })
  })

  it('reports lastInsertRowId as a number inside the safe range and as an exact bigint beyond it, from every write method', async () => {
    const { db, path } = await openValues()
    const insertNote = 'INSERT INTO notes (id, body) VALUES (?, ?)'

    const single = await db.execute(insertNote, [LARGEST_SAFE, 'largest safe'])
    const batch = await db.executeBatch(insertNote, [
      [LARGEST_INT64 - 1n, 'batch big'],
      [7, 'batch small'],
    ])
    const list = await db.executeTransaction([
      { sql: insertNote, params: [LARGEST_SAFE + 1n, 'list big'] },
      { sql: insertNote, params: [8, 'list small'] },
    ])
    const inside = await db.transaction(async tx => {
      const beforeAny = tx.lastInsertRowId
      const big = await tx.execute(insertNote, [LARGEST_INT64, 'callback big'])
      await tx.executeBatch(insertNote, [])
      const afterEmptyBatch = tx.lastInsertRowId
      await tx.executeBatch(insertNote, [[9, 'callback small']])
      return { beforeAny, big: big.lastInsertRowId, afterEmptyBatch, afterSmall: tx.lastInsertRowId }
    })
    const chosenBySqlite = await db.execute('INSERT INTO notes (body) VALUES (?)', ['rowid chosen by SQLite'])

    expect(single.lastInsertRowId).toBe(Number(LARGEST_SAFE))
    expect(batch.map(result => result.lastInsertRowId)).toEqual([LARGEST_INT64 - 1n, 7])
    expect(list.map(result => result.lastInsertRowId)).toEqual([LARGEST_SAFE + 1n, 8])
    expect(inside).toEqual({ beforeAny: 0, big: LARGEST_INT64, afterEmptyBatch: LARGEST_INT64, afterSmall: 9 })
    const [chosen] = columnRaw(path, 'SELECT id FROM notes WHERE body = ?', ['rowid chosen by SQLite'])
    expect(chosenBySqlite.lastInsertRowId).toBe(expectedJavaScriptValue(BigInt(String(chosen))))
  })

  it('refuses a new row once an AUTOINCREMENT table has used the largest rowid, writing nothing', async () => {
    const { db, path } = await openValues()
    await db.execute('INSERT INTO counters (id, label) VALUES (?, ?)', [LARGEST_INT64, 'last possible'])

    await expect(db.execute('INSERT INTO counters (label) VALUES (?)', ['one too many'])).rejects.toMatchObject({
      code: 'QUERY_ERROR',
      sql: 'INSERT INTO counters (label) VALUES (?)',
    })
    await expect(
      db.executeBatch('INSERT INTO counters (label) VALUES (?)', [['batch overflow']]),
    ).rejects.toMatchObject({ code: 'QUERY_ERROR' })

    expect(readRaw(path, 'SELECT id, label FROM counters')).toEqual([{ id: LARGEST_INT64, label: 'last possible' }])
  })

  it('returns every BLOB byte for byte, from an empty one to a mebibyte', async () => {
    const { db, path } = await openValues()
    const payloads = BLOB_SIZES.map(everyByteValue)
    await db.executeBatch(
      'INSERT INTO blobs (payload) VALUES (?)',
      payloads.map(payload => [payload]),
    )
    await db.execute('INSERT INTO blobs (payload) VALUES (?)', [new Uint8Array([0, 1, 2, 0])])

    const expectedHex = [...payloads.map(payload => payload.toString('hex').toUpperCase()), '00010200']
    expect(columnRaw(path, 'SELECT hex(payload) FROM blobs ORDER BY id')).toEqual(expectedHex)
    expect(columnRaw(path, 'SELECT DISTINCT typeof(payload) FROM blobs')).toEqual(['blob'])
    const read = await db.query<{ payload: Uint8Array }>('SELECT payload FROM blobs ORDER BY id')
    expect(read.map(row => Buffer.from(row.payload).toString('hex').toUpperCase())).toEqual(expectedHex)
    for (const row of read) expect(row.payload).toBeInstanceOf(Uint8Array)
  })

  it('stores reals, text, and null exactly as given', async () => {
    const { db, path } = await openValues()
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, reading REAL, label TEXT)')
    const rows: [number | null, string | null][] = [
      [0.1 + 0.2, 'sum that is not three tenths'],
      [Number.MAX_VALUE, 'largest double'],
      [Number.MIN_VALUE, 'smallest subnormal'],
      [-1.5, 'emoji 🧪 and accents éàü and CJK 漢字'],
      [null, null],
      [Number.POSITIVE_INFINITY, ''],
    ]
    await db.executeBatch('INSERT INTO readings (reading, label) VALUES (?, ?)', rows)

    const raw = readRaw<{ reading: number | null; label: string | null; kind: string }>(
      path,
      'SELECT reading, label, typeof(reading) AS kind FROM readings ORDER BY id',
    )
    expect(raw.map(row => [row.reading, row.label])).toEqual(rows)
    expect(raw.map(row => row.kind)).toEqual(['real', 'real', 'real', 'real', 'null', 'real'])
    const read = await db.query<{ reading: number | null; label: string | null }>(
      'SELECT reading, label FROM readings ORDER BY id',
    )
    expect(read.map(row => [row.reading, row.label])).toEqual(rows)
  })

  it('refuses a value that SQLite cannot bind, writing nothing', async () => {
    const { db, path } = await openValues()
    const insertAmount = 'INSERT INTO amounts (amount) VALUES (?)'
    const unbindable: unknown[] = [LARGEST_INT64 + 1n, SMALLEST_INT64 - 1n, true, { amount: 1 }]

    for (const value of unbindable) {
      await expect(db.execute(insertAmount, [value]), String(value)).rejects.toMatchObject({
        code: 'QUERY_ERROR',
        sql: insertAmount,
      })
      await expect(db.executeBatch(insertAmount, [[1], [value]]), String(value)).rejects.toMatchObject({
        code: 'QUERY_ERROR',
      })
      await expect(
        db.executeTransaction([
          { sql: insertAmount, params: [2] },
          { sql: insertAmount, params: [value] },
        ]),
        String(value),
      ).rejects.toMatchObject({ code: 'QUERY_ERROR', sql: insertAmount })
    }

    expect(countRaw(path, 'amounts')).toBe(0)
  })

  it('binds positional and named parameters in every prefix form', async () => {
    const { db, path } = await openValues()

    await db.execute('INSERT INTO notes (id, body) VALUES (:id, :body)', { id: 1, body: 'colon' })
    await db.execute('INSERT INTO notes (id, body) VALUES (@id, @body)', { id: 2, body: 'at sign' })
    await db.execute('INSERT INTO notes (id, body) VALUES ($id, $body)', { id: 3, body: 'dollar' })
    await db.executeBatch('INSERT INTO notes (id, body) VALUES (:id, :body)', [
      { id: 4, body: 'batch one' },
      { id: 5, body: 'batch two' },
    ])
    await db.executeTransaction([{ sql: 'INSERT INTO notes (id, body) VALUES (?, ?)', params: [6, 'positional'] }])

    expect(columnRaw(path, 'SELECT body FROM notes ORDER BY id')).toEqual([
      'colon',
      'at sign',
      'dollar',
      'batch one',
      'batch two',
      'positional',
    ])
    expect(await db.query('SELECT id FROM notes WHERE body = :body OR id = :id', { body: 'dollar', id: 6 })).toEqual([
      { id: 3 },
      { id: 6 },
    ])
  })
})
