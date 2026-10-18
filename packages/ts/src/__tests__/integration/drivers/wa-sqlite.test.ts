import 'fake-indexeddb/auto'
import { readFile } from 'node:fs/promises'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import type { Database } from '../../../core/database.js'
import { Sirannon } from '../../../core/sirannon.js'
import { waSqlite } from '../../../drivers/wa-sqlite/index.js'

const LARGEST_SAFE = 2n ** 53n - 1n
const LARGEST_INT64 = 2n ** 63n - 1n
const SMALLEST_INT64 = -(2n ** 63n)
const INSERT_CELL = 'INSERT INTO cells (id, value) VALUES (?, ?)'
const STORED_CELLS = `SELECT id, typeof(value) AS kind,
  CASE WHEN typeof(value) IN ('text', 'blob') THEN hex(CAST(value AS BLOB)) ELSE quote(value) END AS literal
  FROM cells ORDER BY id`
const MULTILINGUAL_TEXT = 'nul\u0000between 😀 𝄞'
const TEXT_OPENING_WITH_BYTE_ORDER_MARK = '﻿mark'
const EVERY_BYTE = Uint8Array.from({ length: 256 }, (_, index) => index)
const TRAILING_BLOB = Uint8Array.from([255, 0, 7])
const webLocksPresent = typeof navigator !== 'undefined' && navigator.locks !== undefined

interface StoredCell {
  id: number
  kind: string
  literal: string
}

const fetchOverNetwork = globalThis.fetch
const openTransaction = IDBDatabase.prototype.transaction

async function fetchWithFiles(input: string | URL | Request, init?: RequestInit): Promise<Response> {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
  if (!url.startsWith('file:')) return fetchOverNetwork(input, init)
  return new Response(await readFile(new URL(url)), { headers: { 'content-type': 'application/wasm' } })
}

function utf8Hex(text: string): string {
  return Buffer.from(text, 'utf8').toString('hex').toUpperCase()
}

function bytesHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex').toUpperCase()
}

function transactionOverStoreSequence(
  this: IDBDatabase,
  storeNames: string | Iterable<string>,
  ...rest: [IDBTransactionMode?, IDBTransactionOptions?]
): IDBTransaction {
  return openTransaction.call(this, typeof storeNames === 'string' ? storeNames : Array.from(storeNames), ...rest)
}

const registries: Sirannon[] = []
let opened = 0

async function openCells(): Promise<Database> {
  const sirannon = new Sirannon({ driver: waSqlite() })
  registries.push(sirannon)
  opened += 1
  const db = await sirannon.open('cells', `cells-${opened}.db`)
  await db.execute('CREATE TABLE cells (id INTEGER PRIMARY KEY, value)')
  return db
}

beforeAll(() => {
  vi.stubGlobal('fetch', fetchWithFiles)
  IDBDatabase.prototype.transaction = transactionOverStoreSequence
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown()
})

afterAll(() => {
  vi.unstubAllGlobals()
  IDBDatabase.prototype.transaction = openTransaction
})

describe.skipIf(!webLocksPresent)('values through the wa-sqlite driver', () => {
  it('stores every value that the caller binds, at the edges of each storage class, and reads each one back exactly', async () => {
    const db = await openCells()
    const written: unknown[] = [
      0,
      42,
      -1.5,
      Number.MAX_VALUE,
      LARGEST_SAFE,
      LARGEST_SAFE + 1n,
      -(2n ** 53n),
      LARGEST_INT64,
      SMALLEST_INT64,
      '',
      MULTILINGUAL_TEXT,
      TEXT_OPENING_WITH_BYTE_ORDER_MARK,
      new Uint8Array(0),
      EVERY_BYTE,
      TRAILING_BLOB,
      true,
      false,
      null,
    ]
    await db.executeBatch(
      INSERT_CELL,
      written.map((value, index) => [index, value]),
    )

    const stored = await db.query<StoredCell>(STORED_CELLS)
    expect(stored.map(cell => [cell.kind, cell.literal])).toEqual([
      ['real', '0.0'],
      ['real', '42.0'],
      ['real', '-1.5'],
      ['real', '1.797693134862315708e+308'],
      ['integer', '9007199254740991'],
      ['integer', '9007199254740992'],
      ['integer', '-9007199254740992'],
      ['integer', '9223372036854775807'],
      ['integer', '-9223372036854775808'],
      ['text', ''],
      ['text', utf8Hex(MULTILINGUAL_TEXT)],
      ['text', utf8Hex(TEXT_OPENING_WITH_BYTE_ORDER_MARK)],
      ['blob', ''],
      ['blob', bytesHex(EVERY_BYTE)],
      ['blob', bytesHex(TRAILING_BLOB)],
      ['integer', '1'],
      ['integer', '0'],
      ['null', 'NULL'],
    ])
    const read = await db.query<{ value: unknown }>('SELECT value FROM cells ORDER BY id')
    expect(read.map(row => row.value)).toEqual([
      0,
      42,
      -1.5,
      Number.MAX_VALUE,
      Number(LARGEST_SAFE),
      LARGEST_SAFE + 1n,
      -(2n ** 53n),
      LARGEST_INT64,
      SMALLEST_INT64,
      '',
      MULTILINGUAL_TEXT,
      TEXT_OPENING_WITH_BYTE_ORDER_MARK,
      new Uint8Array(0),
      EVERY_BYTE,
      TRAILING_BLOB,
      1,
      0,
      null,
    ])
  })

  it('binds named parameters in every prefix form, in a statement, a batch, and a transaction', async () => {
    const db = await openCells()
    await db.execute('INSERT INTO cells (id, value) VALUES (:id, :value)', { id: 1, value: 'colon' })
    await db.execute('INSERT INTO cells (id, value) VALUES (@id, @value)', { id: 2, value: true })
    await db.execute('INSERT INTO cells (id, value) VALUES ($id, $value)', { id: 3, value: LARGEST_INT64 })
    await db.executeBatch('INSERT INTO cells (id, value) VALUES (:id, :value)', [{ id: 4, value: new Uint8Array(0) }])
    await db.executeTransaction([
      { sql: 'INSERT INTO cells (id, value) VALUES (:id, :value)', params: { id: 5, value: 'in a transaction' } },
    ])

    expect(await db.query<StoredCell>(STORED_CELLS)).toEqual([
      { id: 1, kind: 'text', literal: utf8Hex('colon') },
      { id: 2, kind: 'integer', literal: '1' },
      { id: 3, kind: 'integer', literal: '9223372036854775807' },
      { id: 4, kind: 'blob', literal: '' },
      { id: 5, kind: 'text', literal: utf8Hex('in a transaction') },
    ])
    expect(
      await db.query('SELECT id FROM cells WHERE value = :value OR id = :id ORDER BY id', { value: true, id: 5 }),
    ).toEqual([{ id: 2 }, { id: 5 }])
  })

  it('refuses a value that SQLite cannot store and more values than the statement takes, writing nothing', async () => {
    const db = await openCells()
    const unbindable: unknown[] = [LARGEST_INT64 + 1n, SMALLEST_INT64 - 1n, { amount: 1 }, new Date(0), Symbol('cell')]

    for (const value of unbindable) {
      await expect(db.execute(INSERT_CELL, [1, value]), String(value)).rejects.toMatchObject({
        code: 'QUERY_ERROR',
        sql: INSERT_CELL,
      })
      await expect(
        db.executeBatch(INSERT_CELL, [
          [2, 'kept until the batch fails'],
          [3, value],
        ]),
      ).rejects.toMatchObject({
        code: 'QUERY_ERROR',
      })
      await expect(
        db.executeTransaction([
          { sql: INSERT_CELL, params: [4, 'kept until the transaction fails'] },
          { sql: INSERT_CELL, params: [5, value] },
        ]),
      ).rejects.toMatchObject({ code: 'QUERY_ERROR', sql: INSERT_CELL })
    }
    await expect(db.execute(INSERT_CELL, [6, 'one', 'too many'])).rejects.toMatchObject({ code: 'QUERY_ERROR' })

    expect(await db.query('SELECT count(*) AS n FROM cells')).toEqual([{ n: 0 }])
  })
})
