import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { collect, createWorkspace, PLACEMENTS, readRaw, type Workspace, waitForCount } from './harness.js'

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER)
const LARGE_VALUE_BYTES = 1024 * 1024

function deterministicBytes(length: number, seed: number): Buffer {
  const bytes = Buffer.alloc(length)
  let state = seed
  for (let index = 0; index < length; index++) {
    state = (state * 1103515245 + 12345) % 2147483648
    bytes[index] = state & 0xff
  }
  return bytes
}

interface Cell {
  label: string
  written: unknown
  storageClass: 'integer' | 'real' | 'text' | 'blob' | 'null'
}

const CELLS: Cell[] = [
  { label: 'zero', written: 0n, storageClass: 'integer' },
  { label: 'largest safe integer', written: MAX_SAFE, storageClass: 'integer' },
  { label: 'smallest safe integer', written: -MAX_SAFE, storageClass: 'integer' },
  { label: 'a whole number bound as a double', written: 42, storageClass: 'real' },
  { label: 'just past the safe range', written: MAX_SAFE + 2n, storageClass: 'integer' },
  { label: 'just below the safe range', written: -MAX_SAFE - 2n, storageClass: 'integer' },
  { label: 'largest 64-bit integer', written: 2n ** 63n - 1n, storageClass: 'integer' },
  { label: 'smallest 64-bit integer', written: -(2n ** 63n), storageClass: 'integer' },
  { label: 'a tenth', written: 0.1, storageClass: 'real' },
  { label: 'a sum that binary floating point cannot hold', written: 0.1 + 0.2, storageClass: 'real' },
  { label: 'seventeen significant digits', written: 123456789.12345678, storageClass: 'real' },
  { label: 'largest double', written: Number.MAX_VALUE, storageClass: 'real' },
  { label: 'most negative double', written: -Number.MAX_VALUE, storageClass: 'real' },
  { label: 'smallest subnormal double', written: Number.MIN_VALUE, storageClass: 'real' },
  { label: 'positive infinity', written: Number.POSITIVE_INFINITY, storageClass: 'real' },
  { label: 'negative infinity', written: Number.NEGATIVE_INFINITY, storageClass: 'real' },
  { label: 'empty text', written: '', storageClass: 'text' },
  { label: 'text holding a NUL and astral characters', written: 'nul\u0000between 😀 ∑ 𝄞', storageClass: 'text' },
  { label: 'text shaped like an integer envelope', written: '{"__sirannon_int":"5"}', storageClass: 'text' },
  { label: 'text shaped like a blob envelope', written: '{"__sirannon_blob":"FF"}', storageClass: 'text' },
  { label: 'a megabyte of text', written: 'ab'.repeat(LARGE_VALUE_BYTES / 2), storageClass: 'text' },
  { label: 'empty blob', written: Buffer.alloc(0), storageClass: 'blob' },
  {
    label: 'every byte value',
    written: Buffer.from(Array.from({ length: 256 }, (_, index) => index)),
    storageClass: 'blob',
  },
  { label: 'a megabyte of bytes', written: deterministicBytes(LARGE_VALUE_BYTES, 7), storageClass: 'blob' },
  { label: 'null', written: null, storageClass: 'null' },
]

function asDelivered(value: unknown): unknown {
  if (typeof value === 'bigint' && value >= -MAX_SAFE && value <= MAX_SAFE) return Number(value)
  return value
}

function comparable(row: Record<string, unknown> | undefined): Record<string, unknown> | undefined {
  if (row === undefined) return undefined
  return Object.fromEntries(
    Object.entries(row).map(([column, value]) => [
      column,
      value instanceof Uint8Array ? `bytes:${Buffer.from(value).toString('hex')}` : value,
    ]),
  )
}

let workspace: Workspace

beforeEach(() => {
  workspace = createWorkspace('values')
})

afterEach(async () => {
  await workspace.dispose()
})

describe.each(PLACEMENTS)('change event values with $label', placement => {
  it('delivers every storage class exactly as SQLite stored it, at the edges of each, to every subscriber', async () => {
    const db = await workspace.open('cells', { writerWorker: placement.writerWorker }, placement.driver())
    await db.execute('CREATE TABLE cells (id INTEGER PRIMARY KEY, value)')
    await db.watch('cells')
    const first = collect(db.on('cells'))
    const second = collect(db.on('cells'))

    for (const [index, cell] of CELLS.entries()) {
      await db.execute('INSERT INTO cells (id, value) VALUES (?, ?)', [index, cell.written])
    }
    const stored = readRaw<{ value: unknown; kind: string }>(
      workspace.path('cells'),
      'SELECT value, typeof(value) AS kind FROM cells ORDER BY id',
    )
    expect(stored.map(row => [asDelivered(row.value), row.kind])).toEqual(
      CELLS.map(cell => [asDelivered(cell.written), cell.storageClass]),
    )
    const values = stored.map(row => asDelivered(row.value))
    const next = (index: number) => values[(index + 1) % values.length]

    for (let index = 0; index < CELLS.length; index++) {
      await db.execute('UPDATE cells SET value = ? WHERE id = ?', [CELLS[(index + 1) % CELLS.length].written, index])
    }
    await db.execute('DELETE FROM cells')
    await waitForCount(first, CELLS.length * 3)

    const expected = [
      ...values.map((value, index) => ({ type: 'insert', row: { id: index, value }, oldRow: undefined })),
      ...values.map((value, index) => ({
        type: 'update',
        row: { id: index, value: next(index) },
        oldRow: { id: index, value },
      })),
      ...values.map((_, index) => ({ type: 'delete', row: {}, oldRow: { id: index, value: next(index) } })),
    ]
    for (const collected of [first, second]) {
      expect(collected.events).toHaveLength(expected.length)
      for (const [index, event] of collected.events.entries()) {
        const want = expected[index]
        expect(
          { type: event.type, row: comparable(event.row), oldRow: comparable(event.oldRow) },
          `${want.type} of ${CELLS[index % CELLS.length].label}`,
        ).toEqual({ type: want.type, row: comparable(want.row), oldRow: comparable(want.oldRow) })
      }
    }
  })
})

describe('column names that JavaScript objects treat specially', () => {
  it('delivers columns named __proto__, constructor, and toString as ordinary values of the row', async () => {
    const db = await workspace.open('prototype')
    await db.execute('CREATE TABLE __proto__ (id INTEGER PRIMARY KEY, __proto__, "constructor" TEXT, "toString" TEXT)')
    await db.watch('__proto__')
    const seen = collect(db.on('__proto__'))
    const matching = collect(db.on('__proto__').filter(JSON.parse('{"__proto__":"owner"}')))

    await db.execute('INSERT INTO __proto__ VALUES (?, ?, ?, ?)', [1, 'owner', 'built', 'text'])
    await db.execute('INSERT INTO __proto__ VALUES (?, ?, ?, ?)', [2, null, null, null])
    await db.execute('INSERT INTO __proto__ VALUES (?, ?, ?, ?)', [3, Buffer.from([1, 2]), 'x', 'y'])
    await waitForCount(seen, 3)

    const rows = seen.events.map(event => event.row)
    for (const row of rows) {
      expect(Object.getPrototypeOf(row)).toBe(Object.prototype)
      expect(Object.keys(row)).toEqual(['id', '__proto__', 'constructor', 'toString'])
    }
    expect(rows.map(row => Object.getOwnPropertyDescriptor(row, '__proto__')?.value)).toEqual([
      'owner',
      null,
      Buffer.from([1, 2]),
    ])
    expect(rows.map(row => [row.constructor, row.toString])).toEqual([
      ['built', 'text'],
      [null, null],
      ['x', 'y'],
    ])
    expect(matching.events.map(event => event.row.id)).toEqual([1])
  })
})
