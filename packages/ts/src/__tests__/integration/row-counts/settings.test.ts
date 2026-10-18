import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, relative } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SirannonError } from '../../../core/errors.js'
import { Sirannon } from '../../../core/sirannon.js'
import type { QueryMetrics } from '../../../core/types.js'
import { type BetterSqlite3Options, betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { builtSirannonSqliteBindingPath, stockBetterSqliteBindingPath } from '../../helpers/sirannon-sqlite.js'

const SEEDED_ORDERS = 120
const LAST_CUSTOMER = `customer-${SEEDED_ORDERS - 1}`
const SCAN_BY_CUSTOMER = 'SELECT * FROM orders WHERE customer = ?'
const BLANK_TOTAL_BY_CUSTOMER = 'UPDATE orders SET total = NULL WHERE customer = ?'

const binding = builtSirannonSqliteBindingPath()

let dir: string
let reported: QueryMetrics[]
const registries: Sirannon[] = []

function registry(options?: BetterSqlite3Options): Sirannon {
  const sirannon = new Sirannon({
    driver: betterSqlite3(options),
    metrics: { onQueryComplete: metrics => reported.push(metrics) },
  })
  registries.push(sirannon)
  return sirannon
}

async function openOrders(sirannon: Sirannon): Promise<void> {
  const db = await sirannon.open('tenant', join(dir, 'tenant.db'))
  await db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT NOT NULL, total INTEGER NOT NULL)')
  await db.bulkLoad(
    'INSERT INTO orders (customer, total) VALUES (?, ?)',
    Array.from({ length: SEEDED_ORDERS }, (_, index) => [`customer-${index}`, index]),
  )
  reported = []
  await db.query(SCAN_BY_CUSTOMER, [LAST_CUSTOMER])
  await expect(db.execute(BLANK_TOTAL_BY_CUSTOMER, [LAST_CUSTOMER])).rejects.toThrow(/NOT NULL/)
}

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-row-count-settings-'))
  reported = []
})

afterEach(async () => {
  for (const sirannon of registries.splice(0)) await sirannon.shutdown()
  rmSync(dir, { recursive: true, force: true })
})

describe.skipIf(binding === null)('betterSqlite3 row counts under bad settings', () => {
  it('refuses to open a database on the stock better-sqlite3 build, which has no row counters', async () => {
    const opening = registry({ rowCounts: true, sqliteBindingPath: stockBetterSqliteBindingPath() }).open(
      'tenant',
      join(dir, 'tenant.db'),
    )

    await expect(opening).rejects.toBeInstanceOf(SirannonError)
    await expect(opening).rejects.toMatchObject({
      code: 'ROW_COUNTS_UNSUPPORTED',
      message: expect.stringContaining('has no row counters'),
    })
  })

  it('refuses to open a database when the binding path names a missing file or a file that is no native module', async () => {
    const notAModule = join(dir, 'not-a-module.node')
    writeFileSync(notAModule, 'these bytes are not a native module')

    for (const sqliteBindingPath of [join(dir, 'missing.node'), notAModule, dir]) {
      await expect(
        registry({ rowCounts: true, sqliteBindingPath }).open('tenant', join(dir, 'tenant.db')),
      ).rejects.toMatchObject({ code: 'ROW_COUNTS_UNSUPPORTED', message: expect.stringContaining('failed to load') })
    }
  })

  it('resolves a relative binding path from the working directory and counts with it', async () => {
    const relativeBinding = relative(process.cwd(), binding ?? '')
    await openOrders(registry({ rowCounts: true, sqliteBindingPath: relativeBinding }))

    expect(reported.find(metrics => metrics.sql === SCAN_BY_CUSTOMER)).toMatchObject({
      rowsRead: SEEDED_ORDERS,
      rowsWritten: 0,
    })
    expect(reported.find(metrics => metrics.sql === BLANK_TOTAL_BY_CUSTOMER)).toMatchObject({
      error: true,
      rowsRead: SEEDED_ORDERS,
      rowsWritten: 0,
    })
  })

  it('reads no binding path while rowCounts is off, so a stale path cannot stop a database from opening', async () => {
    await openOrders(registry({ sqliteBindingPath: join(dir, 'missing.node') }))

    expect(reported.length).toBeGreaterThan(0)
  })
})

describe('a driver that counts no rows', () => {
  it('leaves rowsRead and rowsWritten out of every report, so that a host cannot mistake them for zero', async () => {
    await openOrders(registry())

    expect(reported.map(metrics => metrics.sql)).toEqual([SCAN_BY_CUSTOMER, BLANK_TOTAL_BY_CUSTOMER])
    for (const metrics of reported) {
      expect(metrics).not.toHaveProperty('rowsRead')
      expect(metrics).not.toHaveProperty('rowsWritten')
    }
  })
})
