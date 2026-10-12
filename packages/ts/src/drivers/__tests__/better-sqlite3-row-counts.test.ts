import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  builtRowCountingBindingPath,
  ROW_COUNTING_SQLITE_REQUIRED,
  stockBetterSqliteBindingPath,
} from '../../__tests__/helpers/row-counting-sqlite.js'
import { SirannonError } from '../../core/errors.js'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../better-sqlite3/index.js'

const SEEDED_ROWS = 300
const binding = builtRowCountingBindingPath()

let dir: string

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-driver-row-counts-'))
})

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

describe('the row-counting build', () => {
  it.skipIf(!ROW_COUNTING_SQLITE_REQUIRED)('is built for this runner', () => {
    expect(binding).not.toBeNull()
  })
})

describe.skipIf(binding === null)('betterSqlite3({ rowCounts: true })', () => {
  it('refuses to open a database on a SQLite build that has no row counters', async () => {
    const sirannon = new Sirannon({
      driver: betterSqlite3({ rowCounts: true, rowCountsBindingPath: stockBetterSqliteBindingPath() }),
    })
    const opening = sirannon.open('tenant', join(dir, 'tenant.db'))

    await expect(opening).rejects.toBeInstanceOf(SirannonError)
    await expect(opening).rejects.toMatchObject({
      code: 'ROW_COUNTS_UNSUPPORTED',
      message: expect.stringContaining('has no row counters'),
    })
    await sirannon.shutdown()
  })

  it('refuses to open a database when the row-counting build is missing', async () => {
    const driver = betterSqlite3({ rowCounts: true, rowCountsBindingPath: join(dir, 'missing.node') })

    await expect(driver.open(join(dir, 'tenant.db'))).rejects.toMatchObject({ code: 'ROW_COUNTS_UNSUPPORTED' })
  })

  it('gives a counted call only its own rows, after uncounted calls on the same statement', async () => {
    const conn = await betterSqlite3({ rowCounts: true, rowCountsBindingPath: binding ?? undefined }).open(
      join(dir, 'tenant.db'),
    )
    await conn.exec('CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL)')
    await conn.runBatchSummary?.(
      'INSERT INTO events (kind) VALUES (?)',
      Array.from({ length: SEEDED_ROWS }, (_, index) => [`kind-${index}`]),
    )
    const scan = await conn.prepare('SELECT * FROM events WHERE kind = ?')
    await scan.all('kind-0')
    await scan.get('kind-1')

    const counted = await scan.allCounted?.('kind-2')
    expect(counted).toMatchObject({ rowsRead: SEEDED_ROWS, rowsWritten: 0 })
    expect(counted?.value).toHaveLength(1)
    await conn.close()
  })
})
