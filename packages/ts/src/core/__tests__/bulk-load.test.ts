import { existsSync, mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { runBulkLoad } from '../bulk-load.js'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { Sirannon } from '../sirannon.js'
import type { BulkLoadDurability, BulkLoadResult, ChangeEvent } from '../types.js'

interface StubOptions {
  failExecOn?: string
  checkpointBusy?: boolean
}

function createStubWriter(log: string[], options?: StubOptions): SQLiteConnection {
  return {
    async exec(sql: string): Promise<void> {
      if (options?.failExecOn !== undefined && sql.includes(options.failExecOn)) {
        throw new Error(`stub failure on ${options.failExecOn}`)
      }
      log.push(sql)
    },
    async prepare(sql: string) {
      log.push(sql)
      return {
        async all<T = unknown>(): Promise<T[]> {
          return []
        },
        async get<T = unknown>(): Promise<T | undefined> {
          return { busy: options?.checkpointBusy ? 1 : 0 } as T
        },
        async run() {
          return { changes: 0, lastInsertRowId: 0 }
        },
      }
    },
    async transaction<T>(): Promise<T> {
      throw new Error('not used by runBulkLoad')
    },
    async close(): Promise<void> {},
  }
}

const oneRow: BulkLoadResult = { rowsLoaded: 1, changes: 1 }

describe('runBulkLoad pragma sequencing', () => {
  it('relaxes durability, loads, restores, then checkpoints', async () => {
    const log: string[] = []
    const summary = await runBulkLoad({
      writer: createStubWriter(log),
      configuredSynchronous: 'full',
      walMode: true,
      durability: undefined,
      loadRows: async () => {
        log.push('<load>')
        return oneRow
      },
    })

    expect(summary).toEqual(oneRow)
    expect(log).toEqual([
      'PRAGMA synchronous = OFF',
      '<load>',
      'PRAGMA synchronous = FULL',
      'PRAGMA wal_checkpoint(TRUNCATE)',
    ])
  })

  it('skips the checkpoint on a non-final load but still restores durability', async () => {
    const log: string[] = []
    const summary = await runBulkLoad({
      writer: createStubWriter(log),
      configuredSynchronous: 'full',
      walMode: true,
      durability: undefined,
      checkpoint: false,
      loadRows: async () => {
        log.push('<load>')
        return oneRow
      },
    })

    expect(summary).toEqual(oneRow)
    expect(log).toEqual(['PRAGMA synchronous = OFF', '<load>', 'PRAGMA synchronous = FULL'])
    expect(log).not.toContain('PRAGMA wal_checkpoint(TRUNCATE)')
  })

  it('uses the requested load durability', async () => {
    const log: string[] = []
    await runBulkLoad({
      writer: createStubWriter(log),
      configuredSynchronous: 'normal',
      walMode: true,
      durability: 'normal',
      loadRows: async () => oneRow,
    })
    expect(log[0]).toBe('PRAGMA synchronous = NORMAL')
  })

  it('skips the checkpoint when WAL mode is off', async () => {
    const log: string[] = []
    await runBulkLoad({
      writer: createStubWriter(log),
      configuredSynchronous: 'normal',
      walMode: false,
      durability: undefined,
      loadRows: async () => oneRow,
    })
    expect(log).not.toContain('PRAGMA wal_checkpoint(TRUNCATE)')
  })

  it('retries a busy checkpoint before giving up on a committed load', async () => {
    const log: string[] = []
    const summary = await runBulkLoad({
      writer: createStubWriter(log, { checkpointBusy: true }),
      configuredSynchronous: 'full',
      walMode: true,
      durability: undefined,
      loadRows: async () => oneRow,
    })
    expect(summary).toEqual(oneRow)
    expect(log.filter(sql => sql === 'PRAGMA wal_checkpoint(TRUNCATE)')).toHaveLength(3)
  })

  it('restores the configured level when the load fails, without a checkpoint', async () => {
    const log: string[] = []
    await expect(
      runBulkLoad({
        writer: createStubWriter(log),
        configuredSynchronous: 'full',
        walMode: true,
        durability: undefined,
        loadRows: async () => {
          throw new Error('load exploded')
        },
      }),
    ).rejects.toThrow('load exploded')

    expect(log).toEqual(['PRAGMA synchronous = OFF', 'PRAGMA synchronous = FULL'])
  })

  it('reports a committed load whose restore fails as DURABILITY_RESTORE_FAILED', async () => {
    await expect(
      runBulkLoad({
        writer: createStubWriter([], { failExecOn: 'synchronous = FULL' }),
        configuredSynchronous: 'full',
        walMode: true,
        durability: undefined,
        loadRows: async () => oneRow,
      }),
    ).rejects.toMatchObject({ code: 'DURABILITY_RESTORE_FAILED' })
  })

  it('keeps the load error dominant when both the load and the restore fail', async () => {
    await expect(
      runBulkLoad({
        writer: createStubWriter([], { failExecOn: 'synchronous = FULL' }),
        configuredSynchronous: 'full',
        walMode: true,
        durability: undefined,
        loadRows: async () => {
          throw new Error('load exploded')
        },
      }),
    ).rejects.toThrow('load exploded')
  })

  it('rejects an invalid durability level before touching the writer', async () => {
    const log: string[] = []
    await expect(
      runBulkLoad({
        writer: createStubWriter(log),
        configuredSynchronous: 'normal',
        walMode: true,
        durability: 'everything' as BulkLoadDurability,
        loadRows: async () => oneRow,
      }),
    ).rejects.toMatchObject({ code: 'INVALID_DURABILITY' })
    expect(log).toEqual([])
  })
})

describe('Database.bulkLoad', () => {
  let tempDir: string
  let sirannon: Sirannon

  const driver = betterSqlite3()

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-bulk-load-'))
    sirannon = new Sirannon({ driver })
  })

  afterEach(async () => {
    await sirannon.shutdown()
    rmSync(tempDir, { recursive: true, force: true })
  })

  async function writerSynchronousLevel(db: Awaited<ReturnType<Sirannon['open']>>): Promise<number> {
    return db.transaction(async tx => {
      const rows = await tx.query<{ synchronous: number }>('PRAGMA synchronous')
      return rows[0].synchronous
    })
  }

  const FULL = 2
  const NORMAL = 1

  it('loads rows atomically and restores the configured durability', async () => {
    const db = await sirannon.open('load', join(tempDir, 'load.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    const paramsBatch = Array.from({ length: 500 }, (_, i) => [i + 1, i * 0.5])
    const summary = await db.bulkLoad('INSERT INTO readings (id, value) VALUES (?, ?)', paramsBatch)

    expect(summary.rowsLoaded).toBe(500)
    expect(summary.changes).toBe(500)
    const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
    expect(rows[0].count).toBe(500)
    expect(await writerSynchronousLevel(db)).toBe(FULL)
  })

  it('truncates the WAL after a successful load', async () => {
    const path = join(tempDir, 'wal.db')
    const db = await sirannon.open('wal', path)
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    await db.bulkLoad(
      'INSERT INTO readings (id, value) VALUES (?, ?)',
      Array.from({ length: 100 }, (_, i) => [i + 1, i]),
    )

    const walPath = `${path}-wal`
    if (existsSync(walPath)) {
      expect(statSync(walPath).size).toBe(0)
    }
  })

  it('checkpoints once at the end of a multi-batch load and keeps measured writes at full durability', async () => {
    const path = join(tempDir, 'multi-batch.db')
    const db = await sirannon.open('multi-batch', path, { synchronous: 'full' })
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    const walPath = `${path}-wal`
    const insert = 'INSERT INTO readings (id, value) VALUES (?, ?)'
    const batchFrom = (start: number): number[][] => Array.from({ length: 200 }, (_, i) => [start + i, i])

    await db.bulkLoad(insert, batchFrom(1), { durability: 'off', checkpoint: false })
    await db.bulkLoad(insert, batchFrom(201), { durability: 'off', checkpoint: false })

    expect(existsSync(walPath)).toBe(true)
    expect(statSync(walPath).size).toBeGreaterThan(0)
    expect(await writerSynchronousLevel(db)).toBe(FULL)

    await db.bulkLoad(insert, batchFrom(401), { durability: 'off' })

    if (existsSync(walPath)) {
      expect(statSync(walPath).size).toBe(0)
    }
    const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
    expect(rows[0].count).toBe(600)

    await db.execute('INSERT INTO readings (id, value) VALUES (99999, 1.5)')
    expect(await writerSynchronousLevel(db)).toBe(FULL)
  })

  it('rolls back an interrupted load and stays recoverable by re-running it', async () => {
    const db = await sirannon.open('recover', join(tempDir, 'recover.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    const failing = [
      [1, 1.0],
      [2, 2.0],
      [1, 3.0],
    ]
    await expect(db.bulkLoad('INSERT INTO readings (id, value) VALUES (?, ?)', failing)).rejects.toMatchObject({
      code: 'QUERY_ERROR',
    })

    const afterFailure = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
    expect(afterFailure[0].count).toBe(0)
    expect(await writerSynchronousLevel(db)).toBe(FULL)

    const corrected = [
      [1, 1.0],
      [2, 2.0],
      [3, 3.0],
    ]
    const summary = await db.bulkLoad('INSERT INTO readings (id, value) VALUES (?, ?)', corrected)
    expect(summary.rowsLoaded).toBe(3)
  })

  it('holds durability at the configured level for a concurrent write during a load', async () => {
    const db = await sirannon.open('concurrent', join(tempDir, 'concurrent.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    const load = db.bulkLoad(
      'INSERT INTO readings (id, value) VALUES (?, ?)',
      Array.from({ length: 2000 }, (_, i) => [i + 1, i]),
      { durability: 'off' },
    )
    const concurrentWrite = db.execute('INSERT INTO readings (id, value) VALUES (100001, 9.9)')

    await Promise.all([load, concurrentWrite])

    expect(await writerSynchronousLevel(db)).toBe(FULL)
    const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
    expect(rows[0].count).toBe(2001)
  })

  it('serialises two overlapping loads without a nested-transaction error', async () => {
    const db = await sirannon.open('overlap', join(tempDir, 'overlap.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    const first = db.bulkLoad(
      'INSERT INTO readings (id, value) VALUES (?, ?)',
      Array.from({ length: 500 }, (_, i) => [i + 1, i]),
    )
    const second = db.bulkLoad(
      'INSERT INTO readings (id, value) VALUES (?, ?)',
      Array.from({ length: 500 }, (_, i) => [i + 501, i]),
    )

    const [firstSummary, secondSummary] = await Promise.all([first, second])
    expect(firstSummary.rowsLoaded).toBe(500)
    expect(secondSummary.rowsLoaded).toBe(500)
    expect(await writerSynchronousLevel(db)).toBe(FULL)
  })

  it('is visible to CDC subscribers', async () => {
    const db = await sirannon.open('cdc', join(tempDir, 'cdc.db'))
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')
    await db.watch('readings')

    const events: ChangeEvent[] = []
    const sub = db.on('readings').subscribe(event => {
      events.push(event)
    })

    await db.bulkLoad('INSERT INTO readings (id, value) VALUES (?, ?)', [
      [1, 1.0],
      [2, 2.0],
    ])

    await new Promise(resolve => setTimeout(resolve, 300))
    sub.unsubscribe()
    expect(events).toHaveLength(2)
    expect(events.every(event => event.type === 'insert')).toBe(true)
  })

  it('rejects loads on a read-only database', async () => {
    const path = join(tempDir, 'readonly.db')
    const setup = await sirannon.open('setup', path)
    await setup.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY)')
    await setup.close()

    const db = await sirannon.open('readonly', path, { readOnly: true })
    await expect(db.bulkLoad('INSERT INTO readings (id) VALUES (?)', [[1]])).rejects.toBeInstanceOf(SirannonError)
  })

  it('applies the configured synchronous level when opening', async () => {
    const db = await sirannon.open('sync-full', join(tempDir, 'sync-full.db'), { synchronous: 'full' })
    expect(await writerSynchronousLevel(db)).toBe(FULL)

    const defaultDb = await sirannon.open('sync-default', join(tempDir, 'sync-default.db'))
    expect(await writerSynchronousLevel(defaultDb)).toBe(NORMAL)
  })
})
