import { existsSync, mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { runBulkLoad } from '../bulk-load.js'
import type { SQLiteConnection } from '../driver/types.js'
import { SirannonError } from '../errors.js'
import { Sirannon } from '../sirannon.js'
import type { BulkLoadDurability, ChangeEvent, ExecuteResult } from '../types.js'

function createStubWriter(executedSql: string[], failOn?: string): SQLiteConnection {
  return {
    async exec(sql: string): Promise<void> {
      if (failOn !== undefined && sql.includes(failOn)) {
        throw new Error(`stub failure on ${failOn}`)
      }
      executedSql.push(sql)
    },
    async prepare(): Promise<never> {
      throw new Error('not used by runBulkLoad')
    },
    async transaction<T>(): Promise<T> {
      throw new Error('not used by runBulkLoad')
    },
    async close(): Promise<void> {},
  }
}

describe('runBulkLoad pragma sequencing', () => {
  it('relaxes durability, loads, restores, then checkpoints', async () => {
    const executedSql: string[] = []
    const writer = createStubWriter(executedSql)
    const loadResults: ExecuteResult[] = [{ changes: 1, lastInsertRowId: 1 }]

    const results = await runBulkLoad({
      writer,
      configuredSynchronous: 'full',
      walMode: true,
      durability: undefined,
      execute: async () => {
        executedSql.push('<load>')
        return loadResults
      },
    })

    expect(results).toBe(loadResults)
    expect(executedSql).toEqual([
      'PRAGMA synchronous = OFF',
      '<load>',
      'PRAGMA synchronous = FULL',
      'PRAGMA wal_checkpoint(TRUNCATE)',
    ])
  })

  it('uses the requested load durability', async () => {
    const executedSql: string[] = []
    await runBulkLoad({
      writer: createStubWriter(executedSql),
      configuredSynchronous: 'normal',
      walMode: true,
      durability: 'normal',
      execute: async () => [],
    })
    expect(executedSql[0]).toBe('PRAGMA synchronous = NORMAL')
  })

  it('skips the checkpoint when WAL mode is off', async () => {
    const executedSql: string[] = []
    await runBulkLoad({
      writer: createStubWriter(executedSql),
      configuredSynchronous: 'normal',
      walMode: false,
      durability: undefined,
      execute: async () => [],
    })
    expect(executedSql).not.toContain('PRAGMA wal_checkpoint(TRUNCATE)')
  })

  it('restores the configured level when the load fails, without a checkpoint', async () => {
    const executedSql: string[] = []
    const writer = createStubWriter(executedSql)

    await expect(
      runBulkLoad({
        writer,
        configuredSynchronous: 'full',
        walMode: true,
        durability: undefined,
        execute: async () => {
          throw new Error('load exploded')
        },
      }),
    ).rejects.toThrow('load exploded')

    expect(executedSql).toEqual(['PRAGMA synchronous = OFF', 'PRAGMA synchronous = FULL'])
  })

  it('propagates the load error even when the restore also fails', async () => {
    const executedSql: string[] = []
    const writer = createStubWriter(executedSql, 'synchronous = FULL')

    await expect(
      runBulkLoad({
        writer,
        configuredSynchronous: 'full',
        walMode: true,
        durability: undefined,
        execute: async () => {
          throw new Error('load exploded')
        },
      }),
    ).rejects.toThrow('load exploded')
  })

  it('rejects an invalid durability level before touching the writer', async () => {
    const executedSql: string[] = []
    await expect(
      runBulkLoad({
        writer: createStubWriter(executedSql),
        configuredSynchronous: 'normal',
        walMode: true,
        durability: 'everything' as BulkLoadDurability,
        execute: async () => [],
      }),
    ).rejects.toMatchObject({ code: 'INVALID_DURABILITY' })
    expect(executedSql).toEqual([])
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

  it('loads rows atomically and restores the configured durability', async () => {
    const db = await sirannon.open('load', join(tempDir, 'load.db'), { synchronous: 'full' })
    await db.execute('CREATE TABLE readings (id INTEGER PRIMARY KEY, value REAL)')

    const paramsBatch = Array.from({ length: 500 }, (_, i) => [i + 1, i * 0.5])
    const results = await db.bulkLoad('INSERT INTO readings (id, value) VALUES (?, ?)', paramsBatch)

    expect(results).toHaveLength(500)
    const rows = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM readings')
    expect(rows[0].count).toBe(500)

    const fullLevel = 2
    expect(await writerSynchronousLevel(db)).toBe(fullLevel)
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
    const fullLevel = 2
    expect(await writerSynchronousLevel(db)).toBe(fullLevel)

    const corrected = [
      [1, 1.0],
      [2, 2.0],
      [3, 3.0],
    ]
    const results = await db.bulkLoad('INSERT INTO readings (id, value) VALUES (?, ?)', corrected)
    expect(results).toHaveLength(3)
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
    const fullLevel = 2
    expect(await writerSynchronousLevel(db)).toBe(fullLevel)

    const defaultDb = await sirannon.open('sync-default', join(tempDir, 'sync-default.db'))
    const normalLevel = 1
    expect(await writerSynchronousLevel(defaultDb)).toBe(normalLevel)
  })
})
