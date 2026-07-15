import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { Database } from '../../database.js'
import { defineDriver } from '../../driver/define.js'
import { Sirannon } from '../../sirannon.js'
import type { ChangeEvent } from '../../types.js'
import { EXIT_TABLE, exitingDriver } from './fixtures/exiting-driver.js'

let dir: string
let sirannon: Sirannon
let db: Database

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'sirannon-writer-worker-'))
  sirannon = new Sirannon({ driver: betterSqlite3() })
})

afterEach(async () => {
  await sirannon.shutdown().catch(() => {})
  rmSync(dir, { recursive: true, force: true })
})

async function openOffloaded(overrides: Record<string, unknown> = {}): Promise<Database> {
  db = await sirannon.open('main', join(dir, 'data.db'), {
    synchronous: 'full',
    writerWorker: true,
    ...overrides,
  })
  return db
}

describe('writer worker offload', () => {
  it('commits writes on the worker and reads them back on the calling thread', async () => {
    await openOffloaded()
    await db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
    const result = await db.execute('INSERT INTO items (name) VALUES (?)', ['widget'])
    expect(result.changes).toBe(1)

    const rows = await db.query<{ id: number; name: string }>('SELECT id, name FROM items')
    expect(rows).toEqual([{ id: 1, name: 'widget' }])
  })

  it('runs a batch atomically and returns a result per row', async () => {
    await openOffloaded()
    await db.execute('CREATE TABLE nums (n INTEGER)')
    const results = await db.executeBatch('INSERT INTO nums (n) VALUES (?)', [[1], [2], [3]])
    expect(results).toHaveLength(3)
    expect(results.every(r => r.changes === 1)).toBe(true)

    const rows = await db.query<{ n: number }>('SELECT n FROM nums ORDER BY n')
    expect(rows.map(r => r.n)).toEqual([1, 2, 3])
  })

  it('bulk loads through the worker and restores durability', async () => {
    await openOffloaded()
    await db.execute('CREATE TABLE bulk (n INTEGER)')
    const batch = Array.from({ length: 500 }, (_, i) => [i])
    const summary = await db.bulkLoad('INSERT INTO bulk (n) VALUES (?)', batch, { durability: 'off' })
    expect(summary.rowsLoaded).toBe(500)

    const [{ count }] = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM bulk')
    expect(count).toBe(500)
  })

  it('round-trips BigInt and blob values across the thread boundary', async () => {
    await openOffloaded()
    await db.execute('CREATE TABLE payloads (id INTEGER PRIMARY KEY, big INTEGER, data BLOB)')
    const big = 9007199254740993n
    const data = new Uint8Array([1, 2, 3, 250])
    await db.execute('INSERT INTO payloads (big, data) VALUES (?, ?)', [big, data])

    const [row] = await db.query<{ big: bigint; data: Uint8Array }>('SELECT big, data FROM payloads')
    expect(row.big).toBe(big)
    expect(Uint8Array.from(row.data)).toEqual(data)
  })

  it('rolls the transaction back on failure', async () => {
    await openOffloaded()
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE)')
    await db.execute('INSERT INTO t (v) VALUES (?)', ['a'])

    await expect(
      db.transaction(async tx => {
        await tx.execute('INSERT INTO t (v) VALUES (?)', ['b'])
        await tx.execute('INSERT INTO t (v) VALUES (?)', ['a'])
      }),
    ).rejects.toThrow()

    const [{ count }] = await db.query<{ count: number }>('SELECT COUNT(*) AS count FROM t')
    expect(count).toBe(1)
  })

  it('delivers change events for writes made on the worker', async () => {
    await openOffloaded({ cdcPollInterval: 10 })
    await db.execute('CREATE TABLE watched (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('watched')

    const event = await new Promise<ChangeEvent>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('no change event')), 5000)
      const sub = db.on('watched').subscribe(e => {
        clearTimeout(timer)
        sub.unsubscribe()
        resolve(e)
      })
      db.execute('INSERT INTO watched (name) VALUES (?)', ['live']).catch(reject)
    })

    expect(event.type).toBe('insert')
    expect(event.table).toBe('watched')
    expect((event.row as { name: string }).name).toBe('live')
  })

  it('offloads writes when the registry defaults the worker on', async () => {
    const registry = new Sirannon({ driver: betterSqlite3(), writerWorker: true })
    const offloaded = await registry.open('reg', join(dir, 'reg.db'), { synchronous: 'full' })
    await offloaded.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')
    await offloaded.execute('INSERT INTO t (name) VALUES (?)', ['from-registry'])

    const rows = await offloaded.query<{ name: string }>('SELECT name FROM t')
    expect(rows).toEqual([{ name: 'from-registry' }])
    await registry.shutdown().catch(() => {})
  })

  it('lets an open call opt out of the registry default', async () => {
    const registry = new Sirannon({ driver: betterSqlite3(), writerWorker: true })
    const plain = await registry.open('plain', join(dir, 'plain.db'), { writerWorker: false })
    await plain.execute('CREATE TABLE t (n INTEGER)')
    const result = await plain.execute('INSERT INTO t (n) VALUES (?)', [1])
    expect(result.changes).toBe(1)
    await registry.shutdown().catch(() => {})
  })

  it('propagates the registry default to the unsupported-driver check', async () => {
    const base = betterSqlite3()
    const noWorker = defineDriver({ capabilities: base.capabilities, open: base.open })
    const registry = new Sirannon({ driver: noWorker, writerWorker: true })
    await expect(registry.open('x', join(dir, 'x.db'))).rejects.toMatchObject({
      code: 'WRITER_WORKER_UNSUPPORTED',
    })
    await registry.shutdown().catch(() => {})
  })

  it('refuses to open when the driver has no worker entry', async () => {
    const base = betterSqlite3()
    const noWorker = defineDriver({ capabilities: base.capabilities, open: base.open })
    const registry = new Sirannon({ driver: noWorker })
    await expect(registry.open('x', join(dir, 'x.db'), { writerWorker: true })).rejects.toMatchObject({
      code: 'WRITER_WORKER_UNSUPPORTED',
    })
    await registry.shutdown().catch(() => {})
  })

  it('sheds load with a busy signal past the pending limit', async () => {
    await openOffloaded({ writerWorker: { maxPendingWrites: 1 } })
    await db.execute('CREATE TABLE q (n INTEGER)')

    const first = db.execute('INSERT INTO q (n) VALUES (?)', [1])
    const second = db.execute('INSERT INTO q (n) VALUES (?)', [2])

    await expect(second).rejects.toMatchObject({ code: 'WRITE_OVERLOADED' })
    await expect(first).resolves.toMatchObject({ changes: 1 })
  })

  it('rejects a stalled operation without killing the worker, then recovers', async () => {
    await openOffloaded({ writerWorker: { writeTimeoutMs: 1_000 } })
    await db.execute('CREATE TABLE sink (n INTEGER)')
    await db.execute('CREATE TABLE recovered (n INTEGER)')

    let stalledError: unknown
    try {
      await db.execute(
        'INSERT INTO sink SELECT count(*) FROM (WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM r WHERE x < 60000000) SELECT x FROM r)',
      )
    } catch (err) {
      stalledError = err
    }
    expect((stalledError as Error | undefined)?.message).toMatch(/did not respond within/)

    await vi.waitFor(
      async () => {
        const [{ c }] = await db.query<{ c: number }>('SELECT count(*) AS c FROM sink')
        expect(c).toBe(1)
      },
      { timeout: 30_000, interval: 100 },
    )

    const after = await db.execute('INSERT INTO recovered (n) VALUES (?)', [1])
    expect(after.changes).toBe(1)
  }, 40_000)

  it('rejects the in-flight write and respawns when the worker exits on its own', async () => {
    const registry = new Sirannon({ driver: exitingDriver() })
    try {
      const crashed = await registry.open('crash', join(dir, 'crash.db'), {
        writerWorker: { writeTimeoutMs: 2_000 },
      })
      await crashed.execute('CREATE TABLE survivors (n INTEGER)')

      await expect(crashed.execute(`INSERT INTO ${EXIT_TABLE} (n) VALUES (1)`)).rejects.toThrow(/exited with code/)
      await expect(crashed.execute('INSERT INTO survivors (n) VALUES (1)')).resolves.toMatchObject({ changes: 1 })
    } finally {
      await registry.shutdown().catch(() => {})
    }
  }, 20_000)

  it('keeps rejecting and recovering across repeated worker exits', async () => {
    const registry = new Sirannon({ driver: exitingDriver() })
    try {
      const crashed = await registry.open('repeat', join(dir, 'repeat.db'), {
        writerWorker: { writeTimeoutMs: 2_000 },
      })
      await crashed.execute('CREATE TABLE survivors (n INTEGER)')

      for (let attempt = 0; attempt < 3; attempt++) {
        await expect(crashed.execute(`INSERT INTO ${EXIT_TABLE} (n) VALUES (1)`)).rejects.toThrow(/exited with code/)
        await expect(crashed.execute('INSERT INTO survivors (n) VALUES (?)', [attempt])).resolves.toMatchObject({
          changes: 1,
        })
      }

      expect(await crashed.query('SELECT count(*) AS c FROM survivors')).toEqual([{ c: 3 }])
    } finally {
      await registry.shutdown().catch(() => {})
    }
  }, 30_000)

  it('fails writes permanently once the worker exceeds maxRestarts', async () => {
    const registry = new Sirannon({ driver: exitingDriver() })
    try {
      const crashed = await registry.open('fatal', join(dir, 'fatal.db'), {
        writerWorker: { writeTimeoutMs: 2_000, maxRestarts: 2 },
      })

      for (let attempt = 0; attempt < 3; attempt++) {
        await expect(crashed.execute(`INSERT INTO ${EXIT_TABLE} (n) VALUES (1)`)).rejects.toThrow(/exited with code/)
      }

      await expect(crashed.execute(`INSERT INTO ${EXIT_TABLE} (n) VALUES (1)`)).rejects.toMatchObject({
        code: 'WRITER_WORKER_FATAL',
      })
      await expect(crashed.execute('SELECT 1')).rejects.toMatchObject({ code: 'WRITER_WORKER_FATAL' })
    } finally {
      await registry.shutdown().catch(() => {})
    }
  }, 30_000)

  it('closes cleanly and can reopen', async () => {
    await openOffloaded()
    await db.execute('CREATE TABLE c (n INTEGER)')
    await db.close()

    const reopened = await sirannon.open('again', join(dir, 'data.db'), { writerWorker: true })
    const [{ count }] = await reopened.query<{ count: number }>(
      "SELECT COUNT(*) AS count FROM sqlite_master WHERE name = 'c'",
    )
    expect(count).toBe(1)
  })
})
