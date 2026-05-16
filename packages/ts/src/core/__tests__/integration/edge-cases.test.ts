import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { HookDeniedError } from '../../errors.js'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Combined integration edge cases', () => {
  it('beforeQuery hook denial prevents metrics from recording a successful query', async () => {
    const metrics: { sql: string; error?: boolean }[] = []

    const sir = new Sirannon({
      driver: testDriver,
      hooks: {
        onBeforeQuery: ctx => {
          if (ctx.sql.includes('forbidden')) {
            throw new HookDeniedError('beforeQuery', 'forbidden query')
          }
        },
      },
      metrics: {
        onQueryComplete: m => metrics.push({ sql: m.sql, error: m.error }),
      },
    })

    const db = await sir.open('main', join(tempDir, 'hook-deny-metrics.db'))
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    await expect(db.query('SELECT forbidden FROM t')).rejects.toThrow(HookDeniedError)

    const forbiddenMetrics = metrics.filter(m => m.sql.includes('forbidden'))
    expect(forbiddenMetrics).toHaveLength(0)

    await sir.shutdown()
  })

  it('afterQuery hook errors do not mask query errors', async () => {
    const db = await Database.create('test', join(tempDir, 'after-mask.db'), testDriver)
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    db.onAfterQuery(() => {
      throw new Error('afterQuery hook error')
    })

    await expect(db.query('INVALID SQL')).rejects.toThrow('syntax error')

    await db.close()
  })

  it('afterQuery hook errors do not mask successful query results', async () => {
    const db = await Database.create('test', join(tempDir, 'after-no-mask.db'), testDriver)
    await db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)')
    await db.execute("INSERT INTO t (val) VALUES ('hello')")

    db.onAfterQuery(() => {
      throw new Error('afterQuery hook error')
    })

    const rows = await db.query<{ val: string }>('SELECT * FROM t')
    expect(rows).toHaveLength(1)
    expect(rows[0].val).toBe('hello')

    await db.close()
  })

  it('multiple databases share global hooks but have independent local hooks', async () => {
    const globalLog: string[] = []
    const localLog1: string[] = []
    const localLog2: string[] = []

    const sir = new Sirannon({ driver: testDriver })
    sir.onBeforeQuery(ctx => {
      globalLog.push(`${ctx.databaseId}:${ctx.sql}`)
    })

    const db1 = await sir.open('db1', join(tempDir, 'multi-hook1.db'))
    const db2 = await sir.open('db2', join(tempDir, 'multi-hook2.db'))

    await db1.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    await db2.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')

    db1.onBeforeQuery(ctx => {
      localLog1.push(ctx.sql)
    })
    db2.onBeforeQuery(ctx => {
      localLog2.push(ctx.sql)
    })

    globalLog.length = 0

    await db1.query('SELECT 1')
    await db2.query('SELECT 2')

    expect(globalLog).toEqual(['db1:SELECT 1', 'db2:SELECT 2'])
    expect(localLog1).toEqual(['SELECT 1'])
    expect(localLog2).toEqual(['SELECT 2'])

    await sir.shutdown()
  })
})
