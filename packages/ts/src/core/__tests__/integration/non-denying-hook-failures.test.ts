import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string
const unhandled: unknown[] = []
const recordUnhandled = (reason: unknown): void => {
  unhandled.push(reason)
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-hook-failures-'))
  unhandled.length = 0
  process.on('unhandledRejection', recordUnhandled)
})

afterEach(() => {
  process.off('unhandledRejection', recordUnhandled)
  rmSync(tempDir, { recursive: true, force: true })
})

async function settle(): Promise<void> {
  await new Promise(resolve => setImmediate(resolve))
  await new Promise(resolve => setImmediate(resolve))
}

describe('a failing hook on an event that cannot refuse the operation', () => {
  it('leaves the after-query hooks registered after it running', async () => {
    const seen: string[] = []
    const sirannon = new Sirannon({ driver: testDriver })
    sirannon.onAfterQuery(() => {
      throw new Error('audit sink offline')
    })
    sirannon.onAfterQuery(ctx => {
      seen.push(ctx.sql)
    })
    const db = await sirannon.open('main', join(tempDir, 'after.db'))

    await db.query('SELECT 1')

    expect(seen).toEqual(['SELECT 1'])
    await sirannon.shutdown()
  })

  it('leaves no unhandled rejection behind when an async after-query hook rejects', async () => {
    const seen: string[] = []
    const sirannon = new Sirannon({ driver: testDriver })
    sirannon.onAfterQuery(async () => {
      throw new Error('async audit sink offline')
    })
    sirannon.onAfterQuery(ctx => {
      seen.push(ctx.sql)
    })
    const db = await sirannon.open('main', join(tempDir, 'async-after.db'))

    await db.query('SELECT 1')
    await settle()

    expect(unhandled).toEqual([])
    expect(seen).toEqual(['SELECT 1'])
    await sirannon.shutdown()
  })

  it('leaves the database-open hooks registered after an async one running', async () => {
    const opened: string[] = []
    const sirannon = new Sirannon({ driver: testDriver })
    sirannon.onDatabaseOpen(async () => {
      throw new Error('async open listener failed')
    })
    sirannon.onDatabaseOpen(ctx => {
      opened.push(ctx.databaseId)
    })

    await sirannon.open('main', join(tempDir, 'open.db'))
    await settle()

    expect(unhandled).toEqual([])
    expect(opened).toEqual(['main'])
    await sirannon.shutdown()
  })

  it('still fails the statement when an async before-query hook rejects, with no unhandled rejection', async () => {
    const sirannon = new Sirannon({ driver: testDriver })
    sirannon.onBeforeQuery(async () => {
      throw new Error('async policy check failed')
    })
    const db = await sirannon.open('main', join(tempDir, 'before.db'))

    await expect(db.query('SELECT 1')).rejects.toThrow("Hook for 'beforeQuery' returned a Promise")
    await settle()

    expect(unhandled).toEqual([])
    await sirannon.shutdown()
  })
})
