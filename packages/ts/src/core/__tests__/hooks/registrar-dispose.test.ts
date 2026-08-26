import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import type { Database } from '../../database.js'
import { Sirannon } from '../../sirannon.js'

let tempDir: string
let sirannon: Sirannon
let db: Database

const driver = betterSqlite3()

beforeEach(async () => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-hook-dispose-'))
  sirannon = new Sirannon({ driver })
  db = await sirannon.open('mydb', join(tempDir, 'hooks.db'))
  await db.execute('CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)')
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('hook registrars', () => {
  it('returns a function that removes a registry hook', async () => {
    const hook = vi.fn()
    const dispose = sirannon.onBeforeQuery(hook)

    await db.query('SELECT 1')
    expect(hook).toHaveBeenCalledTimes(1)

    dispose()
    await db.query('SELECT 1')
    expect(hook).toHaveBeenCalledTimes(1)
  })

  it('returns a function that removes a database hook', async () => {
    const hook = vi.fn()
    const dispose = db.onAfterQuery(hook)

    await db.query('SELECT 1')
    expect(hook).toHaveBeenCalledTimes(1)

    dispose()
    dispose()
    await db.query('SELECT 1')
    expect(hook).toHaveBeenCalledTimes(1)
  })

  it('removes only the hook it was returned for', async () => {
    const kept = vi.fn()
    const removed = vi.fn()
    sirannon.onBeforeQuery(kept)
    const dispose = sirannon.onBeforeQuery(removed)

    dispose()
    await db.query('SELECT 1')

    expect(kept).toHaveBeenCalledTimes(1)
    expect(removed).not.toHaveBeenCalled()
  })

  it('removes a connection hook so a later open runs without it', async () => {
    const hook = vi.fn()
    const dispose = sirannon.onDatabaseOpen(hook)

    await sirannon.open('second', join(tempDir, 'second.db'))
    expect(hook).toHaveBeenCalledTimes(1)

    dispose()
    await sirannon.open('third', join(tempDir, 'third.db'))
    expect(hook).toHaveBeenCalledTimes(1)
  })
})
