import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { Sirannon } from '../../sirannon.js'
import type { ChangeEvent } from '../../types.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('End-to-end smoke test', () => {
  it('open -> create table -> watch -> subscribe -> insert -> verify event -> query -> backup -> close', async () => {
    const queryLog: string[] = []
    const connectionLog: string[] = []

    const sir = new Sirannon({
      driver: testDriver,
      hooks: {
        onBeforeQuery: ctx => {
          queryLog.push(ctx.sql)
        },
      },
      metrics: {
        onConnectionOpen: m => connectionLog.push(`open:${m.databaseId}`),
        onConnectionClose: m => connectionLog.push(`close:${m.databaseId}`),
      },
    })

    const db = await sir.open('e2e', join(tempDir, 'e2e.db'))
    expect(connectionLog).toContain('open:e2e')

    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    await db.watch('users')

    const events: ChangeEvent[] = []
    const sub = db.on('users').subscribe(event => events.push(event))

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events[0].type).toBe('insert')
    expect(events[0].row).toEqual({ id: 1, name: 'Alice', age: 30 })

    sub.unsubscribe()

    const rows = await db.query<{ id: number; name: string; age: number }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')

    const backupPath = join(tempDir, 'e2e-backup.db')
    await db.backup(backupPath)

    const verify = await Database.create('verify', backupPath, testDriver)
    const backupRows = await verify.query<{ name: string }>('SELECT * FROM users')
    expect(backupRows).toHaveLength(1)
    expect(backupRows[0].name).toBe('Alice')
    await verify.close()

    expect(queryLog.length).toBeGreaterThan(0)

    await sir.shutdown()
    expect(connectionLog).toContain('close:e2e')
  })
})
