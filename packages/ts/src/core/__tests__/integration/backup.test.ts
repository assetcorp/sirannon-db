import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { Sirannon } from '../../sirannon.js'
import { testDriver } from '../helpers/test-driver.js'

let tempDir: string

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-integration-'))
})

afterEach(() => {
  rmSync(tempDir, { recursive: true, force: true })
})

describe('Backup integration via Database', () => {
  it('creates a one-shot backup via backup()', async () => {
    const db = await Database.create('test', join(tempDir, 'backup-source.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    const backupPath = join(tempDir, 'backup-copy.db')
    await db.backup(backupPath)
    await db.close()

    const verify = await Database.create('verify', backupPath, testDriver)
    const rows = await verify.query<{ name: string }>('SELECT * FROM users')
    expect(rows).toHaveLength(1)
    expect(rows[0].name).toBe('Alice')
    await verify.close()
  })

  it('throws when backing up a read-only database (no writer)', async () => {
    const dbPath = join(tempDir, 'ro-backup.db')
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    await setup.close()

    const db = await Database.create('test', dbPath, testDriver, { readOnly: true })
    await expect(db.backup(join(tempDir, 'fail.db'))).rejects.toThrow()
    await db.close()
  })

  it('integrates backup through Sirannon', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'sir-backup.db'))
    await db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)')
    await db.execute("INSERT INTO products (title) VALUES ('Widget')")

    const backupPath = join(tempDir, 'sir-backup-copy.db')
    await db.backup(backupPath)

    const verify = await Database.create('verify', backupPath, testDriver)
    const rows = await verify.query<{ title: string }>('SELECT * FROM products')
    expect(rows).toHaveLength(1)
    expect(rows[0].title).toBe('Widget')
    await verify.close()

    await sir.shutdown()
  })
})
