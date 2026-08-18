import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { assembleFromDestination } from '../../backup/assemble.js'
import { Database } from '../../database.js'
import { Sirannon } from '../../sirannon.js'
import { memoryDestination } from '../backup/memory-destination.js'
import { testDriver } from '../helpers/test-driver.js'

async function seedPages(db: Database, rows: number): Promise<void> {
  await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
  await db.bulkLoad(
    'INSERT INTO users (name) VALUES (?)',
    Array.from({ length: rows }, (_, index) => [`user-${index}`.padEnd(200, 'x')]),
  )
}

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

  it('keeps taking writes for the whole copy and finishes without a restart', async () => {
    const db = await Database.create('busy', join(tempDir, 'busy-source.db'), testDriver)
    await seedPages(db, 20000)

    let writesDuringCopy = 0
    let copying = true
    const writer = (async () => {
      while (copying) {
        await new Promise(resolve => setImmediate(resolve))
        if (!copying) break
        await db.execute("INSERT INTO users (name) VALUES ('written-during-copy')")
        writesDuringCopy++
      }
    })()

    let stepsSeen = 0
    const report = await db.backupTo({
      destination: memoryDestination(),
      pagesPerStep: 8,
      stagingDir: tempDir,
      onProgress: progress => {
        if (progress.phase === 'copy') stepsSeen++
      },
    })
    copying = false
    await writer

    expect(report.restarts).toBe(0)
    expect(stepsSeen).toBeGreaterThan(10)
    expect(writesDuringCopy).toBeGreaterThan(1)
    expect(await db.queryOne<{ n: number }>('SELECT count(*) AS n FROM users')).toEqual({
      n: 20000 + writesDuringCopy,
    })
    await db.close()
  })

  it('copies to a caller-supplied destination and assembles back into an openable database', async () => {
    const db = await Database.create('dest', join(tempDir, 'dest-source.db'), testDriver)
    await db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)')
    await db.execute("INSERT INTO products (title) VALUES ('Widget')")

    const destination = memoryDestination()
    const report = await db.backupTo({ destination, name: 'products.db', stagingDir: tempDir })
    await db.close()

    expect(destination.names()).toEqual(['products.db'])
    expect(report.bytesWritten).toBeGreaterThan(0)

    const assembledPath = join(tempDir, 'products-copy.db')
    const assembled = await assembleFromDestination(destination, report, assembledPath)
    expect(assembled.bytesWritten).toBe(report.bytesWritten)

    const verify = await Database.create('verify-dest', assembledPath, testDriver, { walMode: false })
    expect(await verify.query<{ title: string }>('SELECT title FROM products')).toEqual([{ title: 'Widget' }])
    await verify.close()
  })

  it('states which backup operations the runtime supports', async () => {
    const db = await Database.create('caps', join(tempDir, 'caps.db'), testDriver)

    expect(db.backupCapabilities()).toEqual({
      fullCopy: true,
      streamedCopy: false,
      stagedCopy: true,
      localDiskRequired: 'equal-to-backup',
      schedule: true,
    })
    await db.close()
  })
})
