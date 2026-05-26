import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Database } from '../../database.js'
import { ReadOnlyError } from '../../errors.js'
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

describe('CDC integration via Database', () => {
  it('watch -> insert -> on -> subscribe -> verify event fires', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

    await db.watch('users')
    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('insert')
    expect(received[0].table).toBe('users')
    expect(received[0].row).toEqual({ id: 1, name: 'Alice', age: 30 })
    expect(received[0].oldRow).toBeUndefined()

    await db.close()
  })

  it('captures UPDATE events with old and new row data', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-update.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await new Promise(resolve => setTimeout(resolve, 120))
    received.length = 0

    await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('update')
    expect(received[0].row.age).toBe(31)
    expect(received[0].oldRow?.age).toBe(30)

    await db.close()
  })

  it('captures DELETE events with old row data', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-delete.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await new Promise(resolve => setTimeout(resolve, 120))
    received.length = 0

    await db.execute("DELETE FROM users WHERE name = 'Alice'")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].type).toBe('delete')
    expect(received[0].oldRow?.name).toBe('Alice')

    await db.close()
  })

  it('filters subscriptions to matching events only', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-filter.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const aliceOnly: ChangeEvent[] = []
    const allEvents: ChangeEvent[] = []

    db.on('users')
      .filter({ name: 'Alice' })
      .subscribe(event => aliceOnly.push(event))
    db.on('users').subscribe(event => allEvents.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(allEvents).toHaveLength(3)
    expect(aliceOnly).toHaveLength(2)

    await db.close()
  })

  it('unwatch stops capturing new changes', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-unwatch.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await new Promise(resolve => setTimeout(resolve, 120))
    expect(received).toHaveLength(1)

    await db.unwatch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received).toHaveLength(1)

    await db.close()
  })

  it('throws ReadOnlyError when watching on a read-only database', async () => {
    const dbPath = join(tempDir, 'ro-cdc.db')
    const setup = await Database.create('setup', dbPath, testDriver)
    await setup.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await setup.close()

    const db = await Database.create('test', dbPath, testDriver, { readOnly: true })
    await expect(db.watch('users')).rejects.toThrow(ReadOnlyError)
    await db.close()
  })

  it('supports creating subscriptions before watching', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-sub-first.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.name).toBe('Alice')

    await db.close()
  })

  it('respects custom CDC poll interval', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-interval.db'), testDriver, { cdcPollInterval: 20 })
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')

    const received: ChangeEvent[] = []
    db.on('users').subscribe(event => received.push(event))

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    await new Promise(resolve => setTimeout(resolve, 80))

    expect(received.length).toBeGreaterThanOrEqual(1)
    await db.close()
  })

  it('cleans up CDC polling on close', async () => {
    const db = await Database.create('test', join(tempDir, 'cdc-close.db'), testDriver)
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    db.on('users').subscribe(() => {})

    await db.close()

    await new Promise(resolve => setTimeout(resolve, 120))
    expect(db.closed).toBe(true)
  })
})

describe('CDC through Sirannon', () => {
  it('watch and subscribe work on databases opened through Sirannon', async () => {
    const sir = new Sirannon({ driver: testDriver })
    const db = await sir.open('main', join(tempDir, 'sir-cdc.db'))
    await db.execute('CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)')
    await db.watch('messages')

    const received: ChangeEvent[] = []
    db.on('messages').subscribe(event => received.push(event))

    await db.execute("INSERT INTO messages (body) VALUES ('hello world')")

    await new Promise(resolve => setTimeout(resolve, 120))

    expect(received.length).toBeGreaterThanOrEqual(1)
    expect(received[0].row.body).toBe('hello world')

    await sir.shutdown()
  })
})
