import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../../core/sirannon.js'
import { betterSqlite3 } from '../../../drivers/better-sqlite3/index.js'
import { needsResync } from '../../ws-cdc-resume.js'
import { createWSHandler } from '../../ws-handler.js'
import { createMockConnection, parseMessages } from '../helpers.js'

let tempDir: string
let sirannon: Sirannon

const driver = betterSqlite3()

async function flushAsync(predicate: () => boolean, timeout = 2000): Promise<void> {
  const start = Date.now()
  while (!predicate()) {
    if (Date.now() - start >= timeout) {
      throw new Error(`flushAsync timed out after ${timeout}ms: condition never became true`)
    }
    await new Promise(r => setTimeout(r, 5))
  }
}

function changeNames(conn: ReturnType<typeof createMockConnection>): string[] {
  return parseMessages(conn)
    .filter(m => m.type === 'change')
    .map(m => ((m.event as Record<string, unknown>).row as Record<string, string>).name)
}

async function maxSeq(db: Awaited<ReturnType<Sirannon['open']>>): Promise<string> {
  const rows = await db.query('SELECT MAX(seq) AS s FROM _sirannon_changes')
  return String((rows[0] as Record<string, unknown>).s)
}

beforeEach(() => {
  tempDir = mkdtempSync(join(tmpdir(), 'sirannon-ws-resume-'))
  sirannon = new Sirannon({ driver })
})

afterEach(async () => {
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
})

describe('WSHandler resume-from-seq', () => {
  it('replays retained changes after the cursor, then streams live in order', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'replay.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Carol')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users', sinceSeq: '0' }))

    await flushAsync(() => changeNames(conn).length >= 3)
    expect(changeNames(conn)).toEqual(['Alice', 'Bob', 'Carol'])

    await db.execute("INSERT INTO users (name) VALUES ('Dave')")
    await flushAsync(() => changeNames(conn).length >= 4)
    expect(changeNames(conn)).toEqual(['Alice', 'Bob', 'Carol', 'Dave'])
    await handler.close()
  })

  it('replays only changes newer than a mid-stream cursor', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'mid.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    const cursor = await maxSeq(db)
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Carol')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users', sinceSeq: cursor }))

    await flushAsync(() => changeNames(conn).length >= 2)
    expect(changeNames(conn)).toEqual(['Bob', 'Carol'])
    await handler.close()
  })

  it('signals resync when the cursor precedes retained history', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'resync.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Carol')")
    const tail = await maxSeq(db)
    await db.execute('DELETE FROM _sirannon_changes WHERE seq < ?', [Number(tail)])

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users', sinceSeq: '0' }))

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))
    const subscribed = parseMessages(conn).find(m => m.type === 'subscribed') as Record<string, unknown>
    expect(subscribed.resync).toBe(true)
    expect(changeNames(conn)).toHaveLength(0)
    await handler.close()
  })

  it('rejects a malformed cursor', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'bad.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users', sinceSeq: 'abc' }))

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'error'))
    const err = parseMessages(conn).find(m => m.type === 'error') as Record<string, unknown>
    expect((err.error as Record<string, unknown>).code).toBe('INVALID_MESSAGE')
    await handler.close()
  })

  it('returns a baseline seq in the subscribed confirmation so an idle subscriber can resume', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'baseline.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users' }))

    await flushAsync(() => parseMessages(conn).some(m => m.type === 'subscribed'))
    const subscribed = parseMessages(conn).find(m => m.type === 'subscribed') as Record<string, unknown>
    expect(typeof subscribed.seq).toBe('string')
    await handler.close()
  })

  it('signals resync without closing the connection when replay hits a corrupt row', async () => {
    const handler = createWSHandler(sirannon)
    const db = await sirannon.open('mydb', join(tempDir, 'poison.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')
    await db.watch('users')
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("UPDATE _sirannon_changes SET new_data = '{' WHERE seq = (SELECT MIN(seq) FROM _sirannon_changes)")

    const conn = createMockConnection()
    await handler.handleOpen(conn, 'mydb')
    handler.handleMessage(conn, JSON.stringify({ id: 'sub-1', type: 'subscribe', table: 'users', sinceSeq: '0' }))

    await flushAsync(() =>
      parseMessages(conn)
        .filter(m => m.type === 'subscribed')
        .some(m => (m as Record<string, unknown>).resync === true),
    )
    expect(conn.closed).toBe(false)
    await handler.close()
  })
})

describe('needsResync', () => {
  it('is satisfiable when the cursor sits at or past the boundary', () => {
    expect(needsResync(10n, 3n, 10n)).toBe(false)
    expect(needsResync(12n, 3n, 10n)).toBe(false)
  })

  it('is satisfiable when retained history reaches the cursor', () => {
    expect(needsResync(5n, 6n, 10n)).toBe(false)
    expect(needsResync(5n, 5n, 10n)).toBe(false)
  })

  it('requires resync when history below the cursor was pruned', () => {
    expect(needsResync(5n, 8n, 10n)).toBe(true)
  })

  it('requires resync when nothing is retained but events preceded the boundary', () => {
    expect(needsResync(5n, null, 10n)).toBe(true)
  })
})
