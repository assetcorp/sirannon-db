import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Sirannon } from '../../core/sirannon.js'
import { betterSqlite3 } from '../../drivers/better-sqlite3/index.js'
import { createMockConnection, wait } from '../../server/__tests__/helpers.js'
import type { WSHandler } from '../../server/ws-handler.js'
import { createWSHandler } from '../../server/ws-handler.js'

const driver = betterSqlite3()

describe('WebSocket subscription integration', () => {
  let tempDir: string
  let sirannon: Sirannon
  let wsHandler: WSHandler

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), 'sirannon-sub-'))
    sirannon = new Sirannon({ driver })
  })

  afterEach(async () => {
    await wsHandler?.close()
    await sirannon.shutdown()
    rmSync(tempDir, { recursive: true, force: true })
  })

  it('subscribes and receives insert events via mock WS handler', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'sub.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon, { acceptSql: true })
    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    const subMsg = JSON.parse(conn.messages[conn.messages.length - 1])
    expect(subMsg.type).toBe('subscribed')

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const changeMessages = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change')

    expect(changeMessages).toHaveLength(1)
    expect(changeMessages[0].event.type).toBe('insert')
    expect(changeMessages[0].event.row.name).toBe('Alice')
  })

  it('receives filtered events', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'filter.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon, { acceptSql: true })

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
        filter: { name: 'Alice' },
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const changeMessages = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change')

    expect(changeMessages).toHaveLength(1)
    expect(changeMessages[0].event.row.name).toBe('Alice')
  })

  it('stops receiving events after unsubscribe', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'unsub.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon, { acceptSql: true })

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    let changeCount = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change').length
    expect(changeCount).toBe(1)

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'unsubscribe',
      }),
    )

    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await wait(200)

    changeCount = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change').length
    expect(changeCount).toBe(1)
  })

  it('receives update and delete events', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'upddel.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)')

    wsHandler = createWSHandler(sirannon, { acceptSql: true })

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
    await db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
    await db.execute("DELETE FROM users WHERE name = 'Alice'")
    await wait(200)

    const events = conn.messages
      .map(m => JSON.parse(m))
      .filter(m => m.type === 'change')
      .map(m => m.event)

    expect(events).toHaveLength(3)
    expect(events[0].type).toBe('insert')
    expect(events[1].type).toBe('update')
    expect(events[1].oldRow.age).toBe(30)
    expect(events[1].row.age).toBe(31)
    expect(events[2].type).toBe('delete')
  })

  it('multiple subscriptions on the same table', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'multi.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon, { acceptSql: true })

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-a',
        type: 'subscribe',
        table: 'users',
        filter: { name: 'Alice' },
      }),
    )
    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-b',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await db.execute("INSERT INTO users (name) VALUES ('Bob')")
    await wait(200)

    const changes = conn.messages.map(m => JSON.parse(m)).filter(m => m.type === 'change')

    const subAChanges = changes.filter(m => m.id === 'sub-a')
    const subBChanges = changes.filter(m => m.id === 'sub-b')
    expect(subAChanges).toHaveLength(1)
    expect(subBChanges).toHaveLength(2)
  })

  it('change events include seq as string and timestamp', async () => {
    const db = await sirannon.open('mydb', join(tempDir, 'seq.db'))
    await db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)')

    wsHandler = createWSHandler(sirannon, { acceptSql: true })

    const conn = createMockConnection()
    await wsHandler.handleOpen(conn, 'mydb')

    wsHandler.handleMessage(
      conn,
      JSON.stringify({
        id: 'sub-1',
        type: 'subscribe',
        table: 'users',
      }),
    )

    await wait(100)

    await db.execute("INSERT INTO users (name) VALUES ('Alice')")
    await wait(200)

    const change = conn.messages.map(m => JSON.parse(m)).find(m => m.type === 'change')

    expect(typeof change.event.seq).toBe('string')
    expect(typeof change.event.timestamp).toBe('number')
    expect(Number(change.event.seq)).toBeGreaterThan(0)
  })
})
