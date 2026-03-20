import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'

/*
 * PORT defaults to 9876; override with PORT.
 * HOST defaults to loopback (127.0.0.1). Set HOST (for example 0.0.0.0) only when you
 * intentionally want the process to accept connections from other interfaces.
 * createServer is called with cors: true and no onRequest auth; exposing the server
 * beyond localhost requires both that explicit HOST override and your own hardening.
 */
const HOST = process.env.HOST ?? '127.0.0.1'
const PORT = Number(process.env.PORT ?? 9876)

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-client-example-'))

const driver = betterSqlite3()
const sirannon = new Sirannon({
  driver,
  hooks: {
    onDatabaseOpen: [ctx => console.log(`[hook] Database opened: ${ctx.databaseId}`)],
    onDatabaseClose: [ctx => console.log(`[hook] Database closed: ${ctx.databaseId}`)],
  },
})

const db = await sirannon.open('main', join(tempDir, 'client-example.db'), {
  readPoolSize: 4,
  walMode: true,
  cdcPollInterval: 50,
})

await db.execute(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    author TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  )
`)

await db.execute(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active'
  )
`)

await db.watch('messages')
await db.watch('users')

await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com'])
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Bob', 'bob@example.com'])
await db.execute('INSERT INTO messages (author, content) VALUES (?, ?)', ['Alice', 'Welcome to Sirannon DB'])
await db.execute('INSERT INTO messages (author, content) VALUES (?, ?)', ['Bob', 'This is a client-server example'])

console.log('Seed data inserted.')

const server = createServer(sirannon, {
  host: HOST,
  port: PORT,
  cors: true,
})

await server.listen()
console.log(`Sirannon server listening on ${HOST}:${PORT}`)
console.log(`  HTTP: http://localhost:${PORT}`)
console.log(`  WS:   ws://localhost:${PORT}`)

let isShuttingDown = false

const shutdown = async () => {
  if (isShuttingDown) {
    return
  }

  isShuttingDown = true
  let exitCode = 0

  try {
    await server.close()
  } catch (error) {
    exitCode = 1
    console.error('Failed to close server during shutdown.', error)
  }

  try {
    await sirannon.shutdown()
  } catch (error) {
    exitCode = 1
    console.error('Failed to shut down Sirannon during shutdown.', error)
  }

  try {
    rmSync(tempDir, { recursive: true, force: true })
  } catch (error) {
    exitCode = 1
    console.error('Failed to remove temporary directory during shutdown.', error)
  }

  process.exit(exitCode)
}

process.once('SIGTERM', shutdown)
process.once('SIGINT', shutdown)
