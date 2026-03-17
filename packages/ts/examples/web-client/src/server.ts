import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'

const HOST = process.env.HOST ?? '0.0.0.0'
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

process.on('SIGTERM', async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
  process.exit(0)
})

process.on('SIGINT', async () => {
  await server.close()
  await sirannon.shutdown()
  rmSync(tempDir, { recursive: true, force: true })
  process.exit(0)
})
