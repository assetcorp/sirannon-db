import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'
import { DATABASE_ID } from './lib/demo-config'
import { type Operator, operations, SEED_PRODUCTS } from './operations'
import { createOperatorAuthenticator } from './operator-identity'

const DEFAULT_PORT = 9876
const DEFAULT_APP_PORT = '3000'
const HOST = process.env.HOST ?? '127.0.0.1'
const PORT = parsePort(process.env.SIRANNON_PORT)
const APP_ORIGINS = (process.env.APP_ORIGIN ?? `http://localhost:${process.env.PORT ?? DEFAULT_APP_PORT}`)
  .split(',')
  .map(origin => origin.trim())
  .filter(origin => origin.length > 0)

function parsePort(value: string | undefined): number {
  if (value === undefined || value.trim().length === 0) {
    return DEFAULT_PORT
  }

  const port = Number(value)
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('SIRANNON_PORT must be an integer TCP port between 1 and 65535')
  }

  return port
}

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-inventory-'))

const driver = betterSqlite3()
const sirannon = new Sirannon({
  driver,
  hooks: {
    onDatabaseOpen: [ctx => console.log(`[hook] Database opened: ${ctx.databaseId}`)],
    onDatabaseClose: [ctx => console.log(`[hook] Database closed: ${ctx.databaseId}`)],
  },
})

const db = await sirannon.open(DATABASE_ID, join(tempDir, 'inventory.db'), {
  readPoolSize: 4,
  walMode: true,
  cdcPollInterval: 50,
})

await db.execute(`
  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0
  )
`)

await db.execute(`
  CREATE TABLE IF NOT EXISTS activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    action TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    operator TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  )
`)

for (const product of SEED_PRODUCTS) {
  await db.execute('INSERT INTO products (name, price, stock) VALUES (?, ?, ?)', [
    product.name,
    product.price,
    product.stock,
  ])
}

console.log('Seed data inserted.')

const server = createServer<Operator>(sirannon, {
  host: HOST,
  port: PORT,
  cors: {
    origin: APP_ORIGINS,
    methods: ['GET', 'POST', 'OPTIONS'],
    headers: ['Content-Type', 'Authorization'],
  },
  operations,
  authenticate: createOperatorAuthenticator(APP_ORIGINS, DATABASE_ID),
})

await server.listen()
console.log(`Sirannon data server listening on ${HOST}:${PORT}`)
console.log(`  HTTP: http://localhost:${PORT}`)
console.log(`  WS:   ws://localhost:${PORT}`)
console.log('  SQL over the network is off; every call names a registered operation.')

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
