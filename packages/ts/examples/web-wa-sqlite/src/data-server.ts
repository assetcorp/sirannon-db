import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'
import {
  DATABASE_ID,
  migrations,
  SEED_INSERT_SQL,
  SEED_UPDATED_AT,
  SEED_WORK_ORDERS,
  WORK_ORDERS_TABLE,
} from './schema'

const DEFAULT_PORT = 9876
const DEFAULT_APP_ORIGIN = 'http://localhost:5173'
const HOST = process.env.HOST ?? '127.0.0.1'

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

const PORT = parsePort(process.env.SIRANNON_PORT)
const APP_ORIGINS = (process.env.APP_ORIGIN ?? DEFAULT_APP_ORIGIN)
  .split(',')
  .map(origin => origin.trim())
  .filter(origin => origin.length > 0)

const dataDir = fileURLToPath(new URL('../data/', import.meta.url))
mkdirSync(dataDir, { recursive: true })

const sirannon = new Sirannon({ driver: betterSqlite3(), migrations: [...migrations] })
const db = await sirannon.open(DATABASE_ID, `${dataDir}${DATABASE_ID}.db`, {
  readPoolSize: 4,
  walMode: true,
})

await db.watch(WORK_ORDERS_TABLE)

const existing = await db.queryOne<{ count: number }>(`SELECT count(*) AS count FROM ${WORK_ORDERS_TABLE}`)
if ((existing?.count ?? 0) === 0) {
  for (const order of SEED_WORK_ORDERS) {
    await db.execute(SEED_INSERT_SQL, [order.id, order.site, order.task, SEED_UPDATED_AT])
  }
  console.log(`Seeded ${SEED_WORK_ORDERS.length} work orders.`)
}

const server = createServer(sirannon, {
  host: HOST,
  port: PORT,
  cors: { origin: APP_ORIGINS },
})

await server.listen()

console.log(`Field service server listening on http://${HOST}:${PORT}`)
console.log(`Database '${DATABASE_ID}' stored at ${dataDir}${DATABASE_ID}.db`)
console.log(`Accepting device sync from ${APP_ORIGINS.join(', ')}`)
console.log('SQL over the network is refused; devices reach this database through the sync routes only.')

let shuttingDown = false

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return
  shuttingDown = true
  console.log(`\nReceived ${signal}, closing the server...`)
  await server.close()
  await sirannon.shutdown()
  process.exit(0)
}

process.on('SIGINT', () => {
  void shutdown('SIGINT')
})
process.on('SIGTERM', () => {
  void shutdown('SIGTERM')
})
