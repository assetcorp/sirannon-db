import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Sirannon } from '../../src/core/sirannon'
import { createServer } from '../../src/server/server'
import { generateUserRow, microSchemaSqlite } from '../schemas'

const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9876)
const DATA_SIZE = Number(process.env.DATA_SIZE ?? 10_000)

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-e2e-'))
const dbPath = join(tempDir, 'bench.db')

const sirannon = new Sirannon()
const db = sirannon.open('bench', dbPath, {
  readPoolSize: 4,
  walMode: true,
})

db.execute('PRAGMA synchronous = NORMAL')

for (const stmt of microSchemaSqlite
  .split(';')
  .map(s => s.trim())
  .filter(Boolean)) {
  db.execute(stmt)
}

console.log(`Seeding ${DATA_SIZE} rows...`)
const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
console.log(`Seeded ${DATA_SIZE} rows.`)

const server = createServer(sirannon, { host: HOST, port: PORT })
await server.listen()
console.log(`Sirannon app listening on ${HOST}:${PORT}`)

process.on('SIGTERM', async () => {
  await server.close()
  sirannon.shutdown()
  process.exit(0)
})

process.on('SIGINT', async () => {
  await server.close()
  sirannon.shutdown()
  process.exit(0)
})
