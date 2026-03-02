import { generateUserRow, microSchemaPostgres } from '../schemas'
import { createPostgresAppServer } from './postgres-server'

const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9877)
const DATA_SIZE = Number(process.env.DATA_SIZE ?? 10_000)

const poolConfig = {
  host: process.env.PGHOST ?? '127.0.0.1',
  port: Number(process.env.PGPORT ?? 5432),
  user: process.env.PGUSER ?? 'benchmark',
  password: process.env.PGPASSWORD ?? 'benchmark',
  database: process.env.PGDATABASE ?? 'benchmark',
  max: Number(process.env.PG_POOL_SIZE ?? 20),
}

const { pool, close } = await createPostgresAppServer({ host: HOST, port: PORT, poolConfig })

await pool.query('DROP TABLE IF EXISTS users CASCADE')
for (const stmt of microSchemaPostgres
  .split(';')
  .map(s => s.trim())
  .filter(Boolean)) {
  await pool.query(stmt)
}

console.log(`Seeding ${DATA_SIZE} rows...`)
const CHUNK_SIZE = 500
const colCount = 5
const allRows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))

for (let offset = 0; offset < allRows.length; offset += CHUNK_SIZE) {
  const chunk = allRows.slice(offset, offset + CHUNK_SIZE)
  const values: unknown[] = []
  const placeholders: string[] = []

  for (let i = 0; i < chunk.length; i++) {
    const row = chunk[i]
    const rowPh: string[] = []
    for (let j = 0; j < colCount; j++) {
      values.push(row[j])
      rowPh.push(`$${i * colCount + j + 1}`)
    }
    placeholders.push(`(${rowPh.join(', ')})`)
  }

  await pool.query(`INSERT INTO users (id, name, email, age, bio) VALUES ${placeholders.join(', ')}`, values)
}

console.log(`Seeded ${DATA_SIZE} rows.`)
console.log(`Postgres app listening on ${HOST}:${PORT}`)

process.on('SIGTERM', async () => {
  close()
  await pool.end()
  process.exit(0)
})

process.on('SIGINT', async () => {
  close()
  await pool.end()
  process.exit(0)
})
