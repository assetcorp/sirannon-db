import { generateUserRow, microSchemaPostgres, microSchemaSqlite } from '../schemas'

interface SeedOptions {
  sirannonUrl: string
  postgresUrl: string
  dataSize: number
}

async function postJson(url: string, body: unknown): Promise<unknown> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`POST ${url} failed: ${res.status} ${text}`)
  }
  return res.json()
}

async function seedViaHttp(baseUrl: string, dbId: string, schemaSql: string, dataSize: number) {
  for (const stmt of schemaSql
    .split(';')
    .map(s => s.trim())
    .filter(Boolean)) {
    await postJson(`${baseUrl}/db/${dbId}/execute`, { sql: stmt })
  }

  const BATCH = 100
  for (let offset = 0; offset < dataSize; offset += BATCH) {
    const end = Math.min(offset + BATCH, dataSize)
    const statements = []
    for (let i = offset; i < end; i++) {
      const row = generateUserRow(i + 1)
      statements.push({
        sql: 'INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)',
        params: row,
      })
    }
    await postJson(`${baseUrl}/db/${dbId}/transaction`, { statements })
  }
}

export async function seedBothServers(options: SeedOptions) {
  console.log(`Seeding Sirannon (${options.dataSize} rows)...`)
  await seedViaHttp(options.sirannonUrl, 'bench', microSchemaSqlite, options.dataSize)

  console.log(`Seeding Postgres (${options.dataSize} rows)...`)
  await seedViaHttp(options.postgresUrl, 'bench', microSchemaPostgres, options.dataSize)

  console.log('Seeding complete.')
}
