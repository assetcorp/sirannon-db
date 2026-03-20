import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Sirannon } from '../../src/core/sirannon'
import { createServer } from '../../src/server/server'
import { loadBenchDriver } from '../config'
import { generateUserRow, microSchemaSqlite } from '../schemas'

const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9876)
const DATA_SIZE = Number(process.env.DATA_SIZE ?? 10_000)

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-e2e-'))
const dbPath = join(tempDir, 'bench.db')

let sirannon: Sirannon | undefined
let server: ReturnType<typeof createServer> | undefined
let cleanupPromise: Promise<number> | undefined

const cleanup = async (): Promise<number> => {
  if (cleanupPromise) {
    return cleanupPromise
  }

  cleanupPromise = (async () => {
    let exitCode = 0

    if (server) {
      try {
        await server.close()
      } catch (error) {
        exitCode = 1
        console.error('Failed to close benchmark server during cleanup.', error)
      }
    }

    if (sirannon) {
      try {
        await sirannon.shutdown()
      } catch (error) {
        exitCode = 1
        console.error('Failed to shut down Sirannon during cleanup.', error)
      }
    }

    try {
      rmSync(tempDir, { recursive: true, force: true })
    } catch (error) {
      exitCode = 1
      console.error('Failed to remove benchmark temp directory during cleanup.', error)
    }

    return exitCode
  })()

  return cleanupPromise
}

process.once('SIGTERM', () => {
  void cleanup().then(exitCode => {
    process.exit(exitCode)
  })
})

process.once('SIGINT', () => {
  void cleanup().then(exitCode => {
    process.exit(exitCode)
  })
})

try {
  const driver = await loadBenchDriver()
  sirannon = new Sirannon({ driver })

  const db = await sirannon.open('bench', dbPath, {
    readPoolSize: 4,
    walMode: true,
  })

  await db.execute('PRAGMA synchronous = NORMAL')

  for (const stmt of microSchemaSqlite
    .split(';')
    .map(s => s.trim())
    .filter(Boolean)) {
    await db.execute(stmt)
  }

  console.log(`Seeding ${DATA_SIZE} rows...`)
  const rows = Array.from({ length: DATA_SIZE }, (_, i) => generateUserRow(i + 1))
  await db.executeBatch('INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)', rows)
  console.log(`Seeded ${DATA_SIZE} rows.`)

  server = createServer(sirannon, { host: HOST, port: PORT })
  await server.listen()
  console.log(`Sirannon app listening on ${HOST}:${PORT}`)
} catch (error) {
  console.error('Failed to start Sirannon benchmark app.', error)
  const cleanupExitCode = await cleanup()
  process.exit(cleanupExitCode === 0 ? 1 : cleanupExitCode)
}
