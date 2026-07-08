// Boot the real Sirannon HTTP server for the benchmark. The harness drives this exactly as an
// application would: JSON requests to /db/<id>/query, /execute, and /transaction over loopback.
// The database starts empty; the harness applies each workload's schema and seed over the wire.
//
// Durability is applied to the writer connection from BENCH_DURABILITY: FULL fsyncs every commit
// to match PostgreSQL synchronous_commit=on; NORMAL defers the fsync to match synchronous_commit
// =off. Both run in WAL mode.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Sirannon } from '../../packages/ts/dist/core/index.mjs'
import { betterSqlite3 } from '../../packages/ts/dist/driver/better-sqlite3.mjs'
import { createServer } from '../../packages/ts/dist/server/index.mjs'

const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9876)
const DATABASE_ID = process.env.BENCH_SIRANNON_DB ?? 'bench'
const DURABILITY = process.env.BENCH_DURABILITY === 'full' ? 'full' : 'matched'

const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-bench-'))
const dbPath = join(tempDir, 'bench.db')

let sirannon
let server
let cleaningUp

const cleanup = async () => {
  if (cleaningUp) return cleaningUp
  cleaningUp = (async () => {
    let exitCode = 0
    if (server) {
      try {
        await server.close()
      } catch (error) {
        exitCode = 1
        console.error('Failed to close the benchmark server.', error)
      }
    }
    if (sirannon) {
      try {
        await sirannon.shutdown()
      } catch (error) {
        exitCode = 1
        console.error('Failed to shut down Sirannon.', error)
      }
    }
    try {
      rmSync(tempDir, { recursive: true, force: true })
    } catch (error) {
      exitCode = 1
      console.error('Failed to remove the benchmark temp directory.', error)
    }
    return exitCode
  })()
  return cleaningUp
}

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.once(signal, () => {
    void cleanup().then(code => process.exit(code))
  })
}

try {
  sirannon = new Sirannon({ driver: betterSqlite3() })
  const db = await sirannon.open(DATABASE_ID, dbPath, { readPoolSize: 4, walMode: true })
  await db.execute(DURABILITY === 'full' ? 'PRAGMA synchronous = FULL' : 'PRAGMA synchronous = NORMAL')

  server = createServer(sirannon, { host: HOST, port: PORT })
  await server.listen()
  console.log(`Sirannon benchmark server listening on ${HOST}:${PORT} (${DURABILITY} durability)`)
} catch (error) {
  console.error('Failed to start the Sirannon benchmark server.', error)
  const code = await cleanup()
  process.exit(code === 0 ? 1 : code)
}
