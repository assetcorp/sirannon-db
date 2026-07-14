// Boot the real Sirannon HTTP server for the benchmark. The harness drives this exactly as an
// application would: JSON requests to /db/<id>/query, /execute, /transaction, and the bulk-load
// endpoint over loopback. The database starts empty; the harness applies each workload's schema
// and seed over the wire.
//
// Writer durability comes from BENCH_DURABILITY through the open option: 'full' fsyncs every commit
// to match PostgreSQL synchronous_commit=on; 'normal' defers the fsync to match synchronous_commit
// =off. Both run in WAL mode. Seeding uses the bulk-load endpoint, which loads each batch with sync
// relaxed and then restores this configured level, so measured writes always run at it.
//
// The request-body limit is raised well above the load batch size the driver sends, so a seed batch
// is never rejected; the driver keeps each batch within memory on its own side.

import { mkdirSync, mkdtempSync, readdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { Sirannon } from '../../packages/ts/dist/core/index.mjs'
import { betterSqlite3 } from '../../packages/ts/dist/driver/better-sqlite3.mjs'
import { createServer } from '../../packages/ts/dist/server/index.mjs'

const HOST = process.env.HOST ?? '0.0.0.0'
const PORT = Number(process.env.PORT ?? 9876)
const DATABASE_ID = process.env.BENCH_SIRANNON_DB ?? 'bench'
const DURABILITY = process.env.BENCH_DURABILITY === 'full' ? 'full' : 'matched'
const WRITER_SYNCHRONOUS = DURABILITY === 'full' ? 'full' : 'normal'
const WRITER_WORKER = !['off', 'false', '0', 'no'].includes((process.env.BENCH_WRITER_WORKER ?? '').toLowerCase())
// uWebSockets.js stores its payload and backpressure limits in unsigned 32 bits, so anything
// above that silently wraps; the limit is validated here so a configured value always takes
// effect as written.
const UWS_LIMIT_BYTES = 4_294_967_295
const MAX_BODY_BYTES = Number(process.env.BENCH_MAX_BODY_BYTES ?? 1_073_741_824)
if (!Number.isInteger(MAX_BODY_BYTES) || MAX_BODY_BYTES <= 0 || MAX_BODY_BYTES > UWS_LIMIT_BYTES) {
  console.error(`BENCH_MAX_BODY_BYTES must be a positive integer of at most ${UWS_LIMIT_BYTES} bytes.`)
  process.exit(1)
}

// BENCH_DATA_DIR points the database at a chosen directory (the harness uses the local NVMe
// mount); without it the server keeps its throwaway temp directory. Each start begins from an
// empty directory. Only plain files named after the benchmark database are ever deleted, and a
// configured directory itself is never removed, so a mispointed variable can never delete
// unrelated files or a mount point.
const isBenchDatabaseFile = entry => entry.isFile() && entry.name.startsWith('bench.db')

const wipeBenchDatabase = dir => {
  const entries = readdirSync(dir, { withFileTypes: true })
  const foreign = entries.filter(entry => !isBenchDatabaseFile(entry))
  if (foreign.length > 0) {
    throw new Error(`BENCH_DATA_DIR ${dir} contains entries other than a previous benchmark database; refusing to wipe it`)
  }
  for (const entry of entries) {
    rmSync(join(dir, entry.name), { force: true })
  }
}

const prepareDataDir = () => {
  const configured = process.env.BENCH_DATA_DIR
  if (configured === undefined || configured.trim() === '') {
    return { dir: mkdtempSync(join(tmpdir(), 'sirannon-bench-')), temporary: true }
  }
  const dir = resolve(configured)
  mkdirSync(dir, { recursive: true })
  wipeBenchDatabase(dir)
  return { dir, temporary: false }
}

let dataDir
let dataDirIsTemporary
try {
  const prepared = prepareDataDir()
  dataDir = prepared.dir
  dataDirIsTemporary = prepared.temporary
} catch (error) {
  console.error('Failed to prepare the benchmark data directory.', error)
  process.exit(1)
}
const dbPath = join(dataDir, 'bench.db')

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
      if (dataDirIsTemporary) {
        rmSync(dataDir, { recursive: true, force: true })
      } else {
        for (const entry of readdirSync(dataDir, { withFileTypes: true })) {
          if (isBenchDatabaseFile(entry)) {
            rmSync(join(dataDir, entry.name), { force: true })
          }
        }
      }
    } catch (error) {
      exitCode = 1
      console.error('Failed to remove the benchmark database files.', error)
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
  sirannon = new Sirannon({ driver: betterSqlite3(), writerWorker: WRITER_WORKER })
  await sirannon.open(DATABASE_ID, dbPath, { readPoolSize: 4, walMode: true, synchronous: WRITER_SYNCHRONOUS })

  server = createServer(sirannon, { host: HOST, port: PORT, maxBodyBytes: MAX_BODY_BYTES })
  await server.listen()
  console.log(
    `Sirannon benchmark server listening on ${HOST}:${PORT} (${DURABILITY} durability, writer worker ${WRITER_WORKER ? 'on' : 'off'})`,
  )
} catch (error) {
  console.error('Failed to start the Sirannon benchmark server.', error)
  const code = await cleanup()
  process.exit(code === 0 ? 1 : code)
}
