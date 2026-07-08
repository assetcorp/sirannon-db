import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { RemoteDatabase } from '../../src/client'
import { SirannonClient } from '../../src/client'
import { Sirannon } from '../../src/core/sirannon'
import { createServer, type SirannonServer } from '../../src/server/server'
import { loadBenchDriver } from '../config'

/**
 * The fair engine track drives Sirannon the same way a real application does: through the
 * SirannonClient SDK over HTTP into the real server front-end, co-located on one host over
 * loopback. This module owns the co-located server plus the SDK client so both the single
 * client and the concurrency sweep issue queries over the real request/response path rather
 * than reaching the embedded engine by direct function call.
 *
 * Sirannon pays HTTP and JSON framing on every request, which is heavier than Postgres's
 * binary wire protocol. That cost is disclosed in the generated methodology: it is Sirannon's
 * real client cost, so erring in that direction keeps the comparison honest.
 */

const DATABASE_ID = 'bench'
const LOOPBACK_HOST = '127.0.0.1'

export type Durability = 'matched' | 'full'

export interface FairSirannonHarness {
  readonly db: RemoteDatabase
  seed(insertSql: string, rows: unknown[][]): Promise<void>
  info(): Promise<Record<string, string>>
  cleanup(): Promise<void>
}

interface HarnessState {
  sirannon: Sirannon
  server: SirannonServer
  client: SirannonClient
  db: RemoteDatabase
  tempDir: string
  port: number
}

function pragmaForDurability(durability: Durability): string {
  return durability === 'full' ? 'PRAGMA synchronous = FULL' : 'PRAGMA synchronous = NORMAL'
}

function splitStatements(schemaSql: string): string[] {
  return schemaSql
    .split(';')
    .map(statement => statement.trim())
    .filter(Boolean)
}

async function startState(schemaSql: string, durability: Durability): Promise<HarnessState> {
  const tempDir = mkdtempSync(join(tmpdir(), 'sirannon-fair-'))
  const dbPath = join(tempDir, 'bench.db')

  let sirannon: Sirannon | undefined
  let server: SirannonServer | undefined
  let client: SirannonClient | undefined

  try {
    const driver = await loadBenchDriver()
    sirannon = new Sirannon({ driver })
    const localDb = await sirannon.open(DATABASE_ID, dbPath, { readPoolSize: 4, walMode: true })
    await localDb.execute(pragmaForDurability(durability))

    for (const statement of splitStatements(schemaSql)) {
      await localDb.execute(statement)
    }

    server = createServer(sirannon, { host: LOOPBACK_HOST, port: 0 })
    await server.listen()
    const port = server.listeningPort
    if (port <= 0) {
      throw new Error('Fair harness server did not report a bound port')
    }

    client = new SirannonClient(`http://${LOOPBACK_HOST}:${port}`, { transport: 'http', autoReconnect: false })
    const db = client.database(DATABASE_ID)

    return { sirannon, server, client, db, tempDir, port }
  } catch (error) {
    if (client) client.close()
    if (server) await server.close().catch(() => {})
    if (sirannon) await sirannon.shutdown().catch(() => {})
    rmSync(tempDir, { recursive: true, force: true })
    throw error
  }
}

async function seedThroughServer(db: RemoteDatabase, insertSql: string, rows: unknown[][]): Promise<void> {
  const CHUNK = 500
  for (let offset = 0; offset < rows.length; offset += CHUNK) {
    const chunk = rows.slice(offset, offset + CHUNK)
    await db.transaction(chunk.map(row => ({ sql: insertSql, params: row })))
  }
}

async function collectInfo(sirannon: Sirannon): Promise<Record<string, string>> {
  const localDb = sirannon.get(DATABASE_ID)
  if (!localDb) {
    return { engine: 'sirannon', delivery: 'client-server-http', status: 'not initialized' }
  }
  const pragmas = ['journal_mode', 'synchronous', 'cache_size', 'page_size']
  const result: Record<string, string> = { engine: 'sirannon', delivery: 'client-server-http' }
  for (const pragma of pragmas) {
    const pragmaRows = await localDb.query<Record<string, unknown>>(`PRAGMA ${pragma}`)
    const first = pragmaRows[0]
    result[pragma] = String(first ? Object.values(first)[0] : 'unknown')
  }
  const versionRows = await localDb.query<Record<string, unknown>>('SELECT sqlite_version() AS version')
  result.version = String(versionRows[0]?.version ?? 'unknown')
  return result
}

export async function createFairSirannonHarness(
  schemaSql: string,
  durability: Durability,
): Promise<FairSirannonHarness> {
  const state = await startState(schemaSql, durability)
  let cleaned = false

  return {
    db: state.db,
    async seed(insertSql: string, rows: unknown[][]) {
      if (rows.length === 0) return
      await seedThroughServer(state.db, insertSql, rows)
    },
    async info() {
      return collectInfo(state.sirannon)
    },
    async cleanup() {
      if (cleaned) return
      cleaned = true
      state.client.close()
      try {
        await state.server.close()
      } finally {
        try {
          await state.sirannon.shutdown()
        } finally {
          rmSync(state.tempDir, { recursive: true, force: true })
        }
      }
    },
  }
}
