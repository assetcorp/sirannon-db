import pg from 'pg'
import { type BenchConfig, loadConfig } from './config'
import type { Engine } from './engine'

export interface PostgresEngine extends Engine {
  name: 'postgres'
  pool: pg.Pool
}

const SEED_CHUNK_SIZE = 500

export function createPostgresEngine(config?: BenchConfig): PostgresEngine {
  const cfg = config ?? loadConfig()
  let pool: pg.Pool

  const engine: PostgresEngine = {
    name: 'postgres',

    get pool() {
      return pool
    },

    async setup(schemaSql: string) {
      pool = new pg.Pool({
        host: cfg.postgres.host,
        port: cfg.postgres.port,
        user: cfg.postgres.user,
        password: cfg.postgres.password,
        database: cfg.postgres.database,
        max: cfg.postgres.max,
      })

      if (cfg.durability === 'full') {
        await pool.query('SET synchronous_commit = on')
      }

      for (const stmt of schemaSql
        .split(';')
        .map(s => s.trim())
        .filter(Boolean)) {
        const match = stmt.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i)
        if (match) {
          await pool.query(`DROP TABLE IF EXISTS ${match[1]} CASCADE`)
        }
        await pool.query(stmt)
      }
    },

    async seed(insertSql: string, rows: unknown[][]) {
      if (rows.length === 0) return

      const colCount = rows[0].length

      for (let offset = 0; offset < rows.length; offset += SEED_CHUNK_SIZE) {
        const chunk = rows.slice(offset, offset + SEED_CHUNK_SIZE)
        const values: unknown[] = []
        const placeholders: string[] = []

        for (let i = 0; i < chunk.length; i++) {
          const row = chunk[i]
          const rowPlaceholders: string[] = []
          for (let j = 0; j < colCount; j++) {
            values.push(row[j])
            rowPlaceholders.push(`$${i * colCount + j + 1}`)
          }
          placeholders.push(`(${rowPlaceholders.join(', ')})`)
        }

        const baseSql = insertSql.replace(/VALUES\s*\(.*\)/i, '')
        const fullSql = `${baseSql} VALUES ${placeholders.join(', ')}`

        await pool.query(fullSql, values)
      }
    },

    async cleanup() {
      if (pool) {
        const tables = ['order_items', 'orders', 'products', 'customers', 'usertable', 'users', 'events']
        for (const table of tables) {
          try {
            await pool.query(`DROP TABLE IF EXISTS ${table} CASCADE`)
          } catch {
            // table might not exist
          }
        }
        await pool.end()
      }
    },

    async getInfo() {
      const settings = ['shared_buffers', 'work_mem', 'synchronous_commit', 'effective_cache_size', 'wal_level']
      const info: Record<string, string> = {}
      for (const setting of settings) {
        const result = await pool.query(`SHOW ${setting}`)
        info[setting] = result.rows[0]?.[setting] ?? 'unknown'
      }

      const versionResult = await pool.query('SELECT version()')
      info.version = versionResult.rows[0]?.version ?? 'unknown'

      return info
    },
  }

  return engine
}

export async function isPostgresAvailable(config?: BenchConfig): Promise<boolean> {
  const cfg = config ?? loadConfig()
  const client = new pg.Client({
    host: cfg.postgres.host,
    port: cfg.postgres.port,
    user: cfg.postgres.user,
    password: cfg.postgres.password,
    database: cfg.postgres.database,
    connectionTimeoutMillis: 3_000,
  })

  try {
    await client.connect()
    await client.query('SELECT 1')
    await client.end()
    return true
  } catch {
    try {
      await client.end()
    } catch {
      // already closed
    }
    return false
  }
}
