import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Database } from '../src/core/database'
import type { SQLiteDriver } from '../src/core/driver/types'
import type { DatabaseOptions } from '../src/core/types'
import type { BenchConfig } from './config'
import type { Engine } from './engine'

export interface SirannonEngine extends Engine {
  name: 'sirannon'
  db: Database
}

export function createSirannonEngine(
  driver: SQLiteDriver,
  config?: BenchConfig,
  dbOptions?: DatabaseOptions,
): SirannonEngine {
  let tempDir: string
  let db: Database

  const engine: SirannonEngine = {
    name: 'sirannon',

    get db() {
      return db
    },

    async setup(schemaSql: string) {
      tempDir = mkdtempSync(join(tmpdir(), 'sirannon-bench-'))
      const dbPath = join(tempDir, 'bench.db')
      db = await Database.create('bench', dbPath, driver, {
        readPoolSize: dbOptions?.readPoolSize ?? 4,
        walMode: true,
        ...dbOptions,
      })

      if (config?.durability === 'full') {
        await db.execute('PRAGMA synchronous = FULL')
      }

      for (const stmt of schemaSql
        .split(';')
        .map(s => s.trim())
        .filter(Boolean)) {
        await db.execute(stmt)
      }
    },

    async seed(insertSql: string, rows: unknown[][]) {
      await db.executeBatch(
        insertSql,
        rows.map(r => r as unknown[]),
      )
    },

    async cleanup() {
      if (db && !db.closed) {
        await db.close()
      }
      if (tempDir) {
        rmSync(tempDir, { recursive: true, force: true })
      }
    },

    async getInfo() {
      const pragmas = ['journal_mode', 'synchronous', 'cache_size', 'mmap_size', 'page_size']
      const info: Record<string, string> = {}
      for (const pragma of pragmas) {
        const result = await db.query<Record<string, unknown>>(`PRAGMA ${pragma}`)
        const val = result[0] ? Object.values(result[0])[0] : 'unknown'
        info[pragma] = String(val)
      }
      return info
    },
  }

  return engine
}
