// Drive PostgreSQL through node-postgres, the client an application reaches it with: a pooled
// binary-protocol connection over the socket, one statement per call in autocommit mode to match
// Sirannon's per-statement commit. The pool size is disclosed, because the pool is where
// PostgreSQL's per-connection cost shows up as the client count climbs. Durability is set per
// session: `synchronous_commit = on` for the full-durability pass and `off` for the
// matched-relaxed pass, so the fsync behaviour matches Sirannon's `synchronous = FULL` and
// `NORMAL` respectively.

import { Pool } from 'pg'
import type { PostgresConfig } from '../config.ts'
import type { SeedTable } from '../workloads.ts'
import { Driver } from './driver.ts'

const SYNCHRONOUS_COMMIT: Record<string, string> = { full: 'on', matched: 'off' }
const MAX_BIND_PARAMS = 60_000

export class PostgresDriver extends Driver {
  readonly name = 'postgres'
  readonly delivery = 'socket'
  readonly dialect = 'postgres'

  private readonly config: PostgresConfig
  private readonly durability: string
  private readonly synchronousCommit: string
  private pool: Pool | null = null

  constructor(config: PostgresConfig, durability: string) {
    super()
    this.config = config
    this.durability = durability
    this.synchronousCommit = SYNCHRONOUS_COMMIT[durability] ?? 'off'
  }

  async connect(): Promise<void> {
    // The fsync behaviour is set at connection start through the libpq `options` startup
    // parameter, so every pooled session carries it before it serves a request. Setting it on a
    // per-query `connect` hook would race the pool handing the client to a caller, running two
    // statements on one connection at once.
    this.pool = new Pool({
      host: this.config.host,
      port: this.config.port,
      user: this.config.user,
      password: this.config.password,
      database: this.config.database,
      max: this.config.poolSize,
      options: `-c synchronous_commit=${this.synchronousCommit}`,
    })
    await this.read('SELECT 1', [])
  }

  private poolOrThrow(): Pool {
    if (this.pool === null) {
      throw new Error('Postgres driver used before connect()')
    }
    return this.pool
  }

  override render(sql: string): string {
    let index = 0
    return sql.replace(/\?/g, () => `$${++index}`)
  }

  async info(): Promise<Record<string, unknown>> {
    const pool = this.poolOrThrow()
    const version = (await pool.query('SELECT version()')).rows[0].version as string
    const sync = (await pool.query('SHOW synchronous_commit')).rows[0].synchronous_commit as string
    return {
      engine: 'postgres',
      delivery: this.delivery,
      durability_requested: this.durability,
      synchronous_commit: String(sync),
      version: String(version),
    }
  }

  async executeDdl(statements: string[]): Promise<void> {
    const pool = this.poolOrThrow()
    const client = await pool.connect()
    try {
      for (const statement of statements) {
        await client.query(statement)
      }
    } finally {
      client.release()
    }
  }

  async dropTables(tables: string[]): Promise<void> {
    const pool = this.poolOrThrow()
    const client = await pool.connect()
    try {
      for (const table of tables) {
        await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`)
      }
    } finally {
      client.release()
    }
  }

  async seed(tables: SeedTable[]): Promise<void> {
    const pool = this.poolOrThrow()
    const client = await pool.connect()
    try {
      for (const table of tables) {
        const columnCount = table.columns.length
        const rowsPerBatch = Math.max(1, Math.floor(MAX_BIND_PARAMS / columnCount))
        const columns = table.columns.join(', ')
        let batch: unknown[][] = []
        let began = false
        const flush = async (): Promise<void> => {
          if (batch.length === 0) {
            return
          }
          if (!began) {
            await client.query('BEGIN')
            began = true
          }
          const values: unknown[] = []
          const tuples: string[] = []
          let bind = 0
          for (const row of batch) {
            const marks: string[] = []
            for (const value of row) {
              values.push(value)
              marks.push(`$${++bind}`)
            }
            tuples.push(`(${marks.join(', ')})`)
          }
          batch = []
          await client.query(`INSERT INTO ${table.table} (${columns}) VALUES ${tuples.join(', ')}`, values)
        }
        try {
          for (const row of table.rows) {
            batch.push(row)
            if (batch.length >= rowsPerBatch) {
              await flush()
            }
          }
          await flush()
          if (began) {
            await client.query('COMMIT')
          }
        } catch (err) {
          if (began) {
            await client.query('ROLLBACK')
          }
          throw err
        }
      }
    } finally {
      client.release()
    }
  }

  async read(sql: string, params: unknown[]): Promise<void> {
    await this.poolOrThrow().query(this.render(sql), params)
  }

  async write(sql: string, params: unknown[]): Promise<void> {
    await this.poolOrThrow().query(this.render(sql), params)
  }

  async close(): Promise<void> {
    if (this.pool !== null) {
      await this.pool.end()
      this.pool = null
    }
  }
}
